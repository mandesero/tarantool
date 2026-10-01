#include "memtx_vector.h"

#include <small/small.h>
#include <small/mempool.h>

#include "index.h"
#include "box.h"
#include "port.h"
#include "memtx_index.h"
#include "errinj.h"
#include "fiber.h"
#include "clock.h"
#include "trivia/util.h"

#include "tuple.h"
#include "txn.h"
#include "memtx_tx.h"
#include "space.h"
#include "schema.h"
#include "memtx_engine.h"
#include "ann_memory.h"
#include "ann_tuple_map.h"
#include "info/info.h"
#include "error.h"
#include "lib/ann/ann_numeric.h"
#include "lib/ann/ann_usearch.h"

#include <cmath>

/** A memtx secondary index backed by one ANN graph. */
struct memtx_vector_index {
	/** Generic index interface. */
	struct index base;
	/** Number of scalar coordinates in each vector. */
	unsigned dimension;
	/** Numeric distance function configured for this index. */
	enum ann_metric metric;
	/** All backend allocations are charged to the memtx quota. */
	struct ann_quota_memory memory_owner;
	/** Current HNSW generation. */
	struct ann_backend *backend;
	/** Stable labels and retained tuple addresses. */
	struct ann_tuple_map tuples;
	/** Process-local identifier of the active graph generation. */
	uint64_t generation;
	/** Cumulative local search requests. */
	uint64_t search_requests;
	/** Cumulative failed local search requests. */
	uint64_t search_errors;
	/** Cumulative request-deadline expirations. */
	uint64_t search_timeouts;
	/** Cumulative backend work units visited by searches. */
	uint64_t visited_candidates;
	/** Cumulative predicate rejections, including visibility. */
	uint64_t filtered_candidates;
	/** Estimated peak temporary bytes allocated by this layer. */
	uint64_t temporary_peak;
};

/** TX-thread-local monotonic diagnostic generation counter. */
static uint64_t vector_generation_next;

/** Read context shared by the ANN candidate predicate. */
struct vector_index_filter_ctx {
	/** Index whose label map is read. */
	struct memtx_vector_index *index;
	/** Space needed for memtx version clarification. */
	struct space *space;
	/** Transaction whose visible version is selected. */
	struct txn *txn;
	/** Optional one-based unsigned field number. */
	uint32_t fieldno;
	/** Sorted membership set, owned by the fiber region. */
	const uint64_t *values;
	/** Number of membership values. */
	uint32_t value_count;
};

/** Accept only a candidate's own visible tuple version. */
static bool
vector_index_filter(uint64_t label, void *ctx)
{
	struct vector_index_filter_ctx *filter =
		(struct vector_index_filter_ctx *)ctx;
	struct tuple *tuple = (struct tuple *)ann_tuple_map_get(
		&filter->index->tuples, label);
	if (tuple == NULL ||
	    !ann_usearch_ops.is_live(filter->index->backend, label) ||
	    memtx_tx_tuple_clarify(filter->txn, filter->space, tuple,
				   &filter->index->base, 0) != tuple) {
		++filter->index->filtered_candidates;
		return false;
	}
	if (filter->fieldno == 0)
		return true;
	const char *field = tuple_field(tuple, filter->fieldno - 1);
	if (field == NULL || mp_typeof(*field) != MP_UINT) {
		++filter->index->filtered_candidates;
		return false;
	}
	uint64_t value = mp_decode_uint(&field);
	uint32_t lo = 0, hi = filter->value_count;
	while (lo < hi) {
		uint32_t mid = lo + (hi - lo) / 2;
		if (filter->values[mid] < value)
			lo = mid + 1;
		else
			hi = mid;
	}
	bool accepted = lo < filter->value_count &&
			filter->values[lo] == value;
	if (!accepted)
		++filter->index->filtered_candidates;
	return accepted;
}

static int
vector_u64_compare(const void *left, const void *right)
{
	uint64_t a = *(const uint64_t *)left;
	uint64_t b = *(const uint64_t *)right;
	return (a > b) - (a < b);
}

/** Report a backend failure through the Tarantool diagnostic area. */
static int
vector_index_diag(enum ann_status status, const char *what)
{
	if (status == ANN_OK)
		return 0;
	if (status == ANN_OUT_OF_MEMORY) {
		diag_set(OutOfMemory, 0, "vector index", what);
	} else if (status == ANN_TIMEOUT) {
		if (box_check_slice() == 0)
			diag_set(ClientError, ER_VECTOR_TIMEOUT);
	} else if (status == ANN_WORK_LIMIT) {
		diag_set(ClientError, ER_VECTOR_WORK_LIMIT);
	} else if (status == ANN_INVALID) {
		diag_set(ClientError, ER_VECTOR_INVALID);
	} else if (status == ANN_STOPPED) {
		diag_set(FiberIsCancelled);
	} else {
		diag_set(IllegalParams,
			 tt_sprintf("vector index: %s: status %d", what,
				    (int)status));
	}
	return -1;
}

/** Read the system monotonic clock rather than the cached event-loop time. */
static uint64_t
vector_index_now_ns(void *ctx)
{
	(void)ctx;
	return clock_monotonic64();
}

/** Apply the query deadline to work done outside the ANN backend. */
static int
vector_index_check_deadline(uint64_t deadline)
{
	if (vector_index_now_ns(NULL) < deadline)
		return 0;
	diag_set(ClientError, ER_VECTOR_TIMEOUT);
	return -1;
}

/** Cancellation is safe to check in a USearch noexcept traversal. */
static bool
vector_index_should_stop(void *ctx)
{
	(void)ctx;
	return fiber_is_cancelled();
}

/** Bound HNSW work by the remaining slice of the current fiber call. */
static struct ann_search_control
vector_index_control(void)
{
	struct ann_search_control control = {};
	control.work_limit = UINT64_MAX;
	control.now_ns = vector_index_now_ns;
	control.should_stop = vector_index_should_stop;
	double deadline = cord()->call_time + cord()->slice.err;
	if (std::isfinite(deadline) && deadline > 0 &&
	    deadline < (double)UINT64_MAX / 1000000000.0)
		control.deadline_ns = (uint64_t)(deadline * 1000000000.0);
	return control;
}

static inline int
mp_decode_num(const char **data, uint32_t fieldno, double *ret)
{
	if (mp_read_double(data, ret) != 0) {
		diag_set(ClientError, ER_FIELD_TYPE,
			 int2str(fieldno + TUPLE_INDEX_BASE),
			 field_type_strs[FIELD_TYPE_NUMBER],
			 mp_type_strs[mp_typeof(**data)]);
		return -1;
	}
	return 0;
}

static inline int
mp_decode_vector(double **vector, unsigned dimension,
	         const char *mp, unsigned count, const char *what)
{
	(void)what;
	double c = 0;
    if (count == dimension) {
        for (unsigned i = 0; i < dimension; i++) {
            if (mp_decode_num(&mp, i, &c) < 0)
                return -1;
            (*vector)[i] = c;
        }
    } else {
		diag_set(ClientError, ER_RTREE_RECT,
			 what, dimension, dimension);
		return -1;
    }
	return 0;
}

static inline int
mp_decode_vector_from_key(double **vector, unsigned dimensions,
			  const char *mp, uint32_t part_count)
{
	if (part_count == 1)
		part_count = mp_decode_array(&mp);
	return mp_decode_vector(vector, dimensions, mp, part_count, "Key");
}

static inline int
extract_vector(double **vector, struct tuple *tuple,
	       struct index_def *index_def)
{
	assert(index_def->key_def->part_count == 1);
	assert(!index_def->key_def->is_multikey);
	const char *elems = tuple_field_by_part(tuple,
				index_def->key_def->parts, MULTIKEY_NONE);
	unsigned dimension = index_def->opts.dimension;
	uint32_t count = mp_decode_array(&elems);
	return mp_decode_vector(vector, dimension, elems, count, "Field");
}

static int
memtx_vector_index_get_internal(struct index *base, const char *key,
			       uint32_t part_count, struct tuple **result,
			       bool is_rw)
{
	(void)base;
	(void)key;
	(void)part_count;
	(void)result;
	(void)is_rw;
	diag_set(ClientError, ER_VECTOR_UNSUPPORTED);
	return -1;
}

/** Generic iterators cannot carry the bounded VECTOR search options. */
static struct iterator *
memtx_vector_index_create_iterator(struct index *base, enum iterator_type type,
				  const char *key, uint32_t part_count,
				  const char *pos)
{
	(void)base;
	(void)type;
	(void)key;
	(void)part_count;
	(void)pos;
	diag_set(ClientError, ER_VECTOR_UNSUPPORTED);
	return NULL;
}

int
memtx_vector_index_search(struct index *base,
			  const struct memtx_vector_search_opts *request,
			  struct port *port, double *distances)
{
	struct memtx_vector_index *index = (struct memtx_vector_index *)base;
	struct txn *txn = in_txn();
	if (txn != NULL && txn->isolation == TXN_ISOLATION_LINEARIZABLE) {
		diag_set(ClientError, ER_VECTOR_UNSUPPORTED);
		return -1;
	}
	if (request->limit > 1024 || request->offset != 0 ||
	    !std::isfinite(request->timeout) || request->timeout <= 0 ||
	    request->timeout > 30 || request->ef_search > 8192 ||
	    (request->filter == NULL) != (request->filter_fieldno == 0)) {
		diag_set(ClientError, ER_VECTOR_INVALID);
		return -1;
	}
	uint64_t deadline = vector_index_now_ns(NULL) +
			(uint64_t)(request->timeout * 1000000000.0);
	++index->search_requests;
	struct region *region = &fiber()->gc;
	size_t svp = region_used(region);
	const char *key = request->key;
	uint32_t part_count = 0;
	double *vector = NULL;
	float *query = NULL;
	enum ann_status status = ANN_OK;
	struct ann_candidate *candidates = NULL;
	struct ann_filter filter = {};
	struct ann_search_control control = {};
	struct ann_usearch_search_opts algorithm = {};
	struct ann_search_opts opts = {};
	uint32_t count = 0;
	struct key_def *pk = NULL;
	uint32_t out_count = 0;
	struct errinj *work_inj = NULL;
	uint64_t temporary_bytes = 0;
	struct vector_index_filter_ctx filter_ctx = {};
	filter_ctx.index = index;
	filter_ctx.space = space_by_id(base->def->space_id);
	filter_ctx.txn = txn;
	filter_ctx.fieldno = request->filter_fieldno;
	if (request->filter != NULL) {
		const char *data = request->filter;
		if (mp_typeof(*data) != MP_ARRAY) {
			diag_set(ClientError, ER_VECTOR_INVALID);
			goto fail;
		}
		filter_ctx.value_count = mp_decode_array(&data);
		if (filter_ctx.value_count > 65536) {
			diag_set(ClientError, ER_VECTOR_INVALID);
			goto fail;
		}
		uint64_t *values = filter_ctx.value_count == 0 ? NULL :
			xregion_alloc_array(region, uint64_t,
					    filter_ctx.value_count);
		for (uint32_t i = 0; i < filter_ctx.value_count; ++i) {
			if ((i & 1023) == 0 &&
			    vector_index_check_deadline(deadline) != 0)
				goto fail;
			if (data >= request->filter_end ||
			    mp_typeof(*data) != MP_UINT) {
				diag_set(ClientError, ER_VECTOR_INVALID);
				goto fail;
			}
			values[i] = mp_decode_uint(&data);
		}
		if (data != request->filter_end) {
			diag_set(ClientError, ER_VECTOR_INVALID);
			goto fail;
		}
		if (filter_ctx.value_count > 1)
			qsort(values, filter_ctx.value_count, sizeof(*values),
			      vector_u64_compare);
		filter_ctx.values = values;
	}
	part_count = mp_decode_array(&key);
	vector = xregion_alloc_array(region, double, index->dimension);
	if (mp_decode_vector_from_key(&vector, index->dimension, key,
				      part_count) != 0)
		goto fail;
	query = xregion_alloc_array(region, float, index->dimension);
	status = ann_vector_from_double(vector, index->dimension,
							index->metric, query);
	if (vector_index_diag(status, "query") != 0)
		goto fail;
	if (vector_index_check_deadline(deadline) != 0)
		goto fail;
	if (box_check_slice() != 0)
		goto fail;
	if (request->limit == 0) {
		region_truncate(region, svp);
		return 0;
	}
	temporary_bytes = (uint64_t)filter_ctx.value_count * sizeof(uint64_t) +
		(uint64_t)index->dimension * (sizeof(double) + sizeof(float)) +
		(uint64_t)request->limit * sizeof(struct ann_candidate);
	if (temporary_bytes > index->temporary_peak)
		index->temporary_peak = temporary_bytes;
	memtx_tx_track_full_scan(txn, filter_ctx.space, base);
	candidates = xregion_alloc_array(region,
					struct ann_candidate, request->limit);
	filter.accept = vector_index_filter;
	filter.ctx = &filter_ctx;
	control = vector_index_control();
	if (control.deadline_ns == 0 || deadline < control.deadline_ns)
		control.deadline_ns = deadline;
	control.work_limit = 1ULL << 32;
	work_inj = errinj(ERRINJ_VECTOR_WORK_LIMIT, ERRINJ_INT);
	if (work_inj != NULL && work_inj->iparam > 0)
		control.work_limit = work_inj->iparam;
	algorithm.expansion = request->ef_search != 0 ?
		request->ef_search : base->def->opts.vector_ef_search;
	if (algorithm.expansion < request->limit)
		algorithm.expansion = request->limit;
	opts.candidate_limit = request->limit;
	opts.query_dimension = index->dimension;
	opts.filter = &filter;
	opts.control = &control;
	opts.algorithm = &algorithm;
	status = ann_usearch_ops.search(index->backend, query, &opts,
					&candidates[0], &count);
	index->visited_candidates += control.work_done;
	if (status == ANN_TIMEOUT &&
	    vector_index_check_deadline(deadline) != 0)
		goto fail;
	if (status == ANN_TIMEOUT && control.deadline_ns < deadline) {
		diag_set(FiberSliceIsExceeded);
		goto fail;
	}
	if (vector_index_diag(status, "search") != 0)
		goto fail;
	/* The backend orders by distance; resolve equal distances by PK. */
	pk = filter_ctx.space->index[0]->def->key_def;
	for (uint32_t i = 1; i < count; ++i) {
		if (vector_index_check_deadline(deadline) != 0)
			goto fail;
		struct ann_candidate item = candidates[i];
		uint32_t j = i;
		while (j > 0 && candidates[j - 1].distance == item.distance) {
			struct tuple *left = (struct tuple *)ann_tuple_map_get(
				&index->tuples, candidates[j - 1].label);
			struct tuple *right = (struct tuple *)ann_tuple_map_get(
				&index->tuples, item.label);
			if (tuple_compare(left, HINT_NONE, right, HINT_NONE,
					  pk) <= 0)
				break;
			candidates[j] = candidates[j - 1];
			--j;
		}
		candidates[j] = item;
	}
	for (uint32_t i = 0; i < count; ++i) {
		if (vector_index_check_deadline(deadline) != 0)
			goto fail;
		if (box_check_slice() != 0)
			goto fail;
		struct tuple *tuple = (struct tuple *)ann_tuple_map_get(
			&index->tuples, candidates[i].label);
		if (tuple == NULL)
			continue;
		port_c_add_tuple(port, tuple);
		distances[out_count++] = candidates[i].distance;
	}
	region_truncate(region, svp);
	return 0;
fail:
	++index->search_errors;
	if (box_error_code(diag_last_error(diag_get())) ==
	    ER_VECTOR_TIMEOUT)
		++index->search_timeouts;
	region_truncate(region, svp);
	return -1;
}

int
memtx_vector_index_rebuild(struct index *base)
{
	struct memtx_vector_index *index = (struct memtx_vector_index *)base;
	struct ann_usearch_config algorithm = {
		base->def->opts.vector_m,
		base->def->opts.vector_ef_construction,
		base->def->opts.vector_ef_search,
	};
	struct ann_config config = {index->dimension, index->metric,
				    &algorithm};
	struct ann_memory memory = ann_quota_memory_bind(&index->memory_owner);
	uint64_t original_bytes = index->memory_owner.bytes;
	struct ann_backend *replacement = NULL;
	struct ann_tuple_map replacement_map;
	ann_tuple_map_create(&replacement_map, &memory);
	struct region *region = &fiber()->gc;
	size_t svp = region_used(region);
	double *decoded = xregion_alloc_array(region, double,
					       index->dimension);
	float *canonical = xregion_alloc_array(region, float,
						index->dimension);
	struct ann_search_control control = vector_index_control();
	enum ann_status status = ann_usearch_ops.create(&config, &memory,
						     &replacement);
	if (status != ANN_OK)
		goto fail_status;
	for (uint32_t i = 0; i < index->tuples.count; ++i) {
		if (box_check_slice() != 0)
			goto fail;
		uint64_t old_label = (uint64_t)i + 1;
		struct tuple *tuple = (struct tuple *)ann_tuple_map_get(
			&index->tuples, old_label);
		if (tuple == NULL)
			continue;
		bool live = ann_usearch_ops.is_live(index->backend,
							old_label);
		if (extract_vector(&decoded, tuple, base->def) != 0)
			goto fail;
		status = ann_vector_from_double(decoded, index->dimension,
						index->metric, canonical);
		if (status != ANN_OK)
			goto fail_status;
		status = ann_tuple_map_prepare(&replacement_map);
		if (status != ANN_OK)
			goto fail_status;
		uint64_t new_label = (uint64_t)replacement_map.count + 1;
		struct ann_change *change = NULL;
		status = ann_usearch_ops.prepare(replacement, ANN_INSERT,
						new_label, canonical,
						index->dimension, &change);
		if (status != ANN_OK)
			goto fail_status;
		status = ann_usearch_ops.apply(change, &control);
		if (status != ANN_OK)
			ann_usearch_ops.rollback(change);
		ann_usearch_ops.finish(change);
		if (status != ANN_OK)
			goto fail_status;
		if (!live) {
			status = ann_usearch_ops.set_live(replacement,
							 new_label, false);
			if (status != ANN_OK)
				goto fail_status;
		}
		uint64_t assigned = ann_tuple_map_insert(&replacement_map,
							     tuple);
		assert(assigned == new_label);
		tuple_ref(tuple);
		uint64_t temporary = index->memory_owner.bytes -
				     original_bytes;
		if (temporary > index->temporary_peak)
			index->temporary_peak = temporary;
	}
	{
		struct ann_backend *old_backend = index->backend;
		struct ann_tuple_map old_map = index->tuples;
		index->backend = replacement;
		index->tuples = replacement_map;
		index->generation = ++vector_generation_next;
		ann_usearch_ops.destroy(old_backend);
		for (uint32_t i = 0; i < old_map.count; ++i) {
			struct tuple *tuple = (struct tuple *)ann_tuple_map_get(
				&old_map, (uint64_t)i + 1);
			if (tuple != NULL)
				tuple_unref(tuple);
		}
		ann_tuple_map_destroy(&old_map);
	}
	region_truncate(region, svp);
	return 0;
fail_status:
	vector_index_diag(status, "rebuild");
fail:
	if (index->memory_owner.bytes > original_bytes) {
		uint64_t temporary = index->memory_owner.bytes -
				     original_bytes;
		if (temporary > index->temporary_peak)
			index->temporary_peak = temporary;
	}
	for (uint32_t i = 0; i < replacement_map.count; ++i) {
		struct tuple *tuple = (struct tuple *)ann_tuple_map_get(
			&replacement_map, (uint64_t)i + 1);
		if (tuple != NULL)
			tuple_unref(tuple);
	}
	ann_tuple_map_destroy(&replacement_map);
	if (replacement != NULL)
		ann_usearch_ops.destroy(replacement);
	region_truncate(region, svp);
	return -1;
}

static int
memtx_vector_index_replace(struct index *base, struct tuple *old_tuple,
			  struct tuple *new_tuple, enum dup_replace_mode mode,
			  struct tuple **result, struct tuple **successor)
{
	(void)mode;
	struct memtx_vector_index *index = (struct memtx_vector_index *)base;
	struct ann_change *new_change = NULL;
	bool retain_old = false;
	struct ann_search_control control = vector_index_control();
	*successor = NULL;
	if (old_tuple == new_tuple) {
		*result = old_tuple;
		return 0;
	}
	uint64_t old_label = old_tuple == NULL ? 0 :
		ann_tuple_map_find(&index->tuples, old_tuple);
	uint64_t new_label = new_tuple == NULL ? 0 :
		ann_tuple_map_find(&index->tuples, new_tuple);
	if (old_tuple != NULL && old_label == 0) {
		vector_index_diag(ANN_NOT_FOUND, "old tuple");
		return -1;
	}

	struct region *region = &fiber()->gc;
	size_t region_svp = region_used(region);
	float *canonical = NULL;
	if (new_tuple != NULL && new_label == 0) {
		double *vector = xregion_alloc_array(region, double,
						       index->dimension);
		canonical = xregion_alloc_array(region, float,
						  index->dimension);
		if (extract_vector(&vector, new_tuple, base->def) != 0)
			goto fail;
		enum ann_status status = ann_vector_from_double(vector,
			index->dimension, index->metric, canonical);
		if (vector_index_diag(status, "convert") != 0)
			goto fail;
		status = ann_tuple_map_prepare(&index->tuples);
		if (vector_index_diag(status, "tuple map") != 0)
			goto fail;
		new_label = (uint64_t)index->tuples.count + 1;
		status = ann_usearch_ops.prepare(index->backend,
			ANN_INSERT, new_label, canonical,
			index->dimension, &new_change);
		if (vector_index_diag(status, "prepare") != 0)
			goto fail;
	}
	retain_old = memtx_tx_manager_use_mvcc_engine &&
		     new_change != NULL;
	if (old_label != 0 && !retain_old) {
		enum ann_status status = ann_usearch_ops.set_live(
			index->backend, old_label, false);
		if (vector_index_diag(status, "retire") != 0)
			goto fail;
	}
	if (new_tuple != NULL) {
		enum ann_status status = new_change == NULL ?
			ann_usearch_ops.set_live(index->backend, new_label, true) :
			ann_usearch_ops.apply(new_change, &control);
		if (vector_index_diag(status, "insert") != 0)
			goto fail_restore_old;
		if (new_change != NULL) {
			uint64_t published = ann_tuple_map_insert(&index->tuples,
								new_tuple);
			assert(published == new_label);
			tuple_ref(new_tuple);
		}
	}
	ann_usearch_ops.finish(new_change);
	region_truncate(region, region_svp);
	*result = old_tuple;
	return 0;
fail_restore_old:
	if (old_label != 0) {
		enum ann_status status = ann_usearch_ops.set_live(
			index->backend, old_label, true);
		assert(status == ANN_OK);
	}
fail:
	ann_usearch_ops.rollback(new_change);
	ann_usearch_ops.finish(new_change);
	region_truncate(region, region_svp);
	return -1;
}

static ssize_t
memtx_vector_index_size(struct index *base)
{
	struct memtx_vector_index *index = (struct memtx_vector_index *)base;
	struct ann_backend_stats stats;
	ann_usearch_ops.stat(index->backend, &stats);
	return stats.live;
}

static ssize_t
memtx_vector_index_bsize(struct index *base)
{
	struct memtx_vector_index *index = (struct memtx_vector_index *)base;
	return index->memory_owner.bytes;
}

/** Report current index-owned bytes and cumulative search counters. */
static void
memtx_vector_index_stat(struct index *base, struct info_handler *handler)
{
	struct memtx_vector_index *index = (struct memtx_vector_index *)base;
	struct ann_backend_stats stats;
	ann_usearch_ops.stat(index->backend, &stats);
	struct space *space = space_by_id(base->def->space_id);
	ssize_t current = index_size(space->index[0]);
	uint64_t live = current > 0 ? (uint64_t)current : 0;
	uint64_t retained = stats.live > live ? stats.live - live : 0;
	uint64_t retained_vectors =
		(retained + stats.retired) * index->dimension * sizeof(float);
	if (retained_vectors > stats.vector_bytes)
		retained_vectors = stats.vector_bytes;
	uint64_t tuple_lookup =
		(uint64_t)index->tuples.capacity * sizeof(void *) +
		(uint64_t)index->tuples.hash_capacity * sizeof(uint32_t);
	uint64_t lookup_bytes = stats.lookup_bytes + tuple_lookup;
	uint64_t graph_bytes = stats.graph_bytes;
	uint64_t classified = graph_bytes + stats.vector_bytes + lookup_bytes;
	if (index->memory_owner.bytes > classified)
		graph_bytes += index->memory_owner.bytes - classified;
	const char *distance = "l2";
	switch (base->def->opts.vector_distance) {
	case VECTOR_INDEX_DISTANCE_L2:
		break;
	case VECTOR_INDEX_DISTANCE_COSINE:
		distance = "cosine";
		break;
	case VECTOR_INDEX_DISTANCE_IP:
		distance = "ip";
		break;
	default:
		unreachable();
	}
	info_begin(handler);
	info_table_begin(handler, "config");
	info_append_int(handler, "dimension", index->dimension);
	info_append_str(handler, "distance", distance);
	info_append_str(handler, "algorithm", "hnsw");
	info_append_int(handler, "ef_search", base->def->opts.vector_ef_search);
	info_table_end(handler);
	info_table_begin(handler, "versions");
	info_append_int(handler, "live", live);
	info_append_int(handler, "retained", retained);
	info_append_int(handler, "retired", stats.retired);
	info_table_end(handler);
	info_table_begin(handler, "slots");
	info_append_int(handler, "capacity", stats.capacity);
	info_append_int(handler, "reusable", stats.reclaimable);
	info_table_end(handler);
	info_table_begin(handler, "memory");
	info_append_int(handler, "graph", graph_bytes);
	info_append_int(handler, "vectors",
			stats.vector_bytes - retained_vectors);
	info_append_int(handler, "lookup", lookup_bytes);
	info_append_int(handler, "retained", retained_vectors);
	info_append_int(handler, "temporary_peak", index->temporary_peak);
	info_append_int(handler, "total", index->memory_owner.bytes);
	info_table_begin(handler, "generations");
	info_append_int(handler, "current", index->memory_owner.bytes);
	info_table_end(handler);
	info_table_end(handler);
	info_table_begin(handler, "search");
	info_append_int(handler, "requests", index->search_requests);
	info_append_int(handler, "errors", index->search_errors);
	info_append_int(handler, "timeouts", index->search_timeouts);
	info_append_int(handler, "visited_candidates",
			index->visited_candidates);
	info_append_int(handler, "filtered_candidates",
			index->filtered_candidates);
	info_table_end(handler);
	info_table_begin(handler, "hnsw");
	info_append_int(handler, "generation", index->generation);
	info_append_int(handler, "m", base->def->opts.vector_m);
	info_append_int(handler, "ef_construction",
			base->def->opts.vector_ef_construction);
	info_table_end(handler);
	info_end(handler);
}

static struct index_read_view *
memtx_vector_index_create_read_view(struct index *base)
{
	(void)base;
	diag_set(ClientError, ER_VECTOR_UNSUPPORTED);
	return NULL;
}

/** Retire a version only after memtx has released all dependent readers. */
static void
memtx_vector_index_gc_tuple(struct index *base, struct tuple *tuple)
{
	struct memtx_vector_index *index = (struct memtx_vector_index *)base;
	uint64_t label = ann_tuple_map_find(&index->tuples, tuple);
	if (label == 0)
		return;
	enum ann_status status = ann_usearch_ops.set_live(index->backend,
							    label, false);
	assert(status == ANN_OK);
	status = ann_usearch_ops.reclaim(index->backend, label);
	assert(status == ANN_OK);
	uint64_t removed = ann_tuple_map_remove(&index->tuples, tuple);
	assert(removed == label);
	tuple_unref(tuple);
}

static void
memtx_vector_index_destroy(struct index *base)
{
	struct memtx_vector_index *index = (struct memtx_vector_index *)base;
	ann_usearch_ops.destroy(index->backend);
	for (uint32_t i = 0; i < index->tuples.count; ++i) {
		struct tuple *tuple = (struct tuple *)ann_tuple_map_get(
			&index->tuples, (uint64_t)i + 1);
		if (tuple != NULL)
			tuple_unref(tuple);
	}
	ann_tuple_map_destroy(&index->tuples);
	assert(index->memory_owner.blocks == 0);
	free(index);
}

/** Search width reads the current definition; structural changes rebuild. */
static bool
memtx_vector_index_def_change_requires_rebuild(
	struct index *base, const struct index_def *new_def)
{
	if (memtx_index_def_change_requires_rebuild(base, new_def))
		return true;
	const struct index_opts *old = &base->def->opts;
	const struct index_opts *new_opts = &new_def->opts;
	return old->dimension != new_opts->dimension ||
	       old->vector_distance != new_opts->vector_distance ||
	       old->vector_algorithm != new_opts->vector_algorithm ||
	       old->vector_m != new_opts->vector_m ||
	       old->vector_ef_construction !=
		new_opts->vector_ef_construction;
}

static const struct index_vtab memtx_vector_index_vtab_base = {
	/* .destroy = */ memtx_vector_index_destroy,
	/* .commit_create = */ generic_index_commit_create,
	/* .abort_create = */ generic_index_abort_create,
	/* .commit_modify = */ generic_index_commit_modify,
	/* .commit_drop = */ generic_index_commit_drop,
	/* .update_def = */ generic_index_update_def,
	/* .depends_on_pk = */ generic_index_depends_on_pk,
	/* .def_change_requires_rebuild = */
		memtx_vector_index_def_change_requires_rebuild,
	/* .size = */ memtx_vector_index_size,
	/* .bsize = */ memtx_vector_index_bsize,
	/* .quantile = */ generic_index_quantile,
	/* .min = */ generic_index_min,
	/* .max = */ generic_index_max,
	/* .random = */ generic_index_random,
	/* .count = */ generic_index_count,
	/* .get = */ memtx_index_get,
	/* .create_iterator = */ memtx_vector_index_create_iterator,
	/* .create_iterator_with_offset = */
	generic_index_create_iterator_with_offset,
	/* .create_arrow_stream = */ generic_index_create_arrow_stream,
	/* .create_read_view = */ memtx_vector_index_create_read_view,
	/* .info = */ generic_index_info,
	/* .stat = */ memtx_vector_index_stat,
	/* .compact = */ generic_index_compact,
	/* .reset_stat = */ generic_index_reset_stat,
};

static const struct memtx_index_vtab memtx_vector_index_vtab = {
	/* .base = */ memtx_vector_index_vtab_base,
	/* .get_internal = */ memtx_vector_index_get_internal,
	/* .replace = */ memtx_vector_index_replace,
	/* .begin_build = */ generic_memtx_index_begin_build,
	/* .reserve = */ generic_memtx_index_reserve,
	/* .build_next = */ generic_memtx_index_build_next,
	/* .end_build = */ generic_memtx_index_end_build,
	/* .gc_tuple = */ memtx_vector_index_gc_tuple,
};
struct index *
memtx_vector_index_new(struct memtx_engine *memtx, struct index_def *def)
{
	assert(def->iid > 0);
	assert(def->key_def->part_count == 1);
	assert(def->key_def->parts[0].type == FIELD_TYPE_ARRAY);
	assert(def->opts.is_unique == false);

	/* Checked by memtx_space_check_index_def(). */
	assert(def->opts.dimension >= 1 &&
	       def->opts.dimension <= MEMTX_VECTOR_MAX_DIMENSION);

	struct memtx_vector_index *index =
		(struct memtx_vector_index *)xcalloc(1, sizeof(*index));
	index_create(&index->base, (struct engine *)memtx,
		     &memtx_vector_index_vtab.base, def);

	ann_quota_memory_create(&index->memory_owner, &memtx->quota);
	struct ann_memory memory = ann_quota_memory_bind(&index->memory_owner);
	struct ann_config config = {};
	config.dimension = def->opts.dimension;
	switch (def->opts.vector_distance) {
	case VECTOR_INDEX_DISTANCE_L2:
		config.metric = ANN_L2;
		break;
	case VECTOR_INDEX_DISTANCE_COSINE:
		config.metric = ANN_COSINE;
		break;
	case VECTOR_INDEX_DISTANCE_IP:
		config.metric = ANN_IP;
		break;
	default:
		unreachable();
	}
	struct ann_usearch_config algorithm = {
		def->opts.vector_m,
		def->opts.vector_ef_construction,
		def->opts.vector_ef_search,
	};
	config.algorithm = &algorithm;
	enum ann_status status = ann_usearch_ops.create(&config, &memory,
							  &index->backend);
	if (vector_index_diag(status, "init") != 0) {
		free(index);
		return NULL;
	}
	ann_tuple_map_create(&index->tuples, &memory);

	index->dimension = def->opts.dimension;
	index->metric = config.metric;
	index->generation = ++vector_generation_next;
	return &index->base;
}
