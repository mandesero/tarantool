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
#include "small/rlist.h"
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
	/** Stable allocation owners for the active and private generations. */
	struct ann_quota_memory memory_owners[2];
	/** All backend allocations are charged to the memtx quota. */
	struct ann_quota_memory *memory_owner;
	/** Private generation being built or reclaimed, if any. */
	struct ann_quota_memory *transient_memory;
	/** Tuple bindings and embedding groups independent of generations. */
	struct ann_quota_memory common_memory;
	/** Coordinates shared by current and transient generations. */
	struct ann_quota_memory vector_memory;
	/** Unused quota reserved for the next maintenance pass. */
	struct ann_quota_memory maintenance;
	/** Group registry with reusable labels. */
	struct ann_tuple_map groups;
	/** Private graph receiving concurrent DML during rebuild. */
	struct ann_backend *builder;
	/** New groups admitted during this rebuild. */
	uint32_t builder_inserts;
	/** Bound on concurrent graph growth while migrating initial groups. */
	uint32_t builder_insert_limit;
	/** Background maintenance worker holding an index reference. */
	struct fiber *gc_fiber;
	/** Primary-index replacement used only during the next DML call. */
	struct tuple *replace_source;
	/** Completed maintenance passes. */
	uint64_t rebuilds;
	/** Failed maintenance passes. */
	uint64_t rebuild_errors;
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
	/** Cumulative time in admitted local searches, including failures. */
	uint64_t search_duration_ns;
	/** Cumulative backend work units visited by searches. */
	uint64_t visited_candidates;
	/** Cumulative predicate rejections, including visibility. */
	uint64_t filtered_candidates;
	/** Estimated peak temporary bytes allocated by this layer. */
	uint64_t temporary_peak;
};

/** TX-thread-local monotonic diagnostic generation counter. */
static uint64_t vector_generation_next;

/** One immutable embedding shared by versions of the same primary key. */
struct vector_group {
	/** Versions retaining this embedding, including rollback versions. */
	struct rlist versions;
	/** Coordinates shared with both graph generations. */
	struct ann_usearch_vector *vector;
	/** Recyclable storage label, independent of a graph slot. */
	uint64_t label;
	/** Number of bindings not explicitly retired by rollback or DML. */
	uint32_t live_count;
};

/** A tuple version remains independently visible after vector sharing. */
struct vector_binding {
	/** Link in the embedding's version list. */
	struct rlist link;
	/** Referenced tuple version. */
	struct tuple *tuple;
	/** Immutable embedding of this version. */
	struct vector_group *group;
	/** Whether this version participates in visibility checks. */
	bool live;
};

/** DML and rollback can omit either side of a tuple replacement. */
static struct vector_binding *
vector_binding_find(struct memtx_vector_index *index, struct tuple *tuple)
{
	if (tuple == NULL)
		return NULL;
	uint64_t label = ann_tuple_map_find(&index->tuples, tuple);
	return label == 0 ? NULL :
		(struct vector_binding *)index->tuples.values[label - 1];
}

static struct vector_group *
vector_group_get(struct memtx_vector_index *index, uint64_t label)
{
	return (struct vector_group *)ann_tuple_map_get(&index->groups, label);
}

/** Visibility is shared while each generation keeps its own graph slots. */
static void
vector_group_set_live(struct memtx_vector_index *index,
		      struct vector_group *group)
{
	ann_usearch_ops.set_live(index->backend, group->label,
				group->live_count != 0);
	if (index->builder != NULL &&
	    ann_usearch_contains(index->builder, group->label))
		ann_usearch_ops.set_live(index->builder, group->label,
					group->live_count != 0);
}

/** A retired version cannot hide another version sharing its embedding. */
static void
vector_binding_set_live(struct memtx_vector_index *index,
			struct vector_binding *binding, bool live)
{
	if (binding->live == live)
		return;
	binding->live = live;
	if (live)
		++binding->group->live_count;
	else
		--binding->group->live_count;
	vector_group_set_live(index, binding->group);
}

/** Background maintenance holds the index until private cleanup completes. */
static int
vector_auto_rebuild_f(va_list ap);

/** Schedule without yielding in a statement rollback or story GC. */
static void
vector_schedule_rebuild(struct memtx_vector_index *index)
{
	if (!box_is_configured() || index->gc_fiber != NULL ||
	    index->transient_memory != NULL)
		return;
	uint64_t size;
	uint64_t garbage = ann_usearch_reclaimable(index->backend, &size);
	if (garbage < 64 || garbage * 4 < size)
		return;
	struct fiber *worker =
		fiber_new_system("vector.gc", vector_auto_rebuild_f);
	if (worker == NULL) {
		diag_clear(diag_get());
		return;
	}
	index_ref(&index->base);
	index->gc_fiber = worker;
	fiber_set_ctx(worker, index);
	fiber_wakeup(worker);
}

/** Keep maintenance headroom outside the quota available to user writes. */
static bool
vector_reserve_maintenance(struct memtx_vector_index *index)
{
	if (index->builder != NULL)
		return index->builder_inserts < index->builder_insert_limit;
	size_t charged = index->memory_owner->charged;
	if (charged > (QUOTA_MAX - 65536) / 4)
		return false;
	return ann_quota_memory_reserve(&index->maintenance,
					charged * 4 + 65536);
}

/** Resolve a graph node to its own visible tuple, not its latest payload. */
static struct tuple *
vector_visible_tuple(struct vector_index_filter_ctx *filter, uint64_t label);

/** Read context shared by the ANN candidate predicate. */
struct vector_index_filter_ctx {
	/** Index whose label map is read. */
	struct memtx_vector_index *index;
	/** Space needed for memtx version clarification. */
	struct space *space;
	/** Transaction whose visible version is selected. */
	struct txn *txn;
	/** Shared search budget, including version-list traversal. */
	struct ann_search_control *control;
	/** First budget failure detected inside a predicate. */
	enum ann_status status;
	/** Optional one-based unsigned field number. */
	uint32_t fieldno;
	/** Sorted membership set, owned by the fiber region. */
	const uint64_t *values;
	/** Number of membership values. */
	uint32_t value_count;
};

static struct tuple *
vector_visible_tuple(struct vector_index_filter_ctx *filter, uint64_t label)
{
	struct vector_group *group = vector_group_get(filter->index, label);
	if (group == NULL)
		return NULL;
	struct vector_binding *binding;
	rlist_foreach_entry(binding, &group->versions, link) {
		filter->status = ann_search_control_step(filter->control);
		if (filter->status != ANN_OK)
			return NULL;
		if (!binding->live)
			continue;
		struct tuple *visible = memtx_tx_tuple_clarify(
			filter->txn, filter->space, binding->tuple,
			&filter->index->base, 0);
		if (visible == binding->tuple)
			return visible;
	}
	return NULL;
}

static bool
vector_index_filter(uint64_t label, void *ctx)
{
	struct vector_index_filter_ctx *filter =
		(struct vector_index_filter_ctx *)ctx;
	struct tuple *tuple = vector_visible_tuple(filter, label);
	if (tuple == NULL) {
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

/** Bound client HNSW work by the current fiber slice. */
static struct ann_search_control
vector_index_control(void)
{
	struct ann_search_control control = {};
	control.work_limit = UINT64_MAX;
	control.now_ns = vector_index_now_ns;
	control.should_stop = vector_index_should_stop;
	/* Recovery can rebuild an index for longer than a client slice. */
	if (!box_is_configured())
		return control;
	double deadline = cord()->call_time + cord()->slice.err;
	if (std::isfinite(deadline) && deadline > 0 &&
	    deadline < (double)UINT64_MAX / 1000000000.0)
		control.deadline_ns = (uint64_t)(deadline * 1000000000.0);
	return control;
}

static int
mp_decode_vector(double **vector, unsigned dimension,
		 const char *mp, unsigned count, const char *what)
{
	(void)what;
	if (count != dimension) {
		diag_set(ClientError, ER_VECTOR_INVALID,
			 "Vector dimension does not match index dimension");
		return -1;
	}
	for (unsigned i = 0; i < dimension; i++) {
		if (mp_read_double(&mp, &(*vector)[i]) != 0) {
			diag_set(ClientError, ER_VECTOR_INVALID,
				 "Vector coordinates must be numbers");
			return -1;
		}
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
	uint64_t started = vector_index_now_ns(NULL);
	uint64_t deadline = started +
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
	filter_ctx.control = &control;
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
		index->search_duration_ns +=
			vector_index_now_ns(NULL) - started;
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
	if (filter_ctx.status != ANN_OK)
		status = filter_ctx.status;
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
			struct tuple *left = vector_visible_tuple(
				&filter_ctx, candidates[j - 1].label);
			struct tuple *right = vector_visible_tuple(
				&filter_ctx, item.label);
			if (left == NULL || right == NULL) {
				vector_index_diag(filter_ctx.status,
						  "visibility");
				goto fail;
			}
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
		struct tuple *tuple = vector_visible_tuple(
			&filter_ctx, candidates[i].label);
		if (tuple == NULL) {
			vector_index_diag(filter_ctx.status, "visibility");
			goto fail;
		}
		port_c_add_tuple(port, tuple);
		if (distances != NULL)
			distances[out_count] = candidates[i].distance;
		++out_count;
	}
	region_truncate(region, svp);
	index->search_duration_ns += vector_index_now_ns(NULL) - started;
	return 0;
fail:
	++index->search_errors;
	if (box_error_code(diag_last_error(diag_get())) ==
	    ER_VECTOR_TIMEOUT)
		++index->search_timeouts;
	region_truncate(region, svp);
	index->search_duration_ns += vector_index_now_ns(NULL) - started;
	return -1;
}

int
memtx_vector_index_rebuild(struct index *base)
{
	struct memtx_vector_index *index = (struct memtx_vector_index *)base;
	if (index->transient_memory != NULL) {
		diag_set(ClientError, ER_MODIFY_INDEX, base->def->name,
			 base->def->space_name,
			 "VECTOR rebuild is in progress");
		return -1;
	}
	if (!vector_reserve_maintenance(index)) {
		diag_set(OutOfMemory, 0, "vector index", "rebuild reserve");
		return -1;
	}
	struct ann_quota_memory *owner =
		index->memory_owner == &index->memory_owners[0] ?
		&index->memory_owners[1] : &index->memory_owners[0];
	ann_quota_memory_create(owner, index->memory_owner->quota);
	owner->reserved = index->maintenance.reserved;
	index->maintenance.reserved = 0;
	owner->use_reserve = true;
	struct ann_memory memory = ann_quota_memory_bind(owner);
	struct ann_usearch_config algorithm = {
		base->def->opts.vector_m,
		base->def->opts.vector_ef_construction,
		base->def->opts.vector_ef_search,
	};
	struct ann_config config = {index->dimension, index->metric,
				    &algorithm};
	struct ann_backend *replacement = NULL;
	struct ann_search_control control = vector_index_control();
	struct index_weak_ref ref;
	index_weak_ref_create(&ref, base);
	uint32_t end = index->groups.count;
	int64_t yield_deadline = clock_monotonic64() + 1000000;
	int rc = -1;
	index_ref(base);
	index->transient_memory = owner;
	index->builder_inserts = 0;
	index->builder_insert_limit = MAX(64, index->groups.live_count / 4);
	enum ann_status status = ann_usearch_ops.create(&config, &memory,
						      &replacement);
	if (status != ANN_OK)
		goto fail_status;
	index->builder = replacement;
	owner->use_reserve = false;
	for (uint32_t i = 0; i < end; ++i) {
		if (box_check_slice() != 0)
			goto out;
		bool must_yield = clock_monotonic64() >= yield_deadline;
		ERROR_INJECT(ERRINJ_VECTOR_REBUILD_DELAY, {
			must_yield = true;
		});
		if (must_yield) {
			ERROR_INJECT_YIELD(ERRINJ_VECTOR_REBUILD_DELAY);
			fiber_sleep(0);
			if (fiber_is_cancelled()) {
				diag_set(FiberIsCancelled);
				goto out;
			}
			if (!index_weak_ref_check(&ref)) {
				diag_set(ClientError, ER_MODIFY_INDEX,
					 base->def->name, base->def->space_name,
					 "VECTOR replaced during rebuild");
				goto out;
			}
			control = vector_index_control();
			yield_deadline = clock_monotonic64() + 1000000;
		}
		struct vector_group *group = vector_group_get(index, i + 1);
		if (group == NULL ||
		    ann_usearch_contains(replacement, group->label))
			continue;
		struct ann_change *change = NULL;
		owner->use_reserve = true;
		status = ann_usearch_prepare_shared(
			replacement, group->label, group->vector, &change);
		if (status == ANN_OK) {
			status = ann_usearch_ops.apply(change, &control);
			if (status != ANN_OK)
				ann_usearch_ops.rollback(change);
			ann_usearch_ops.finish(change);
		}
		owner->use_reserve = false;
		if (status != ANN_OK)
			goto fail_status;
		ann_usearch_ops.set_live(replacement, group->label,
					group->live_count != 0);
		if (owner->bytes > index->temporary_peak)
			index->temporary_peak = owner->bytes;
	}
	{
		struct ann_backend *old_backend = index->backend;
		struct ann_quota_memory *old_owner = index->memory_owner;
		index->backend = replacement;
		index->memory_owner = owner;
		index->generation = ++vector_generation_next;
		index->transient_memory = old_owner;
		replacement = old_backend;
		ann_quota_memory_reserve(owner, 0);
		owner->use_reserve = false;
		owner = old_owner;
	}
	rc = 0;
	goto out;
fail_status:
	vector_index_diag(status, "rebuild");
out:
	index->builder = NULL;
	owner->use_reserve = false;
	ann_quota_memory_reserve(owner, 0);
	ann_quota_memory_start_gc(owner);
	ERROR_INJECT_YIELD(ERRINJ_VECTOR_REBUILD_GC_DELAY);
	ann_usearch_ops.destroy(replacement);
	assert(owner->blocks == 0);
	index->transient_memory = NULL;
	if (rc == 0) {
		++index->rebuilds;
		if (index_weak_ref_check(&ref))
			vector_schedule_rebuild(index);
	} else {
		++index->rebuild_errors;
	}
	/* DROP can leave this fiber as the last owner of the index. */
	if (base->refs == 1) {
		ann_quota_memory_start_gc(index->memory_owner);
		ann_quota_memory_start_gc(&index->common_memory);
		ann_quota_memory_start_gc(&index->vector_memory);
	}
	index_unref(base);
	return rc;
}

static int
vector_auto_rebuild_f(va_list ap)
{
	(void)ap;
	struct memtx_vector_index *index = (struct memtx_vector_index *)
		fiber_get_ctx(fiber());
	struct index_weak_ref ref;
	index_weak_ref_create(&ref, &index->base);
	int rc = -1;
	if (index_weak_ref_check(&ref) && index->transient_memory == NULL)
		rc = memtx_vector_index_rebuild(&index->base);
	index->gc_fiber = NULL;
	if (rc == 0 && index_weak_ref_check(&ref))
		vector_schedule_rebuild(index);
	index_unref(&index->base);
	return 0;
}

/** Share only within one primary-key history, never between distinct rows. */
static enum ann_status
vector_prepare_group(struct memtx_vector_index *index, struct tuple *tuple,
		     struct tuple *source, struct vector_group **out)
{
	struct index_def *def = index->base.def;
	struct vector_binding *previous = vector_binding_find(index, source);
	struct space *space = space_by_id(def->space_id);
	bool same_row = previous != NULL &&
		tuple_compare(source, HINT_NONE, tuple, HINT_NONE,
			      space->index[0]->def->key_def) == 0;
	if (same_row) {
		const char *a = tuple_field_by_part(source, def->key_def->parts,
						    MULTIKEY_NONE);
		const char *b = tuple_field_by_part(tuple, def->key_def->parts,
						    MULTIKEY_NONE);
		const char *a_end = a;
		const char *b_end = b;
		mp_next(&a_end);
		mp_next(&b_end);
		if (a_end - a == b_end - b && memcmp(a, b, a_end - a) == 0) {
			*out = previous->group;
			return ANN_OK;
		}
	}
	struct region *region = &fiber()->gc;
	double *decoded = xregion_alloc_array(region, double, index->dimension);
	float *canonical = xregion_alloc_array(region, float, index->dimension);
	if (extract_vector(&decoded, tuple, def) != 0)
		return ANN_INVALID;
	enum ann_status status = ann_vector_from_double(
		decoded, index->dimension, index->metric, canonical);
	if (status != ANN_OK)
		return status;
	if (same_row) {
		const void *data =
			ann_usearch_vector_data(previous->group->vector);
		size_t size = index->dimension * sizeof(float);
		if (memcmp(canonical, data, size) == 0) {
			*out = previous->group;
			return ANN_OK;
		}
	}
	if (!vector_reserve_maintenance(index))
		return ANN_OUT_OF_MEMORY;
	status = ann_tuple_map_prepare(&index->groups);
	if (status != ANN_OK)
		return status;
	struct ann_memory memory = index->groups.memory;
	struct vector_group *group = (struct vector_group *)
		memory.alloc(memory.ctx, sizeof(*group));
	if (group == NULL)
		return ANN_OUT_OF_MEMORY;
	rlist_create(&group->versions);
	group->live_count = 0;
	struct ann_memory vector_memory =
		ann_quota_memory_bind(&index->vector_memory);
	group->vector = ann_usearch_vector_new(&vector_memory, canonical,
					       index->dimension);
	if (group->vector == NULL) {
		memory.free(memory.ctx, group, sizeof(*group));
		return ANN_OUT_OF_MEMORY;
	}
	group->label = ann_tuple_map_insert(&index->groups, group);
	*out = group;
	return ANN_OK;
}

void
memtx_vector_index_set_replace_source(struct index *base, struct tuple *tuple)
{
	((struct memtx_vector_index *)base)->replace_source = tuple;
}

static int
memtx_vector_index_replace(struct index *base, struct tuple *old_tuple,
			  struct tuple *new_tuple, enum dup_replace_mode mode,
			  struct tuple **result, struct tuple **successor)
{
	(void)mode;
	struct memtx_vector_index *index = (struct memtx_vector_index *)base;
	struct tuple *source = index->replace_source;
	index->replace_source = NULL;
	if (source == NULL)
		source = old_tuple;
	*successor = NULL;
	if (old_tuple == new_tuple) {
		*result = old_tuple;
		return 0;
	}
	struct vector_binding *old = vector_binding_find(index, old_tuple);
	struct vector_binding *existing = vector_binding_find(index, new_tuple);
	if (old_tuple != NULL && old == NULL) {
		vector_index_diag(ANN_NOT_FOUND, "old tuple");
		return -1;
	}
	struct region *region = &fiber()->gc;
	size_t svp = region_used(region);
	struct ann_memory memory = ann_quota_memory_bind(&index->common_memory);
	struct vector_binding *binding = NULL;
	struct vector_group *group = NULL;
	struct ann_change *changes[2] = {NULL, NULL};
	struct ann_search_control control = vector_index_control();
	enum ann_status status = ANN_OK;
	bool new_group = false;
	bool old_live = old != NULL && old->live;
	if (new_tuple != NULL && existing == NULL) {
		status = ann_tuple_map_prepare(&index->tuples);
		if (status != ANN_OK)
			goto fail;
		binding = (struct vector_binding *)
			memory.alloc(memory.ctx, sizeof(*binding));
		if (binding == NULL) {
			status = ANN_OUT_OF_MEMORY;
			goto fail;
		}
		status = vector_prepare_group(index, new_tuple, source, &group);
		if (status != ANN_OK)
			goto fail;
		new_group = !ann_usearch_contains(index->backend, group->label);
		if (new_group) {
			struct ann_backend *backends[2] = {index->backend,
							  index->builder};
			for (int i = 0; i < 2 && backends[i] != NULL; ++i) {
				status = ann_usearch_prepare_shared(
					backends[i], group->label,
					group->vector, &changes[i]);
				if (status == ANN_OK) {
					status = ann_usearch_ops.apply(
						changes[i], &control);
				}
				if (status != ANN_OK)
					goto fail;
			}
		}
	}
	if (old != NULL &&
	    (!memtx_tx_manager_use_mvcc_engine || binding == NULL))
		vector_binding_set_live(index, old, false);
	if (existing != NULL)
		vector_binding_set_live(index, existing, true);
	if (binding != NULL) {
		binding->tuple = new_tuple;
		binding->group = group;
		binding->live = true;
		rlist_add_entry(&group->versions, binding, link);
		++group->live_count;
		uint64_t label =
			ann_tuple_map_insert(&index->tuples, new_tuple);
		index->tuples.values[label - 1] = (uintptr_t)binding;
		tuple_ref(new_tuple);
		vector_group_set_live(index, group);
		if (new_group && index->builder != NULL)
			++index->builder_inserts;
	}
	for (int i = 1; i >= 0; --i)
		ann_usearch_ops.finish(changes[i]);
	region_truncate(region, svp);
	*result = old_tuple;
	return 0;
fail:
	vector_index_diag(status, "replace");
	for (int i = 1; i >= 0; --i) {
		ann_usearch_ops.rollback(changes[i]);
		ann_usearch_ops.finish(changes[i]);
	}
	if (new_group) {
		ann_tuple_map_remove(&index->groups, group);
		ann_usearch_vector_unref(group->vector);
		memory.free(memory.ctx, group, sizeof(*group));
	}
	if (binding != NULL)
		memory.free(memory.ctx, binding, sizeof(*binding));
	if (old != NULL)
		vector_binding_set_live(index, old, old_live);
	region_truncate(region, svp);
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
	return index->common_memory.bytes + index->vector_memory.bytes +
		index->memory_owner->bytes + (index->transient_memory == NULL ?
		0 : index->transient_memory->bytes);
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
	uint64_t retained = index->tuples.live_count > live ?
		index->tuples.live_count - live : 0;
	uint64_t vector_bytes = (uint64_t)index->vector_memory.blocks *
		index->dimension * sizeof(float);
	uint64_t active_vectors = (uint64_t)index->groups.live_count *
		index->dimension * sizeof(float);
	uint64_t retained_vectors = vector_bytes - active_vectors;
	uint64_t lookup_bytes = stats.lookup_bytes +
		index->common_memory.bytes +
		index->vector_memory.bytes - vector_bytes;
	uint64_t graph_bytes = stats.graph_bytes +
		index->memory_owner->bytes - stats.resident_bytes;
	uint64_t current_bytes = index->memory_owner->bytes +
		index->common_memory.bytes + index->vector_memory.bytes;
	uint64_t transient = index->transient_memory == NULL ? 0 :
		index->transient_memory->bytes;
	uint64_t reserved = index->maintenance.reserved +
		(index->transient_memory == NULL ? 0 :
		 index->transient_memory->reserved);
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
	info_append_str(handler, "scalar", "float32");
	info_append_str(handler, "numeric_contract", "f32_f64_v1");
	info_append_int(handler, "ef_search", base->def->opts.vector_ef_search);
	info_table_end(handler);
	info_table_begin(handler, "limits");
	info_append_int(handler, "dimension", MEMTX_VECTOR_MAX_DIMENSION);
	info_append_int(handler, "limit", 1024);
	info_append_int(handler, "ef_search", 8192);
	info_append_int(handler, "filter_values", 65536);
	info_append_double(handler, "timeout", 30);
	info_append_double(handler, "default_timeout", 1);
	info_append_int(handler, "work", 1ULL << 32);
	info_table_end(handler);
	info_table_begin(handler, "versions");
	info_append_int(handler, "live", live);
	info_append_int(handler, "retained", retained);
	info_append_int(handler, "retired", stats.retired);
	info_table_end(handler);
	info_table_begin(handler, "slots");
	info_append_int(handler, "capacity", stats.capacity);
	info_append_int(handler, "pending_rebuild", stats.reclaimable);
	info_append_int(handler, "embeddings", index->groups.live_count);
	info_table_end(handler);
	info_table_begin(handler, "memory");
	info_append_int(handler, "graph", graph_bytes);
	info_append_int(handler, "vectors",
			active_vectors);
	info_append_int(handler, "lookup", lookup_bytes);
	info_append_int(handler, "retained", retained_vectors);
	info_append_int(handler, "temporary_peak", index->temporary_peak);
	info_append_int(handler, "maintenance_reserved", reserved);
	info_append_int(handler, "transient", transient);
	info_append_int(handler, "total",
			current_bytes + transient);
	info_table_begin(handler, "generations");
	info_append_int(handler, "current", current_bytes);
	info_append_int(handler, "transient", transient);
	info_table_end(handler);
	info_table_end(handler);
	info_table_begin(handler, "search");
	info_append_int(handler, "requests", index->search_requests);
	info_append_int(handler, "errors", index->search_errors);
	info_append_int(handler, "timeouts", index->search_timeouts);
	info_append_int(handler, "duration_ns", index->search_duration_ns);
	info_append_int(handler, "visited_candidates",
			index->visited_candidates);
	info_append_int(handler, "filtered_candidates",
			index->filtered_candidates);
	info_table_end(handler);
	info_table_begin(handler, "maintenance");
	info_append_int(handler, "rebuilds", index->rebuilds);
	info_append_int(handler, "errors", index->rebuild_errors);
	info_append_bool(handler, "running", index->transient_memory != NULL);
	info_append_int(handler, "admitted", index->builder_inserts);
	info_append_int(handler, "admission_limit",
			index->builder_insert_limit);
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

/** Retire a binding only after memtx releases its dependent readers. */
static void
memtx_vector_index_gc_tuple(struct index *base, struct tuple *tuple)
{
	struct memtx_vector_index *index = (struct memtx_vector_index *)base;
	struct vector_binding *binding =
		vector_binding_find(index, tuple);
	if (binding == NULL)
		return;
	struct vector_group *group = binding->group;
	vector_binding_set_live(index, binding, false);
	rlist_del_entry(binding, link);
	ann_tuple_map_remove(&index->tuples, tuple);
	struct ann_memory memory = ann_quota_memory_bind(&index->common_memory);
	memory.free(memory.ctx, binding, sizeof(*binding));
	tuple_unref(tuple);
	if (rlist_empty(&group->versions)) {
		ann_usearch_ops.reclaim(index->backend, group->label);
		if (index->builder != NULL &&
		    ann_usearch_contains(index->builder, group->label))
			ann_usearch_ops.reclaim(index->builder, group->label);
		ann_tuple_map_remove(&index->groups, group);
		ann_usearch_vector_unref(group->vector);
		memory.free(memory.ctx, group, sizeof(*group));
		vector_schedule_rebuild(index);
	}
}

static void
memtx_vector_index_destroy(struct index *base)
{
	struct memtx_vector_index *index = (struct memtx_vector_index *)base;
	assert(index->transient_memory == NULL && index->gc_fiber == NULL);
	ann_quota_memory_reserve(&index->maintenance, 0);
	ann_usearch_ops.destroy(index->backend);
	for (uint32_t i = 0; i < index->tuples.count; ++i) {
		struct tuple *tuple = (struct tuple *)ann_tuple_map_get(
			&index->tuples, (uint64_t)i + 1);
		if (tuple == NULL)
			continue;
		struct vector_binding *binding =
		vector_binding_find(index, tuple);
		tuple_unref(tuple);
		struct ann_memory memory = index->tuples.memory;
		memory.free(memory.ctx, binding, sizeof(*binding));
	}
	for (uint32_t i = 0; i < index->groups.count; ++i) {
		struct vector_group *group = vector_group_get(index, i + 1);
		if (group == NULL)
			continue;
		ann_usearch_vector_unref(group->vector);
		struct ann_memory memory = index->groups.memory;
		memory.free(memory.ctx, group, sizeof(*group));
	}
	ann_tuple_map_destroy(&index->tuples);
	ann_tuple_map_destroy(&index->groups);
	assert(index->memory_owner->blocks == 0);
	assert(index->common_memory.blocks == 0);
	assert(index->vector_memory.blocks == 0);
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
	index->memory_owner = &index->memory_owners[0];
	index_create(&index->base, (struct engine *)memtx,
		     &memtx_vector_index_vtab.base, def);

	ann_quota_memory_create(index->memory_owner, &memtx->quota);
	ann_quota_memory_create(&index->common_memory, &memtx->quota);
	ann_quota_memory_create(&index->vector_memory, &memtx->quota);
	ann_quota_memory_create(&index->maintenance, &memtx->quota);
	struct ann_memory common = ann_quota_memory_bind(&index->common_memory);
	ann_tuple_map_create(&index->tuples, &common);
	ann_tuple_map_create(&index->groups, &common);
	struct ann_memory memory = ann_quota_memory_bind(index->memory_owner);
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
		index_unref(&index->base);
		return NULL;
	}


	index->dimension = def->opts.dimension;
	index->metric = config.metric;
	index->generation = ++vector_generation_next;
	return &index->base;
}
