#include "memtx_vector.h"

#include <small/small.h>
#include <small/mempool.h>

#include "index.h"
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
#include "lib/ann/ann_numeric.h"
#include "lib/ann/ann_usearch.h"

#include <cmath>

struct memtx_vector_index {
	/** Generic index interface. */
	struct index base;
	/** Number of scalar coordinates in each vector. */
	unsigned dimension;
	/** All backend allocations are charged to the memtx quota. */
	struct ann_quota_memory memory_owner;
	/** Current HNSW generation. */
	struct ann_backend *backend;
};

/**
 * How many neighbours one search asks usearch for. The index API does not
 * pass the select limit down, so this is the upper bound on what a single
 * iterator can return.
 */
#define MEMTX_VECTOR_NEIGHBOURS 32

struct index_vector_iterator {
	struct iterator base;
	/** Neighbours found by the search, nearest first. */
	uint64_t keys[MEMTX_VECTOR_NEIGHBOURS];
	/** How many of them were found. */
	size_t count;
	/** Position of the next neighbour to return. */
	size_t pos;
	/** Memory pool the iterator was allocated from. */
	struct mempool *pool;
};

/**
 * A usearch key is the tuple pointer itself: the index keeps no copy of the
 * data and a search hands the tuples back directly.
 */
static inline uint64_t
vector_index_key(struct tuple *tuple)
{
	return (uint64_t)(uintptr_t)tuple;
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
		diag_set(FiberSliceIsExceeded);
	} else if (status == ANN_STOPPED) {
		diag_set(FiberIsCancelled);
	} else {
		diag_set(ClientError, ER_SYSTEM,
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
	(void)key;
	(void)part_count;
	(void)result;
	(void)is_rw;
	diag_set(UnsupportedIndexFeature, base->def, "get");
	return -1;
}

static int
index_vector_iterator_next(struct iterator *i, struct tuple **ret)
{
	struct index_vector_iterator *itr = (struct index_vector_iterator *)i;
	struct space *space;
	struct index *index;
	index_weak_ref_get_checked(&i->index_ref, &space, &index);
	struct txn *txn = in_txn();

	while (itr->pos < itr->count) {
		struct tuple *tuple =
			(struct tuple *)(uintptr_t)itr->keys[itr->pos++];
		tuple = memtx_tx_tuple_clarify(txn, space, tuple, index, 0);
		if (tuple != NULL) {
			*ret = tuple;
			return 0;
		}
	}
	*ret = NULL;
	return 0;
}

static void
index_vector_iterator_free(struct iterator *i)
{
	struct index_vector_iterator *itr = (struct index_vector_iterator *)i;
	mempool_free(itr->pool, itr);
}

/** Implementation of create_iterator for memtx vector index. */
static struct iterator *
memtx_vector_index_create_iterator(struct index *base, enum iterator_type type,
				  const char *key, uint32_t part_count,
				  const char *pos)
{
	struct memtx_vector_index *index = (struct memtx_vector_index *)base;
	struct memtx_engine *memtx = (struct memtx_engine *)base->engine;

	if (pos != NULL) {
		diag_set(UnsupportedIndexFeature, base->def, "pagination");
		return NULL;
	}
	/*
	 * A vector index answers one question: which tuples are nearest to
	 * this vector. Anything else, a full scan included, belongs to
	 * another index of the space.
	 */
	if (type != ITER_EQ || part_count == 0) {
		diag_set(UnsupportedIndexFeature, base->def,
			 "iterator type other than EQ with a vector key");
		return NULL;
	}

	struct region *region = &fiber()->gc;
	size_t region_svp = region_used(region);
	double *vector = xregion_alloc_array(region, double, index->dimension);
	int rc = mp_decode_vector_from_key(&vector, index->dimension,
					   key, part_count);
	struct ann_candidate candidates[MEMTX_VECTOR_NEIGHBOURS];
	uint32_t count = 0;
	if (rc == 0) {
		float *query = xregion_alloc_array(region, float,
					     index->dimension);
		enum ann_status status = ann_vector_from_double(vector,
			index->dimension, ANN_COSINE, query);
		if (status == ANN_OK) {
			struct ann_search_control control =
				vector_index_control();
			struct ann_search_opts opts = {};
			opts.candidate_limit = MEMTX_VECTOR_NEIGHBOURS;
			opts.query_dimension = index->dimension;
			opts.control = &control;
			status = ann_usearch_ops.search(index->backend, query,
						  &opts, candidates, &count);
		}
		rc = vector_index_diag(status, "search");
	}
	region_truncate(region, region_svp);
	if (rc != 0)
		return NULL;

	struct index_vector_iterator *it = (struct index_vector_iterator *)
		mempool_alloc(&memtx->iterator_pool);
	if (it == NULL) {
		diag_set(OutOfMemory, sizeof(struct index_vector_iterator),
			 "memtx_vector_index", "iterator");
		return NULL;
	}

	iterator_create(&it->base, base);
	it->pool = &memtx->iterator_pool;
	it->base.next_internal = index_vector_iterator_next;
	it->base.next = memtx_iterator_next;
	it->base.position = generic_iterator_position;
	it->base.free = index_vector_iterator_free;
	for (uint32_t i = 0; i < count; ++i)
		it->keys[i] = candidates[i].label;
	it->count = count;
	it->pos = 0;

	return (struct iterator *)it;
}

static int
memtx_vector_index_replace(struct index *base, struct tuple *old_tuple,
			  struct tuple *new_tuple, enum dup_replace_mode mode,
			  struct tuple **result, struct tuple **successor)
{
	(void)mode;
	struct memtx_vector_index *index = (struct memtx_vector_index *)base;
	struct ann_change *old_change = NULL;
	struct ann_change *new_change = NULL;
	struct ann_search_control control = vector_index_control();
	*successor = NULL;

	struct region *region = &fiber()->gc;
	size_t region_svp = region_used(region);
	float *canonical = NULL;
	if (new_tuple != NULL) {
		double *vector = xregion_alloc_array(region, double,
						       index->dimension);
		canonical = xregion_alloc_array(region, float,
						  index->dimension);
		if (extract_vector(&vector, new_tuple, base->def) != 0)
			goto fail;
		enum ann_status status = ann_vector_from_double(vector,
			index->dimension, ANN_COSINE, canonical);
		if (vector_index_diag(status, "convert") != 0)
			goto fail;
	}
	if (old_tuple != NULL) {
		enum ann_status status = ann_usearch_ops.prepare(index->backend,
			ANN_RETIRE, vector_index_key(old_tuple), NULL, 0,
			&old_change);
		if (vector_index_diag(status, "retire") != 0)
			goto fail;
		status = ann_usearch_ops.apply(old_change, &control);
		if (vector_index_diag(status, "retire") != 0)
			goto fail;
	}
	if (new_tuple != NULL) {
		enum ann_status status = ann_usearch_ops.prepare(index->backend,
			ANN_INSERT, vector_index_key(new_tuple), canonical,
			index->dimension, &new_change);
		if (vector_index_diag(status, "prepare") != 0)
			goto fail;
		status = ann_usearch_ops.apply(new_change, &control);
		if (vector_index_diag(status, "insert") != 0)
			goto fail;
	}
	ann_usearch_ops.finish(new_change);
	ann_usearch_ops.finish(old_change);
	if (old_tuple != NULL) {
		enum ann_status status = ann_usearch_ops.reclaim(
			index->backend, vector_index_key(old_tuple));
		assert(status == ANN_OK);
	}
	region_truncate(region, region_svp);
	*result = old_tuple;
	return 0;
fail:
	ann_usearch_ops.rollback(new_change);
	ann_usearch_ops.finish(new_change);
	ann_usearch_ops.rollback(old_change);
	ann_usearch_ops.finish(old_change);
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
	struct ann_backend_stats stats;
	ann_usearch_ops.stat(index->backend, &stats);
	return stats.resident_bytes;
}

static void
memtx_vector_index_destroy(struct index *base)
{
	struct memtx_vector_index *index = (struct memtx_vector_index *)base;
	ann_usearch_ops.destroy(index->backend);
	assert(index->memory_owner.blocks == 0);
	free(index);
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
		generic_index_def_change_requires_rebuild,
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
	/* .create_read_view = */ generic_index_create_read_view,
	/* .info = */ generic_index_info,
	/* .stat = */ generic_index_stat,
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

	// TODO: try different distance types.

	struct memtx_vector_index *index =
		(struct memtx_vector_index *)xcalloc(1, sizeof(*index));
	index_create(&index->base, (struct engine *)memtx,
		     &memtx_vector_index_vtab.base, def);

	ann_quota_memory_create(&index->memory_owner, &memtx->quota);
	struct ann_memory memory = ann_quota_memory_bind(&index->memory_owner);
	struct ann_config config = {};
	config.dimension = def->opts.dimension;
	config.metric = ANN_COSINE;
	enum ann_status status = ann_usearch_ops.create(&config, &memory,
							  &index->backend);
	if (vector_index_diag(status, "init") != 0) {
		free(index);
		return NULL;
	}

	index->dimension = def->opts.dimension;
	return &index->base;
}
