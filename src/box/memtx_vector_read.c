/*
 * SPDX-License-Identifier: BSD-2-Clause
 *
 * Copyright 2026, Tarantool AUTHORS, please see AUTHORS file.
 */
#include "memtx_vector.h"

#include "box.h"
#include "clock.h"
#include "fiber.h"
#include "index.h"
#include "memtx_engine.h"
#include "mp_embedding.h"
#include "port.h"
#include "space.h"
#include "space_cache.h"
#include "tuple.h"
#include "txn.h"
#include "small/region.h"
#include "lib/ann/ann_numeric.h"

/** Bounded top-k is exposed through the same next/free cursor contract. */
struct vector_search_iterator {
	/** Generic index iterator API. */
	struct iterator base;
	/** Pinned index owning the backend. */
	struct index *index;
	/** Options whose variable-sized buffers follow this object. */
	struct memtx_vector_search_opts opts;
	/** Retained top-k tuples. */
	struct port port;
	/** Next result in the materialized bounded batch. */
	struct port_c_entry *entry;
	/** Whether the port has been initialized. */
	bool started;
};

static void
vector_search_iterator_free(struct iterator *base)
{
	struct vector_search_iterator *it =
		(struct vector_search_iterator *)base;
	if (it->started)
		port_destroy(&it->port);
	index_unref(it->index);
	free(it);
}

/** The bounded batch is built on first next, not during cursor creation. */
static int
vector_search_iterator_next(struct iterator *base, struct tuple **out)
{
	struct vector_search_iterator *it =
		(struct vector_search_iterator *)base;
	*out = NULL;
	if (!it->started) {
		port_c_create(&it->port);
		it->started = true;
		if (memtx_vector_index_search(it->index, &it->opts,
					      &it->port, NULL) != 0)
			return -1;
		it->entry = ((struct port_c *)&it->port)->first;
	}
	if (it->entry != NULL) {
		*out = it->entry->tuple;
		it->entry = it->entry->next;
	}
	return 0;
}

struct iterator *
memtx_vector_search_iterator_new(struct index *index,
				 const struct memtx_vector_search_opts *opts)
{
	size_t key_size = opts->key_end - opts->key;
	size_t filter_size = opts->filter == NULL ? 0 :
		opts->filter_end - opts->filter;
	size_t size = sizeof(struct vector_search_iterator) + key_size +
		      filter_size;
	struct vector_search_iterator *it = xcalloc(1, size);
	iterator_create(&it->base, index);
	it->base.next_internal = vector_search_iterator_next;
	it->base.next = memtx_iterator_next;
	it->base.position = generic_iterator_position;
	it->base.free = vector_search_iterator_free;
	it->index = index;
	index_ref(index);
	it->opts = *opts;
	it->opts.key = (char *)(it + 1);
	memcpy((void *)it->opts.key, opts->key, key_size);
	it->opts.key_end = it->opts.key + key_size;
	if (filter_size != 0) {
		it->opts.filter = it->opts.key_end;
		memcpy((void *)it->opts.filter, opts->filter, filter_size);
		it->opts.filter_end = it->opts.filter + filter_size;
	}
	return &it->base;
}

ssize_t
memtx_vector_index_count(struct index *index, enum iterator_type type,
			 const char *key, uint32_t part_count)
{
	(void)key;
	struct txn *txn = in_txn();
	if ((txn != NULL && txn->isolation == TXN_ISOLATION_LINEARIZABLE) ||
	    type != ITER_ALL || part_count != 0) {
		diag_set(ClientError, ER_VECTOR_UNSUPPORTED);
		return -1;
	}
	struct space *space = space_by_id(index->def->space_id);
	return index_count(space->index[0], ITER_ALL, NULL, 0);
}

enum { VECTOR_SCAN_BATCH = 32 };

/** One retained result in a bounded exact-search batch. */
struct vector_scan_row {
	/** Owned copy of frozen tuple data. */
	struct read_view_tuple raw;
	/** Distance in the common numeric contract. */
	double distance;
};

/** Exact ordered scan with memory independent of the number of rows. */
struct vector_scan {
	/** Frozen primary index for a public read view. */
	struct index_read_view *view;
	/** Owned primary-key comparator operating on raw tuple data. */
	struct key_def *pk;
	/** Owned vector field definition. */
	struct key_def *key_def;
	/** Configured scalar dimension. */
	uint32_t dimension;
	/** Configured distance function. */
	enum ann_metric metric;
	/** Whether the source has been exhausted. */
	bool exhausted;
	/** Results retained between calls to next. */
	struct vector_scan_row rows[VECTOR_SCAN_BATCH];
	/** Number of retained rows. */
	uint32_t count;
	/** Next row in the current batch. */
	uint32_t pos;
	/** Last emitted row, retained as the next scan's lower bound. */
	struct vector_scan_row last;
	/** Canonical query. */
	float query[MEMTX_VECTOR_MAX_DIMENSION];
};

/** A result owns a copy of frozen tuple data. */
static void
vector_scan_row_clear(struct vector_scan_row *row)
{
	free((void *)row->raw.data);
	memset(row, 0, sizeof(*row));
}

static void
vector_scan_delete(struct vector_scan *scan)
{
	for (uint32_t i = 0; i < scan->count; ++i)
		vector_scan_row_clear(&scan->rows[i]);
	vector_scan_row_clear(&scan->last);
	key_def_delete(scan->pk);
	key_def_delete(scan->key_def);
	free(scan);
}

/** Exact iteration must use the same canonical coordinates as HNSW. */
static int
vector_scan_decode(const char *data, uint32_t dimension,
		   enum ann_metric metric, float *out)
{
	if (data == NULL)
		goto invalid;

	if (mp_typeof(*data) == MP_EXT) {
		uint32_t size;
		const char *bytes = mp_decode_embedding(&data, &size);
		if (bytes == NULL || size != dimension)
			goto invalid;
		for (uint32_t i = 0; i < dimension; ++i)
			out[i] = embedding_f32le_load(bytes, i);
	} else {
		if (mp_typeof(*data) != MP_ARRAY ||
		    mp_decode_array(&data) != dimension)
			goto invalid;
		for (uint32_t i = 0; i < dimension; ++i) {
			double value;
			if (mp_read_double(&data, &value) != 0 ||
			    ann_f32_from_double(value, &out[i]) != ANN_OK)
				goto invalid;
		}
	}
	if (ann_vector_validate_f32(out, dimension, metric) == ANN_OK)
		return 0;
invalid:
	diag_set(ClientError, ER_VECTOR_INVALID);
	return -1;
}

static struct vector_scan *
vector_scan_new(struct index_read_view *view, struct index_def *def,
		struct key_def *pk, const char *key, uint32_t part_count)
{
	if (part_count != 1) {
		diag_set(ClientError, ER_VECTOR_UNSUPPORTED);
		return NULL;
	}
	struct vector_scan *scan = xcalloc(1, sizeof(*scan));
	scan->view = view;
	scan->dimension = def->opts.dimension;
	scan->metric = (enum ann_metric)def->opts.vector_distance;
	scan->pk = key_def_dup(pk);
	scan->key_def = key_def_dup(def->key_def);
	if (vector_scan_decode(key, scan->dimension, scan->metric,
			       scan->query) != 0) {
		vector_scan_delete(scan);
		return NULL;
	}
	return scan;
}

/** Use the complete primary-key contract to resolve equal distances. */
static int
vector_scan_compare(struct vector_scan *scan,
		    const struct vector_scan_row *a,
		    const struct vector_scan_row *b)
{
	if (a->distance != b->distance)
		return a->distance < b->distance ? -1 : 1;
	for (uint32_t i = 0; i < scan->pk->part_count; ++i) {
		struct key_part *part = &scan->pk->parts[i];
		const char *left = tuple_field_raw_by_part(
			tuple_format_runtime, a->raw.data, NULL,
			part, MULTIKEY_NONE);
		const char *right = tuple_field_raw_by_part(
			tuple_format_runtime, b->raw.data, NULL,
			part, MULTIKEY_NONE);
		int rc = tuple_compare_field(left, right, part->type,
					     part->coll);
		if (rc != 0)
			return part->sort_order == SORT_ORDER_DESC ? -rc : rc;
	}
	return 0;
}

/** Keep only the next batch; no array proportional to the space is built. */
static int
vector_scan_offer(struct vector_scan *scan, struct vector_scan_row *row)
{
	const char *field = tuple_field_raw_by_part(
		tuple_format_runtime, row->raw.data, NULL,
		scan->key_def->parts, MULTIKEY_NONE);
	enum ann_status status;
	uint32_t dimension;
	const char *bytes = field == NULL ? NULL :
		mp_decode_embedding(&field, &dimension);
	status = bytes == NULL || dimension != scan->dimension ? ANN_INVALID :
		ann_distance_raw(scan->query, false, bytes, true,
				 scan->dimension, scan->metric, NULL,
				 &row->distance);
	if (status != ANN_OK) {
		diag_set(ClientError, ER_VECTOR_INVALID);
		return -1;
	}
	if (scan->last.raw.data != NULL &&
	    vector_scan_compare(scan, row, &scan->last) <= 0)
		return 0;
	uint32_t pos = 0;
	while (pos < scan->count &&
	       vector_scan_compare(scan, &scan->rows[pos], row) < 0)
		++pos;
	if (pos == VECTOR_SCAN_BATCH)
		return 0;
	char *copy = xmalloc(row->raw.size);
	memcpy(copy, row->raw.data, row->raw.size);
	row->raw.data = copy;
	row->raw.ptr = NULL;
	if (scan->count == VECTOR_SCAN_BATCH)
		vector_scan_row_clear(&scan->rows[--scan->count]);
	memmove(scan->rows + pos + 1, scan->rows + pos,
		(scan->count - pos) * sizeof(*row));
	scan->rows[pos] = *row;
	++scan->count;
	return 0;
}

/** Rescanning trades CPU work for space independent of the result count. */
static int
vector_scan_refill(struct vector_scan *scan)
{
	scan->count = 0;
	scan->pos = 0;
	struct index_read_view_iterator it;
	if (index_read_view_create_iterator(scan->view, ITER_ALL,
					    NULL, 0, &it) != 0)
		return -1;
	int rc = 0;
	int64_t slice = clock_monotonic64() + 1000000;
	while (true) {
		size_t svp = region_used(&fiber()->gc);
		struct vector_scan_row row = {0};
		rc = index_read_view_iterator_next_raw(&it, &row.raw);
		if (rc != 0 || row.raw.data == NULL)
			break;
		rc = vector_scan_offer(scan, &row);
		region_truncate(&fiber()->gc, svp);
		if (rc != 0)
			break;
		if (clock_monotonic64() < slice)
			continue;
		if (box_check_slice() != 0) {
			rc = -1;
			break;
		}
		slice = clock_monotonic64() + 1000000;
	}
	index_read_view_iterator_destroy(&it);
	if (scan->count == 0 || rc != 0)
		scan->exhausted = true;
	return rc;
}

/** Retain the last row as an exclusive lower bound for the next batch. */
static int
vector_scan_next(struct vector_scan *scan, struct vector_scan_row **out)
{
	*out = NULL;
	if (scan->exhausted)
		return 0;
	if (scan->pos == scan->count && vector_scan_refill(scan) != 0)
		return -1;
	if (scan->exhausted)
		return 0;
	vector_scan_row_clear(&scan->last);
	scan->last = scan->rows[scan->pos];
	memset(&scan->rows[scan->pos++], 0, sizeof(scan->last));
	*out = &scan->last;
	return 0;
}

/** Frozen primary storage supplies stable tuples without copying a graph. */
struct vector_read_view {
	/** Public index read-view interface. */
	struct index_read_view base;
	/** Independently pinned primary-index snapshot. */
	struct index_read_view *primary;
};

/** The opaque read-view iterator only stores a pointer to its scan. */
struct vector_read_view_iterator {
	/** Public read-view iterator interface. */
	struct index_read_view_iterator_base base;
	/** Incremental exact scan, or NULL for an ALL iterator. */
	struct vector_scan *scan;
	/** Primary ALL iterator when no distance order is requested. */
	struct index_read_view_iterator *source;
};

static_assert(sizeof(struct vector_read_view_iterator) <=
	      INDEX_READ_VIEW_ITERATOR_SIZE, "vector read-view iterator size");

static void
vector_read_view_free(struct index_read_view *base)
{
	struct vector_read_view *view = (struct vector_read_view *)base;
	index_read_view_delete(view->primary);
	free(view);
}

static void
vector_read_view_iterator_destroy(struct index_read_view_iterator *base)
{
	struct vector_read_view_iterator *it =
		(struct vector_read_view_iterator *)base;
	if (it->scan != NULL)
		vector_scan_delete(it->scan);
	if (it->source != NULL) {
		index_read_view_iterator_destroy(it->source);
		free(it->source);
	}
}

/** Read-view results remain valid until the next cursor operation. */
static int
vector_read_view_next(struct index_read_view_iterator *base,
		      struct read_view_tuple *out)
{
	struct vector_read_view_iterator *it =
		(struct vector_read_view_iterator *)base;
	if (it->source != NULL)
		return index_read_view_iterator_next_raw(it->source, out);
	struct vector_scan_row *row;
	int rc = vector_scan_next(it->scan, &row);
	*out = row == NULL ? read_view_tuple_none() : row->raw;
	return rc;
}

/** Freeze data through the primary index rather than a mutable HNSW graph. */
static int
vector_read_view_create_iterator(struct index_read_view *base,
				 enum iterator_type type, const char *key,
				 uint32_t part_count, const char *pos,
				 struct index_read_view_iterator *iterator)
{
	if (pos != NULL || (type != ITER_ALL && type != ITER_NEIGHBOR)) {
		diag_set(ClientError, ER_VECTOR_UNSUPPORTED);
		return -1;
	}
	struct vector_read_view *view = (struct vector_read_view *)base;
	struct vector_read_view_iterator *it =
		(struct vector_read_view_iterator *)iterator;
	memset(it, 0, sizeof(*it));
	it->base.index = base;
	it->base.destroy = vector_read_view_iterator_destroy;
	it->base.next_raw = vector_read_view_next;
	it->base.position = generic_index_read_view_iterator_position;
	if (type == ITER_ALL) {
		it->source = xmalloc(sizeof(*it->source));
		if (index_read_view_create_iterator(view->primary, ITER_ALL,
						    NULL, 0, it->source) != 0) {
			free(it->source);
			it->source = NULL;
			return -1;
		}
		return 0;
	}
	it->scan = vector_scan_new(view->primary, base->def,
				   view->primary->def->key_def,
				   key, part_count);
	return it->scan == NULL ? -1 : 0;
}

static int
vector_read_view_get_raw(struct index_read_view *base, const char *key,
			 uint32_t part_count, struct read_view_tuple *out)
{
	(void)key;
	(void)part_count;
	(void)out;
	diag_set(UnsupportedIndexFeature, base->def, "get()");
	return -1;
}

struct index_read_view *
memtx_vector_read_view_new(struct index *index)
{
	static const struct index_read_view_vtab vtab = {
		.free = vector_read_view_free,
		.count = generic_index_read_view_count,
		.quantile = generic_index_read_view_quantile,
		.get_raw = vector_read_view_get_raw,
		.create_iterator = vector_read_view_create_iterator,
		.create_iterator_with_offset =
			generic_index_read_view_create_iterator_with_offset,
		.create_arrow_stream =
			generic_index_read_view_create_arrow_stream,
	};
	struct vector_read_view *view = xmalloc(sizeof(*view));
	struct space *space = space_by_id(index->def->space_id);
	view->primary = index_create_read_view(space->index[0]);
	if (view->primary == NULL) {
		free(view);
		return NULL;
	}
	index_read_view_create(&view->base, &vtab, index->def);
	return &view->base;
}

void
memtx_vector_read_view_set_space(struct index_read_view *base,
				 struct space_read_view *space)
{
	struct vector_read_view *view = (struct vector_read_view *)base;
	view->primary->space = space;
}
