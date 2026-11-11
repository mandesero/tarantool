#ifndef TARANTOOL_BOX_MEMTX_VECTOR_H_INCLUDED
#define TARANTOOL_BOX_MEMTX_VECTOR_H_INCLUDED

#include <stdint.h>
#include "iterator_type.h"

#if defined(__cplusplus)
extern "C" {
#endif /* defined(__cplusplus) */

struct index;
struct index_def;
struct memtx_engine;
struct port;
struct tuple;
struct iterator;
struct index_read_view;
struct space_read_view;

/** Storage-neutral arguments of a local VECTOR search. */
struct memtx_vector_search_opts {
	const char *key;
	const char *key_end;
	const char *filter;
	const char *filter_end;
	uint32_t limit;
	uint32_t offset;
	uint32_t ef_search;
	uint32_t filter_fieldno;
	double timeout;
};

/** Server resource policy, independent of any particular embedding model. */
enum { MEMTX_VECTOR_MAX_DIMENSION = 4096 };

struct index *
memtx_vector_index_new(struct memtx_engine *memtx, struct index_def *def);

/** Materialize visible tuples and optionally their distances. */
int
memtx_vector_index_search(struct index *index,
			  const struct memtx_vector_search_opts *opts,
			  struct port *port, double *distances);

/** Provide the primary-key predecessor without changing MVCC index links. */
void
memtx_vector_index_set_replace_source(struct index *index, struct tuple *tuple);

/** Replace the graph and tuple-label generation after a successful replay. */
int
memtx_vector_index_rebuild(struct index *index);

/** Create a lazy exact cursor for an unbounded scan. */
struct iterator *
memtx_vector_scan_iterator_new(struct index *index, enum iterator_type type,
			       const char *key, uint32_t part_count,
			       const char *pos);

/** Create a bounded HNSW cursor with explicit per-request options. */
struct iterator *
memtx_vector_search_iterator_new(struct index *index,
				 const struct memtx_vector_search_opts *opts);

/** Freeze primary storage for stable exact reads through VECTOR. */
struct index_read_view *
memtx_vector_read_view_new(struct index *index);

/** Attach the parent space before the view is exposed to other threads. */
void
memtx_vector_read_view_set_space(struct index_read_view *view,
				 struct space_read_view *space);

#if defined(__cplusplus)
} /* extern "C" */
#endif /* defined(__cplusplus) */

#endif /* TARANTOOL_BOX_MEMTX_VECTOR_H_INCLUDED */
