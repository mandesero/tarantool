#ifndef TARANTOOL_BOX_MEMTX_VECTOR_H_INCLUDED
#define TARANTOOL_BOX_MEMTX_VECTOR_H_INCLUDED

#include <stdint.h>

#if defined(__cplusplus)
extern "C" {
#endif /* defined(__cplusplus) */

struct index;
struct index_def;
struct memtx_engine;
struct port;

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

/**
 * The largest vector the index accepts. Embedding models in use today stay
 * well below it (384 to 3072 components), and the limit keeps a typo in the
 * index definition from reserving absurd amounts of memory.
 */
enum { MEMTX_VECTOR_MAX_DIMENSION = 4096 };

struct index *
memtx_vector_index_new(struct memtx_engine *memtx, struct index_def *def);

/** Materialize nearest visible tuples into a caller-owned port. */
int
memtx_vector_index_search(struct index *index,
			  const struct memtx_vector_search_opts *opts,
			  struct port *port, double *distances);

/** Replace the graph and tuple-label generation after a successful replay. */
int
memtx_vector_index_rebuild(struct index *index);

#if defined(__cplusplus)
} /* extern "C" */
#endif /* defined(__cplusplus) */

#endif /* TARANTOOL_BOX_MEMTX_VECTOR_H_INCLUDED */
