#ifndef TARANTOOL_BOX_MEMTX_VECTOR_H_INCLUDED
#define TARANTOOL_BOX_MEMTX_VECTOR_H_INCLUDED

#if defined(__cplusplus)
extern "C" {
#endif /* defined(__cplusplus) */

struct index;
struct index_def;
struct memtx_engine;

/**
 * The largest vector the index accepts. Embedding models in use today stay
 * well below it (384 to 3072 components), and the limit keeps a typo in the
 * index definition from reserving absurd amounts of memory.
 */
enum { MEMTX_VECTOR_MAX_DIMENSION = 4096 };

struct index *
memtx_vector_index_new(struct memtx_engine *memtx, struct index_def *def);

#if defined(__cplusplus)
} /* extern "C" */
#endif /* defined(__cplusplus) */

#endif /* TARANTOOL_BOX_MEMTX_VECTOR_H_INCLUDED */
