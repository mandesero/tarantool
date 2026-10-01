/*
 * SPDX-License-Identifier: BSD-2-Clause
 *
 * Copyright 2026, Tarantool AUTHORS, please see AUTHORS file.
 */
#ifndef TARANTOOL_BOX_ANN_TUPLE_MAP_H_INCLUDED
#define TARANTOOL_BOX_ANN_TUPLE_MAP_H_INCLUDED

#include "../lib/ann/ann_backend.h"

#if defined(__cplusplus)
extern "C" {
#endif

/** Monotonic label and tuple-address binding for one memtx VECTOR index. */
struct ann_tuple_map {
	/** Allocation owner shared with the ANN backend. */
	struct ann_memory memory;
	/** Tuple addresses indexed by label minus one. */
	const void **tuples;
	/** Open-addressed table of labels indexed by tuple address. */
	uint32_t *hash;
	/** Number of assigned labels. */
	uint32_t count;
	/** Allocated tuple-address slots. */
	uint32_t capacity;
	/** Power-of-two hash table size. */
	size_t hash_capacity;
};

/** Initialize a map without allocating. */
void
ann_tuple_map_create(struct ann_tuple_map *map,
		     const struct ann_memory *memory);

/** Release arrays, leaving tuple references to the caller. */
void
ann_tuple_map_destroy(struct ann_tuple_map *map);

/** Find a tuple address, returning zero when absent. */
uint64_t
ann_tuple_map_find(const struct ann_tuple_map *map, const void *tuple);

/** Reserve space for one new binding before graph mutation. */
enum ann_status
ann_tuple_map_prepare(struct ann_tuple_map *map);

/** Publish a new tuple address without allocation and return its label. */
uint64_t
ann_tuple_map_insert(struct ann_tuple_map *map, const void *tuple);

/** Resolve an assigned label to its retained tuple address. */
const void *
ann_tuple_map_get(const struct ann_tuple_map *map, uint64_t label);

#if defined(__cplusplus)
} /* extern "C" */
#endif

#endif /* TARANTOOL_BOX_ANN_TUPLE_MAP_H_INCLUDED */
