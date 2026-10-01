/*
 * SPDX-License-Identifier: BSD-2-Clause
 *
 * Copyright 2026, Tarantool AUTHORS, please see AUTHORS file.
 */
#include "ann_tuple_map.h"

#include <assert.h>
#include <stdint.h>
#include <string.h>

static size_t
ann_tuple_hash(const void *tuple)
{
	uint64_t value = (uintptr_t)tuple;
	value ^= value >> 30;
	value *= UINT64_C(0xbf58476d1ce4e5b9);
	value ^= value >> 27;
	value *= UINT64_C(0x94d049bb133111eb);
	return value ^ (value >> 31);
}

static void
ann_tuple_hash_insert(uint32_t *hash, size_t capacity,
		      const void *tuple, uint32_t label)
{
	size_t at = ann_tuple_hash(tuple) & (capacity - 1);
	while (hash[at] != 0)
		at = (at + 1) & (capacity - 1);
	hash[at] = label;
}

void
ann_tuple_map_create(struct ann_tuple_map *map,
		     const struct ann_memory *memory)
{
	memset(map, 0, sizeof(*map));
	map->memory = *memory;
}

void
ann_tuple_map_destroy(struct ann_tuple_map *map)
{
	if (map->hash != NULL)
		map->memory.free(map->memory.ctx, map->hash,
				 map->hash_capacity * sizeof(*map->hash));
	if (map->tuples != NULL)
		map->memory.free(map->memory.ctx, (void *)map->tuples,
				 (size_t)map->capacity * sizeof(*map->tuples));
	memset(map, 0, sizeof(*map));
}

uint64_t
ann_tuple_map_find(const struct ann_tuple_map *map, const void *tuple)
{
	if (map->hash_capacity == 0)
		return 0;
	size_t at = ann_tuple_hash(tuple) & (map->hash_capacity - 1);
	while (map->hash[at] != 0) {
		uint32_t label = map->hash[at];
		if (map->tuples[label - 1] == tuple)
			return label;
		at = (at + 1) & (map->hash_capacity - 1);
	}
	return 0;
}

enum ann_status
ann_tuple_map_prepare(struct ann_tuple_map *map)
{
	if (map->count == UINT32_MAX)
		return ANN_INVALID;
	if (map->count < map->capacity)
		return ANN_OK;
	uint32_t capacity = map->capacity == 0 ? 8 :
		map->capacity > UINT32_MAX / 2 ? UINT32_MAX :
		map->capacity * 2;
#if SIZE_MAX < UINT64_MAX
	if (capacity > SIZE_MAX / sizeof(*map->tuples))
		return ANN_INVALID;
#endif
	size_t tuple_size = (size_t)capacity * sizeof(*map->tuples);
	const void **tuples = map->memory.alloc(map->memory.ctx, tuple_size);
	if (tuples == NULL)
		return ANN_OUT_OF_MEMORY;
	if (map->count != 0)
		memcpy(tuples, map->tuples,
		       (size_t)map->count * sizeof(*tuples));
	size_t hash_capacity = 16;
	while (hash_capacity / 2 < capacity) {
		if (hash_capacity > SIZE_MAX / 2) {
			map->memory.free(map->memory.ctx, (void *)tuples,
					 tuple_size);
			return ANN_INVALID;
		}
		hash_capacity *= 2;
	}
	if (hash_capacity > SIZE_MAX / sizeof(*map->hash)) {
		map->memory.free(map->memory.ctx, (void *)tuples, tuple_size);
		return ANN_INVALID;
	}
	size_t hash_size = hash_capacity * sizeof(*map->hash);
	uint32_t *hash = map->memory.alloc(map->memory.ctx, hash_size);
	if (hash == NULL) {
		map->memory.free(map->memory.ctx, (void *)tuples, tuple_size);
		return ANN_OUT_OF_MEMORY;
	}
	memset(hash, 0, hash_size);
	for (uint32_t i = 0; i < map->count; ++i)
		ann_tuple_hash_insert(hash, hash_capacity, tuples[i], i + 1);
	if (map->tuples != NULL)
		map->memory.free(map->memory.ctx, (void *)map->tuples,
				 (size_t)map->capacity * sizeof(*map->tuples));
	if (map->hash != NULL)
		map->memory.free(map->memory.ctx, map->hash,
				 map->hash_capacity * sizeof(*map->hash));
	map->tuples = tuples;
	map->hash = hash;
	map->capacity = capacity;
	map->hash_capacity = hash_capacity;
	return ANN_OK;
}

uint64_t
ann_tuple_map_insert(struct ann_tuple_map *map, const void *tuple)
{
	assert(tuple != NULL && map->count < map->capacity &&
	       ann_tuple_map_find(map, tuple) == 0);
	uint32_t label = ++map->count;
	map->tuples[label - 1] = tuple;
	ann_tuple_hash_insert(map->hash, map->hash_capacity, tuple, label);
	return label;
}

const void *
ann_tuple_map_get(const struct ann_tuple_map *map, uint64_t label)
{
	return label == 0 || label > map->count ? NULL :
	       map->tuples[label - 1];
}
