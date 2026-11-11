/*
 * SPDX-License-Identifier: BSD-2-Clause
 *
 * Copyright 2026, Tarantool AUTHORS, please see AUTHORS file.
 */
#ifndef TARANTOOL_LIB_ANN_USEARCH_H_INCLUDED
#define TARANTOOL_LIB_ANN_USEARCH_H_INCLUDED

#include "ann_backend.h"

/** Immutable HNSW settings passed through ann_config.algorithm. */
struct ann_usearch_config {
	/** Maximum outgoing graph connections above the base layer. */
	uint32_t connectivity;
	/** Construction traversal width. */
	uint32_t expansion_add;
	/** Default search traversal width. */
	uint32_t expansion_search;
};

/** Per-query HNSW settings passed through ann_search_opts.algorithm. */
struct ann_usearch_search_opts {
	/** Search traversal width overriding the index default. */
	uint32_t expansion;
};

#if defined(__cplusplus)
extern "C" {
#endif

/** Immutable vector shared by tuple versions and graph generations. */
struct ann_usearch_vector;

/** Allocate one reference-counted canonical vector. */
struct ann_usearch_vector *
ann_usearch_vector_new(const struct ann_memory *memory, const float *data,
		       uint32_t dimension);

/** Release one owner; graph references keep navigation data alive. */
void
ann_usearch_vector_unref(struct ann_usearch_vector *vector);

/** Borrow canonical coordinates while holding a vector reference. */
const void *
ann_usearch_vector_data(const struct ann_usearch_vector *vector);

/** Borrow packed coordinates; release(owner) runs after the last reference. */
struct ann_usearch_vector *
ann_usearch_vector_borrow(const struct ann_memory *memory, const void *data,
			  uint32_t dimension, void *owner,
			  void (*release)(void *));

/** Whether coordinate storage belongs to an externally retained tuple. */
bool
ann_usearch_vector_is_borrowed(const struct ann_usearch_vector *vector);

/** Bytes per coordinate-owner header, excluding optional owned payload. */
size_t
ann_usearch_vector_overhead(void);

/** Prepare an insertion borrowing an immutable vector, without copying it. */
enum ann_status
ann_usearch_prepare_shared(struct ann_backend *backend, uint64_t label,
			   struct ann_usearch_vector *vector,
			   struct ann_change **out);

/** Test membership, including labels retained only for rollback. */
bool
ann_usearch_contains(const struct ann_backend *backend, uint64_t label);

/** Read maintenance counters without scanning graph entries. */
uint64_t
ann_usearch_reclaimable(const struct ann_backend *backend, uint64_t *size);

/** Isolated USearch implementation of the C backend contract. */
extern const struct ann_backend_ops ann_usearch_ops;

#if defined(__cplusplus)
}
#endif

#endif /* TARANTOOL_LIB_ANN_USEARCH_H_INCLUDED */
