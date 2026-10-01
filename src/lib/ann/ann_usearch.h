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

/** Isolated USearch implementation of the C backend contract. */
extern const struct ann_backend_ops ann_usearch_ops;

#if defined(__cplusplus)
}
#endif

#endif /* TARANTOOL_LIB_ANN_USEARCH_H_INCLUDED */
