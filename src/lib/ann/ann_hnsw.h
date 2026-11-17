/*
 * SPDX-License-Identifier: BSD-2-Clause
 *
 * Copyright 2026, Tarantool AUTHORS, please see AUTHORS file.
 */
#pragma once

#include <stdint.h>

/** Immutable HNSW settings passed through ann_config.algorithm. */
struct ann_hnsw_config {
	/** Maximum outgoing graph connections above the base layer. */
	uint32_t connectivity;
	/** Construction traversal width. */
	uint32_t expansion_add;
	/** Default search traversal width. */
	uint32_t expansion_search;
};

/** Per-query HNSW settings passed through ann_search_opts.algorithm. */
struct ann_hnsw_search_opts {
	/** Search traversal width overriding the index default. */
	uint32_t expansion;
};
