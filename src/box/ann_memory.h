/*
 * SPDX-License-Identifier: BSD-2-Clause
 *
 * Copyright 2026, Tarantool AUTHORS, please see AUTHORS file.
 */
#ifndef TARANTOOL_BOX_ANN_MEMORY_H_INCLUDED
#define TARANTOOL_BOX_ANN_MEMORY_H_INCLUDED

#include "../lib/ann/ann_backend.h"

#include <small/quota.h>

#if defined(__cplusplus)
extern "C" {
#endif

/** Quota owner shared by all allocations of one ANN generation. */
struct ann_quota_memory {
	/** Shared memtx quota. */
	struct quota *quota;
	/** Retained payload and per-block header bytes. */
	size_t bytes;
	/** Quota units charged after rounding. */
	size_t charged;
	/** Number of outstanding blocks. */
	size_t blocks;
};

/** Initialize a memory owner against the engine's shared quota. */
void
ann_quota_memory_create(struct ann_quota_memory *owner,
			struct quota *quota);

/** Return backend allocation callbacks bound to this owner. */
struct ann_memory
ann_quota_memory_bind(struct ann_quota_memory *owner);

#if defined(__cplusplus)
} /* extern "C" */
#endif

#endif /* TARANTOOL_BOX_ANN_MEMORY_H_INCLUDED */
