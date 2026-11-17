/*
 * SPDX-License-Identifier: BSD-2-Clause
 *
 * Copyright 2026, Tarantool AUTHORS, please see AUTHORS file.
 */
#pragma once

#include "ann_backend.h"

#if defined(__cplusplus)
extern "C" {
#endif

struct ann_cow_page;

/** Persistent radix array of independently reference-counted objects. */
struct ann_cow {
	/** Page allocator, which must outlive every shared root. */
	struct ann_memory memory;
	/** Shared root, or NULL for an empty array. */
	struct ann_cow_page *root;
	/** Retain an object when its leaf page is copied. */
	void (*retain)(void *object);
	/** Release an object when its last leaf reference is removed. */
	void (*release)(void *object);
};

/** Initialize an empty array with object lifetime callbacks. */
void
ann_cow_create(struct ann_cow *array, const struct ann_memory *memory,
	       void (*retain)(void *), void (*release)(void *));

/** Retain an immutable root without allocating or walking the array. */
void
ann_cow_copy(struct ann_cow *copy, const struct ann_cow *array);

/** Release a root and objects no longer reachable from any root. */
void
ann_cow_destroy(struct ann_cow *array);

/** Find an object without allocating or changing shared pages. */
void *
ann_cow_get(const struct ann_cow *array, uint32_t slot);

/**
 * Replace an object, copying only shared pages on its radix path.
 * Failure preserves all object values; private empty pages may be reserved.
 */
enum ann_status
ann_cow_set(struct ann_cow *array, uint32_t slot, void *object);

#if defined(__cplusplus)
} /* extern "C" */
#endif
