/*
 * SPDX-License-Identifier: BSD-2-Clause
 *
 * Copyright 2026, Tarantool AUTHORS, please see AUTHORS file.
 */
#include "ann_memory.h"

#include <assert.h>
#include <stddef.h>
#include <stdlib.h>
#include <string.h>

/** malloc alignment also covers the header and the returned payload. */
union ann_quota_block {
	/** Preserve maximal C alignment for the following payload. */
	max_align_t alignment;
	/** Metadata retained until the block is released. */
	struct {
		/** Owner that charged the shared quota. */
		struct ann_quota_memory *owner;
		/** Requested payload size. */
		size_t size;
		/** Rounded charge returned by quota_use(). */
		size_t charge;
	} value;
};

static void *
ann_quota_alloc(void *ctx, size_t size)
{
	struct ann_quota_memory *owner = ctx;
	if (owner == NULL || owner->quota == NULL || size == 0 ||
	    size > SIZE_MAX - sizeof(union ann_quota_block))
		return NULL;
	size_t total = sizeof(union ann_quota_block) + size;
	ssize_t charged = quota_use(owner->quota, total);
	if (charged < 0)
		return NULL;
	union ann_quota_block *block = malloc(total);
	if (block == NULL) {
		quota_release(owner->quota, charged);
		return NULL;
	}
	block->value.owner = owner;
	block->value.size = size;
	block->value.charge = charged;
	owner->bytes += total;
	owner->charged += charged;
	owner->blocks++;
	return block + 1;
}

static void
ann_quota_free(void *ctx, void *ptr, size_t size)
{
	if (ptr == NULL)
		return;
	union ann_quota_block *block = (union ann_quota_block *)ptr - 1;
	struct ann_quota_memory *owner = ctx;
	assert(block->value.owner == owner);
	assert(block->value.size == size);
	size_t total = sizeof(*block) + size;
	size_t charged = block->value.charge;
	assert(owner->bytes >= total && owner->charged >= charged &&
	       owner->blocks > 0);
	owner->bytes -= total;
	owner->charged -= charged;
	owner->blocks--;
	free(block);
	quota_release(owner->quota, charged);
}

void
ann_quota_memory_create(struct ann_quota_memory *owner, struct quota *quota)
{
	memset(owner, 0, sizeof(*owner));
	owner->quota = quota;
}

struct ann_memory
ann_quota_memory_bind(struct ann_quota_memory *owner)
{
	struct ann_memory result = {
		.ctx = owner,
		.alloc = ann_quota_alloc,
		.free = ann_quota_free,
	};
	return result;
}
