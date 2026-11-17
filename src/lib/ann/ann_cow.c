/*
 * SPDX-License-Identifier: BSD-2-Clause
 *
 * Copyright 2026, Tarantool AUTHORS, please see AUTHORS file.
 */
#include "ann_cow.h"

#include <assert.h>
#include <string.h>

enum {
	ANN_COW_BITS = 5,
	ANN_COW_FANOUT = 1 << ANN_COW_BITS,
	ANN_COW_LEVELS = (32 + ANN_COW_BITS - 1) / ANN_COW_BITS,
};

/** A shared branch or leaf; readers access only immutable slots. */
struct ann_cow_page {
	/** Number of parent or root references, modified by the owner cord. */
	uint32_t refs;
	/** Child pages above level zero, otherwise retained objects. */
	void *slots[ANN_COW_FANOUT];
};

/** Only the owner cord releases roots, including roots read by other cords. */
static void
ann_cow_unref(struct ann_cow *array, struct ann_cow_page *page, unsigned level)
{
	if (page == NULL || --page->refs != 0)
		return;
	for (unsigned i = 0; i < ANN_COW_FANOUT; ++i) {
		if (page->slots[i] == NULL)
			continue;
		if (level != 0)
			ann_cow_unref(array, page->slots[i], level - 1);
		else
			array->release(page->slots[i]);
	}
	array->memory.free(array->memory.ctx, page, sizeof(*page));
}

void
ann_cow_create(struct ann_cow *array, const struct ann_memory *memory,
	       void (*retain)(void *), void (*release)(void *))
{
	array->memory = *memory;
	array->root = NULL;
	array->retain = retain;
	array->release = release;
}

void
ann_cow_copy(struct ann_cow *copy, const struct ann_cow *array)
{
	*copy = *array;
	if (copy->root != NULL) {
		assert(copy->root->refs < UINT32_MAX);
		++copy->root->refs;
	}
}

void
ann_cow_destroy(struct ann_cow *array)
{
	ann_cow_unref(array, array->root, ANN_COW_LEVELS - 1);
	array->root = NULL;
}

void *
ann_cow_get(const struct ann_cow *array, uint32_t slot)
{
	const struct ann_cow_page *page = array->root;
	for (unsigned level = ANN_COW_LEVELS - 1; page != NULL; --level) {
		unsigned offset = (slot >> (level * ANN_COW_BITS)) &
				  (ANN_COW_FANOUT - 1);
		if (level == 0)
			return page->slots[offset];
		page = page->slots[offset];
	}
	return NULL;
}

/** A write never changes a page reachable from a frozen root. */
static struct ann_cow_page *
ann_cow_touch(struct ann_cow *array, struct ann_cow_page *old, unsigned level)
{
	if (old != NULL && old->refs == 1)
		return old;
	struct ann_cow_page *page =
		array->memory.alloc(array->memory.ctx, sizeof(*page));
	if (page == NULL)
		return NULL;
	memset(page, 0, sizeof(*page));
	page->refs = 1;
	if (old != NULL) {
		memcpy(page->slots, old->slots, sizeof(page->slots));
		for (unsigned i = 0; i < ANN_COW_FANOUT; ++i) {
			if (page->slots[i] == NULL)
				continue;
			if (level != 0) {
				struct ann_cow_page *child = page->slots[i];
				assert(child->refs < UINT32_MAX);
				++child->refs;
			} else {
				array->retain(page->slots[i]);
			}
		}
		--old->refs;
	}
	return page;
}

enum ann_status
ann_cow_set(struct ann_cow *array, uint32_t slot, void *object)
{
	unsigned level = ANN_COW_LEVELS - 1;
	struct ann_cow_page *page = ann_cow_touch(array, array->root, level);
	if (page == NULL)
		return ANN_OUT_OF_MEMORY;
	array->root = page;
	for (;; --level) {
		unsigned offset = (slot >> (level * ANN_COW_BITS)) &
				  (ANN_COW_FANOUT - 1);
		if (level == 0) {
			if (object != NULL)
				array->retain(object);
			if (page->slots[offset] != NULL)
				array->release(page->slots[offset]);
			page->slots[offset] = object;
			return ANN_OK;
		}
		struct ann_cow_page *child =
			ann_cow_touch(array, page->slots[offset], level - 1);
		if (child == NULL)
			return ANN_OUT_OF_MEMORY;
		page->slots[offset] = child;
		page = child;
	}
}
