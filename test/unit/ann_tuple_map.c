/*
 * SPDX-License-Identifier: BSD-2-Clause
 *
 * Copyright 2026, Tarantool AUTHORS, please see AUTHORS file.
 */
#include "box/ann_tuple_map.h"
#include "box/ann_memory.h"

#include <stdint.h>

#define UNIT_TAP_COMPATIBLE 1
#include "unit.h"

static const void *
fake_tuple(uintptr_t value)
{
	return (const void *)(value * 16 + 0x1000);
}

int
main(void)
{
	plan(11);
	struct quota quota;
	quota_init(&quota, 1024 * 1024);
	struct ann_quota_memory owner;
	ann_quota_memory_create(&owner, &quota);
	struct ann_memory memory = ann_quota_memory_bind(&owner);
	struct ann_tuple_map map;
	ann_tuple_map_create(&map, &memory);
	is(ann_tuple_map_find(&map, fake_tuple(1)), 0,
	   "empty map has no label");
	bool inserted = true;
	for (uintptr_t i = 1; i <= 8; ++i) {
		inserted &= ann_tuple_map_prepare(&map) == ANN_OK;
		inserted &= ann_tuple_map_insert(&map, fake_tuple(i)) == i;
	}
	ok(inserted, "first eight labels are monotonic");
	size_t charged = quota_used(&quota);
	is(quota_set(&quota, charged), (ssize_t)charged,
	   "quota shrinks to current charge");
	is(ann_tuple_map_prepare(&map), ANN_OUT_OF_MEMORY,
	   "growth fails before publishing a label");
	is(quota_used(&quota), charged,
	   "failed growth returns temporary charges");
	is(map.count, 8, "failed growth keeps label count");
	is(quota_set(&quota, 1024 * 1024), 1024 * 1024,
	   "quota limit is restored");
	for (uintptr_t i = 9; i <= 100; ++i) {
		inserted &= ann_tuple_map_prepare(&map) == ANN_OK;
		inserted &= ann_tuple_map_insert(&map, fake_tuple(i)) == i;
	}
	ok(inserted, "one hundred labels grow without reuse");
	bool found = true;
	for (uintptr_t i = 1; i <= 100; ++i)
		found &= ann_tuple_map_find(&map, fake_tuple(i)) == i &&
			 ann_tuple_map_get(&map, i) == fake_tuple(i);
	ok(found, "forward and reverse lookup agree after growth");
	is(ann_tuple_map_find(&map, fake_tuple(101)), 0,
	   "unknown tuple stays absent");
	ann_tuple_map_destroy(&map);
	ok(owner.blocks == 0 && quota_used(&quota) == 0,
	   "map destruction returns every quota charge");
	return check_plan();
}
