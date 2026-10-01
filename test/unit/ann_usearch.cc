/*
 * SPDX-License-Identifier: BSD-2-Clause
 *
 * Copyright 2026, Tarantool AUTHORS, please see AUTHORS file.
 */
#include "lib/ann/ann_backend.h"
#include "lib/ann/ann_usearch.h"
#include "box/ann_memory.h"

#include <cstddef>
#include <cstdlib>
#include <cstring>

#define UNIT_TAP_COMPATIBLE 1
#include "unit.h"

/** Counting test owner with one deterministic allocation failure. */
struct owner {
	/** Total allocation attempts. */
	size_t calls;
	/** Attempt to fail, or zero. */
	size_t fail_at;
	/** Currently retained bytes. */
	size_t bytes;
	/** Mismatched owner or size on free. */
	size_t bad_free;
};

/** Test block retaining owner identity at maximal C alignment. */
union block {
	/** Payload alignment. */
	max_align_t alignment;
	/** Owner and size expected at release. */
	struct {
		/** Allocation owner. */
		struct owner *owner;
		/** Requested allocation size. */
		size_t size;
	} value;
};

static void *
owner_alloc(void *ctx, size_t size)
{
	struct owner *owner = (struct owner *)ctx;
	if (++owner->calls == owner->fail_at)
		return nullptr;
	union block *header = (union block *)malloc(sizeof(*header) + size);
	if (header == nullptr)
		return nullptr;
	header->value.owner = owner;
	header->value.size = size;
	owner->bytes += size;
	return header + 1;
}

static void
owner_free(void *ctx, void *ptr, size_t size)
{
	if (ptr == nullptr)
		return;
	union block *header = (union block *)ptr - 1;
	struct owner *owner = (struct owner *)ctx;
	if (header->value.owner != owner || header->value.size != size)
		++owner->bad_free;
	owner->bytes -= header->value.size;
	free(header);
}

static struct ann_memory
owner_memory(struct owner *owner)
{
	struct ann_memory memory = {owner, owner_alloc, owner_free};
	return memory;
}

static enum ann_status
insert(const struct ann_backend_ops *ops, struct ann_backend *backend,
	uint64_t label, const float *vector)
{
	struct ann_change *change = nullptr;
	enum ann_status status = ops->prepare(backend, ANN_INSERT, label,
					     vector, 2, &change);
	if (status != ANN_OK)
		return status;
	status = ops->apply(change, nullptr);
	if (status != ANN_OK)
		ops->rollback(change);
	ops->finish(change);
	return status;
}

static enum ann_status
search(const struct ann_backend_ops *ops, struct ann_backend *backend,
	       const float *query, struct ann_candidate *out,
	       uint32_t *count)
{
	struct ann_search_control control = {};
	control.work_limit = 100000;
	struct ann_search_opts opts = {};
	opts.candidate_limit = 2;
	opts.query_dimension = 2;
	opts.control = &control;
	return ops->search(backend, query, &opts, out, count);
}

static void
test_lifecycle(const struct ann_backend_ops *ops)
{
	plan(21);
	struct owner owner = {};
	struct ann_memory memory = owner_memory(&owner);
	struct ann_config config = {2, ANN_L2, nullptr};
	struct ann_backend *backend = nullptr;
	is(ops->create(&config, &memory, &backend), ANN_OK,
	   "%s creates", ops->name);
	float first[] = {1, 0};
	float second[] = {10, 0};
	is(insert(ops, backend, 11, first), ANN_OK, "first insert");
	is(insert(ops, backend, 22, second), ANN_OK, "second insert");
	struct ann_backend_stats stats;
	ops->stat(backend, &stats);
	ok(stats.live == 2 && stats.resident_bytes == owner.bytes,
	   "all backend allocations appear in resident memory");
	struct ann_candidate found[2];
	uint32_t count = 0;
	is(search(ops, backend, first, found, &count), ANN_OK,
	   "search succeeds");
	ops->stat(backend, &stats);
	is(stats.resident_bytes, owner.bytes,
	   "search buffers appear in resident memory");
	is(count, 2, "two candidates found");
	is(found[0].label, 11, "nearest label is first");
	is(found[0].distance, 0, "nearest distance is zero");
	size_t alloc_before = owner.calls;
	is(ops->set_live(backend, 11, false), ANN_OK,
	   "existing label deactivates without preparation");
	is(ops->set_live(backend, 11, true), ANN_OK,
	   "existing label reactivates without preparation");
	is(ops->set_live(backend, 33, false), ANN_NOT_FOUND,
	   "unknown label cannot change visibility");
	ok(owner.calls == alloc_before,
	   "visibility compensation makes no allocations");
	struct ann_change *change = nullptr;
	is(ops->prepare(backend, ANN_RETIRE, 11, nullptr, 0, &change),
	   ANN_OK, "retire prepared");
	is(ops->apply(change, nullptr), ANN_OK, "retire applied");
	ops->finish(change);
	is(search(ops, backend, first, found, &count), ANN_OK,
	   "search after retire succeeds");
	ok(count == 1 && found[0].label == 22,
	   "retired label is excluded");
	is(ops->reclaim(backend, 11), ANN_OK,
	   "visibility GC releases the retired label");
	is(insert(ops, backend, 11, first), ANN_OK,
	   "released label can be assigned to a new version");
	ok(search(ops, backend, first, found, &count) == ANN_OK &&
	   count == 2 && found[0].label == 11,
	   "reclaimed graph node does not appear in results");
	ops->destroy(backend);
	ok(owner.bytes == 0 && owner.bad_free == 0,
	   "backend returns every allocation to its owner");
	check_plan();
}

static void
test_failed_insert(void)
{
	plan(6);
	struct owner baseline = {};
	struct ann_memory memory = owner_memory(&baseline);
	struct ann_config config = {2, ANN_L2, nullptr};
	struct ann_backend *backend = nullptr;
	float first[] = {1, 0};
	float second[] = {10, 0};
	is(ann_usearch_ops.create(&config, &memory, &backend), ANN_OK,
	   "failure baseline creates");
	is(insert(&ann_usearch_ops, backend, 11, first), ANN_OK,
	   "failure baseline first insert");
	size_t before = baseline.calls;
	is(insert(&ann_usearch_ops, backend, 22, second), ANN_OK,
	   "failure baseline second insert");
	size_t steps = baseline.calls - before;
	ann_usearch_ops.destroy(backend);
	is(baseline.bytes, 0, "failure baseline releases all memory");
	bool consistent = true;
	for (size_t point = 1; point <= steps; ++point) {
		struct owner owner = {};
		memory = owner_memory(&owner);
		backend = nullptr;
		if (ann_usearch_ops.create(&config, &memory, &backend) != ANN_OK ||
		    insert(&ann_usearch_ops, backend, 11, first) != ANN_OK) {
			consistent = false;
			break;
		}
		owner.fail_at = owner.calls + point;
		enum ann_status status = insert(&ann_usearch_ops, backend,
						     22, second);
		struct ann_candidate found[2];
		uint32_t count = 0;
		owner.fail_at = 0;
		if (status != ANN_OUT_OF_MEMORY ||
		    search(&ann_usearch_ops, backend, first,
			   found, &count) != ANN_OK ||
		    count != 1 || found[0].label != 11)
			consistent = false;
		ann_usearch_ops.destroy(backend);
		if (owner.bytes != 0 || owner.bad_free != 0)
			consistent = false;
		if (!consistent)
			break;
	}
	ok(steps != 0, "second insert exercises allocations");
	ok(consistent, "each allocation failure preserves prior search and owner");
	check_plan();
}

static void
test_interrupted_search(void)
{
	plan(8);
	struct owner owner = {};
	struct ann_memory memory = owner_memory(&owner);
	struct ann_config config = {2, ANN_L2, nullptr};
	struct ann_backend *backend = nullptr;
	is(ann_usearch_ops.create(&config, &memory, &backend), ANN_OK,
	   "interruption backend creates");
	float first[] = {1, 0};
	float second[] = {10, 0};
	is(insert(&ann_usearch_ops, backend, 11, first), ANN_OK,
	   "interruption first insert");
	is(insert(&ann_usearch_ops, backend, 22, second), ANN_OK,
	   "interruption second insert");
	struct ann_search_control control = {};
	control.work_limit = 2;
	struct ann_search_opts opts = {};
	opts.candidate_limit = 2;
	opts.query_dimension = 2;
	opts.control = &control;
	struct ann_candidate found[2];
	uint32_t count = 9;
	is(ann_usearch_ops.search(backend, first, &opts, found, &count),
	   ANN_WORK_LIMIT, "traversal returns work-limit error");
	is(count, 0, "interrupted search publishes no candidates");
	is(search(&ann_usearch_ops, backend, first, found, &count), ANN_OK,
	   "later search reuses traversal context");
	ok(count == 2 && found[0].label == 11,
	   "later search retains correct results");
	ann_usearch_ops.destroy(backend);
	ok(owner.bytes == 0 && owner.bad_free == 0,
	   "interrupted backend releases every allocation");
	check_plan();
}

static enum ann_status
wide_search(struct ann_backend *backend, const float *query,
	    struct ann_candidate *out, uint32_t *count)
{
	struct ann_search_control control = {};
	control.work_limit = 100000;
	struct ann_usearch_search_opts algorithm = {1024};
	struct ann_search_opts opts = {};
	opts.candidate_limit = 2;
	opts.query_dimension = 2;
	opts.control = &control;
	opts.algorithm = &algorithm;
	return ann_usearch_ops.search(backend, query, &opts, out, count);
}

static void
test_failed_search(void)
{
	plan(5);
	struct owner baseline = {};
	struct ann_memory memory = owner_memory(&baseline);
	struct ann_config config = {2, ANN_L2, nullptr};
	struct ann_backend *backend = nullptr;
	float first[] = {1, 0};
	float second[] = {10, 0};
	is(ann_usearch_ops.create(&config, &memory, &backend), ANN_OK,
	   "search failure baseline creates");
	is(insert(&ann_usearch_ops, backend, 11, first), ANN_OK,
	   "search failure baseline first insert");
	is(insert(&ann_usearch_ops, backend, 22, second), ANN_OK,
	   "search failure baseline second insert");
	size_t before = baseline.calls;
	struct ann_candidate found[2];
	uint32_t count = 0;
	bool baseline_ok = wide_search(backend, first, found, &count) == ANN_OK;
	size_t steps = baseline.calls - before;
	ann_usearch_ops.destroy(backend);
	ok(baseline_ok && steps != 0 && baseline.bytes == 0,
	   "wide search allocates and releases buffers");
	bool consistent = true;
	for (size_t point = 1; point <= steps; ++point) {
		struct owner owner = {};
		memory = owner_memory(&owner);
		backend = nullptr;
		if (ann_usearch_ops.create(&config, &memory, &backend) != ANN_OK ||
		    insert(&ann_usearch_ops, backend, 11, first) != ANN_OK ||
		    insert(&ann_usearch_ops, backend, 22, second) != ANN_OK) {
			consistent = false;
			break;
		}
		owner.fail_at = owner.calls + point;
		count = 9;
		enum ann_status status = wide_search(backend, first,
						     found, &count);
		owner.fail_at = 0;
		if (status != ANN_OUT_OF_MEMORY || count != 0 ||
		    wide_search(backend, first, found, &count) != ANN_OK ||
		    count != 2 || found[0].label != 11)
			consistent = false;
		ann_usearch_ops.destroy(backend);
		if (owner.bytes != 0 || owner.bad_free != 0)
			consistent = false;
		if (!consistent)
			break;
	}
	ok(consistent, "every search allocation failure permits retry");
	check_plan();
}

static void
test_interrupted_insert(void)
{
	plan(3);
	struct ann_config config = {2, ANN_L2, nullptr};
	float first[] = {1, 0};
	float second[] = {10, 0};
	float third[] = {20, 0};
	bool consistent = true;
	bool stopped_late = false;
	bool reached_success = false;
	for (uint64_t budget = 1; budget <= 256; ++budget) {
		struct owner owner = {};
		struct ann_memory memory = owner_memory(&owner);
		struct ann_backend *backend = nullptr;
		if (ann_usearch_ops.create(&config, &memory, &backend) != ANN_OK ||
		    insert(&ann_usearch_ops, backend, 11, first) != ANN_OK ||
		    insert(&ann_usearch_ops, backend, 22, second) != ANN_OK) {
			consistent = false;
			break;
		}
		struct ann_change *change = nullptr;
		if (ann_usearch_ops.prepare(backend, ANN_INSERT, 33,
					    third, 2, &change) != ANN_OK) {
			consistent = false;
			ann_usearch_ops.destroy(backend);
			break;
		}
		struct ann_search_control control = {};
		control.work_limit = budget;
		enum ann_status status = ann_usearch_ops.apply(change,
							   &control);
		if (status == ANN_OK) {
			reached_success = true;
			ann_usearch_ops.finish(change);
		} else {
			if (status != ANN_WORK_LIMIT)
				consistent = false;
			if (budget > 10)
				stopped_late = true;
			ann_usearch_ops.rollback(change);
			ann_usearch_ops.finish(change);
			struct ann_candidate found[2];
			uint32_t count = 0;
			if (search(&ann_usearch_ops, backend, first,
				   found, &count) != ANN_OK ||
			    count != 2 || found[0].label != 11 ||
			    insert(&ann_usearch_ops, backend, 33, third) != ANN_OK)
				consistent = false;
		}
		ann_usearch_ops.destroy(backend);
		if (owner.bytes != 0 || owner.bad_free != 0)
			consistent = false;
		if (reached_success || !consistent)
			break;
	}
	ok(stopped_late, "insertion stops after entering graph traversal");
	ok(reached_success, "sufficient write budget permits insertion");
	ok(consistent, "every interrupted insertion restores graph and owner");
	check_plan();
}

static void
test_rollback_no_alloc(void)
{
	plan(6);
	struct owner owner = {};
	struct ann_memory memory = owner_memory(&owner);
	struct ann_config config = {2, ANN_L2, nullptr};
	struct ann_backend *backend = nullptr;
	is(ann_usearch_ops.create(&config, &memory, &backend), ANN_OK,
	   "rollback backend creates");
	float first[] = {1, 0};
	float second[] = {10, 0};
	is(insert(&ann_usearch_ops, backend, 11, first), ANN_OK,
	   "rollback baseline insert");
	struct ann_change *change = nullptr;
	is(ann_usearch_ops.prepare(backend, ANN_INSERT, 22,
					 second, 2, &change), ANN_OK,
	   "rollback insertion prepares");
	is(ann_usearch_ops.apply(change, nullptr), ANN_OK,
	   "rollback insertion mutates graph");
	size_t calls = owner.calls;
	owner.fail_at = calls + 1;
	ann_usearch_ops.rollback(change);
	ann_usearch_ops.finish(change);
	is(owner.calls, calls, "rollback and finish allocate nothing");
	ann_usearch_ops.destroy(backend);
	ok(owner.bytes == 0 && owner.bad_free == 0,
	   "rolled-back graph releases all memory");
	check_plan();
}

static void
test_growth_and_retire(void)
{
	plan(6);
	struct owner owner = {};
	struct ann_memory memory = owner_memory(&owner);
	struct ann_usearch_config algorithm = {8, 32, 64};
	struct ann_config config = {2, ANN_L2, &algorithm};
	struct ann_backend *backend = nullptr;
	is(ann_usearch_ops.create(&config, &memory, &backend), ANN_OK,
	   "growth backend creates with HNSW options");
	bool inserted = true;
	for (uint64_t i = 0; i < 128; ++i) {
		float vector[] = {(float)i, (float)(i % 3)};
		if (insert(&ann_usearch_ops, backend, i + 1, vector) != ANN_OK) {
			inserted = false;
			break;
		}
	}
	ok(inserted, "128 vectors grow graph and lookup");
	bool retired = true;
	for (uint64_t label = 2; label <= 128; label += 2) {
		struct ann_change *change = nullptr;
		if (ann_usearch_ops.prepare(backend, ANN_RETIRE, label,
					     nullptr, 0, &change) != ANN_OK ||
		    ann_usearch_ops.apply(change, nullptr) != ANN_OK) {
			retired = false;
			break;
		}
		ann_usearch_ops.finish(change);
	}
	ok(retired, "64 versions retire after graph growth");
	struct ann_backend_stats stats;
	ann_usearch_ops.stat(backend, &stats);
	ok(stats.live == 64 && stats.retired == 64 &&
	   stats.resident_bytes == owner.bytes,
	   "growth and tombstones remain charged");
	float query[] = {64, 1};
	struct ann_candidate found[2];
	uint32_t count = 0;
	ok(search(&ann_usearch_ops, backend, query, found, &count) == ANN_OK &&
	   count > 0 && found[0].label == 65 && found[0].distance == 0,
	   "search finds exact live vector through retired nodes");
	ann_usearch_ops.destroy(backend);
	ok(owner.bytes == 0 && owner.bad_free == 0,
	   "grown graph releases every allocation");
	check_plan();
}

static void
test_two_owners(void)
{
	plan(8);
	struct owner first_owner = {}, second_owner = {};
	struct ann_memory first_memory = owner_memory(&first_owner);
	struct ann_memory second_memory = owner_memory(&second_owner);
	struct ann_config config = {2, ANN_L2, nullptr};
	struct ann_backend *first = nullptr, *second = nullptr;
	is(ann_usearch_ops.create(&config, &first_memory, &first), ANN_OK,
	   "first owner creates backend");
	is(ann_usearch_ops.create(&config, &second_memory, &second), ANN_OK,
	   "second owner creates backend");
	float a[] = {1, 0};
	float b[] = {2, 0};
	is(insert(&ann_usearch_ops, first, 11, a), ANN_OK,
	   "first owner inserts");
	is(insert(&ann_usearch_ops, second, 22, b), ANN_OK,
	   "second owner inserts");
	ann_usearch_ops.destroy(first);
	is(first_owner.bytes, 0, "first owner releases only its backend");
	struct ann_candidate found[2];
	uint32_t count = 0;
	ok(search(&ann_usearch_ops, second, b, found, &count) == ANN_OK &&
	   count == 1 && found[0].label == 22,
	   "second backend remains searchable");
	ann_usearch_ops.destroy(second);
	is(second_owner.bytes, 0, "second owner releases its backend");
	ok(first_owner.bad_free == 0 && second_owner.bad_free == 0,
	   "no allocation crosses owner boundary");
	check_plan();
}

static void
test_shared_memtx_quota(void)
{
	plan(8);
	struct quota quota;
	quota_init(&quota, 1024 * 1024);
	struct ann_quota_memory older_owner, newer_owner;
	ann_quota_memory_create(&older_owner, &quota);
	ann_quota_memory_create(&newer_owner, &quota);
	struct ann_memory older_memory = ann_quota_memory_bind(&older_owner);
	struct ann_memory newer_memory = ann_quota_memory_bind(&newer_owner);
	struct ann_config config = {2, ANN_L2, nullptr};
	struct ann_backend *older = nullptr, *newer = nullptr;
	is(ann_usearch_ops.create(&config, &older_memory, &older), ANN_OK,
	   "older graph creates under memtx quota");
	is(ann_usearch_ops.create(&config, &newer_memory, &newer), ANN_OK,
	   "newer graph creates under same quota");
	float vector[] = {1, 2};
	is(insert(&ann_usearch_ops, older, 11, vector), ANN_OK,
	   "older graph grows under quota");
	is(insert(&ann_usearch_ops, newer, 22, vector), ANN_OK,
	   "newer graph grows under quota");
	ok(quota_used(&quota) > 0 && older_owner.blocks > 0 &&
	   newer_owner.blocks > 0, "both owners charge shared quota");
	ann_usearch_ops.destroy(older);
	struct ann_candidate found[2];
	uint32_t count = 0;
	ok(older_owner.blocks == 0 &&
	   search(&ann_usearch_ops, newer, vector, found, &count) == ANN_OK &&
	   count == 1 && found[0].label == 22,
	   "old graph releases charge while newer remains searchable");
	ann_usearch_ops.destroy(newer);
	ok(older_owner.blocks == 0 && newer_owner.blocks == 0,
	   "both graph owners release every allocation");
	is(quota_used(&quota), 0, "memtx quota returns to zero");
	check_plan();
}

static void
test_quota_exhaustion(void)
{
	plan(9);
	struct quota quota;
	quota_init(&quota, 1024 * 1024);
	struct ann_quota_memory owner;
	ann_quota_memory_create(&owner, &quota);
	struct ann_memory memory = ann_quota_memory_bind(&owner);
	struct ann_config config = {2, ANN_L2, nullptr};
	struct ann_backend *backend = nullptr;
	is(ann_usearch_ops.create(&config, &memory, &backend), ANN_OK,
	   "quota-limited graph creates");
	float first[] = {1, 0};
	float second[] = {2, 0};
	is(insert(&ann_usearch_ops, backend, 11, first), ANN_OK,
	   "baseline insertion succeeds");
	size_t charged = quota_used(&quota);
	is(quota_set(&quota, charged), (ssize_t)charged,
	   "quota shrinks to current charge");
	is(insert(&ann_usearch_ops, backend, 22, second), ANN_OUT_OF_MEMORY,
	   "next insertion fails at quota boundary");
	is(quota_used(&quota), charged,
	   "failed insertion returns temporary charges");
	is(quota_set(&quota, 1024 * 1024), 1024 * 1024,
	   "quota limit is restored");
	struct ann_candidate found[2];
	uint32_t count = 0;
	ok(search(&ann_usearch_ops, backend, first, found, &count) == ANN_OK &&
	   count == 1 && found[0].label == 11,
	   "failed insertion leaves baseline graph intact");
	is(insert(&ann_usearch_ops, backend, 22, second), ANN_OK,
	   "insertion succeeds after quota restoration");
	ann_usearch_ops.destroy(backend);
	ok(owner.blocks == 0 && quota_used(&quota) == 0,
	   "destruction returns every quota charge");
	check_plan();
}

static void
test_generation_rebuild(void)
{
	plan(9);
	struct quota quota;
	quota_init(&quota, 8 * 1024 * 1024);
	struct ann_quota_memory old_owner, new_owner;
	ann_quota_memory_create(&old_owner, &quota);
	ann_quota_memory_create(&new_owner, &quota);
	struct ann_memory old_memory = ann_quota_memory_bind(&old_owner);
	struct ann_memory new_memory = ann_quota_memory_bind(&new_owner);
	struct ann_config config = {2, ANN_L2, nullptr};
	struct ann_backend *old_graph = nullptr, *new_graph = nullptr;
	is(ann_usearch_ops.create(&config, &old_memory, &old_graph), ANN_OK,
	   "old generation creates");
	bool inserted = true;
	for (uint64_t label = 1; label <= 32; ++label) {
		float vector[] = {(float)label, 1};
		inserted &= insert(&ann_usearch_ops, old_graph, label,
				   vector) == ANN_OK;
	}
	ok(inserted, "old generation grows to 32 entries");
	bool retired = true;
	for (uint64_t label = 2; label <= 32; label += 2) {
		struct ann_change *change = nullptr;
		retired &= ann_usearch_ops.prepare(old_graph, ANN_RETIRE,
				label, nullptr, 0, &change) == ANN_OK;
		if (change == nullptr)
			break;
		retired &= ann_usearch_ops.apply(change, nullptr) == ANN_OK;
		ann_usearch_ops.finish(change);
		retired &= ann_usearch_ops.reclaim(old_graph, label) == ANN_OK;
	}
	ok(retired, "half the old labels retire and reclaim");
	float extra[] = {33, 1};
	is(insert(&ann_usearch_ops, old_graph, 33, extra), ANN_OK,
	   "capacity growth preserves the reclaimed-slot queue");
	struct ann_backend_stats stats;
	ann_usearch_ops.stat(old_graph, &stats);
	ok(stats.live == 17 && stats.retired == 16 &&
	   stats.reclaimable == 16,
	   "reclaimed navigation slots queue in old generation");
	is(ann_usearch_ops.create(&config, &new_memory, &new_graph), ANN_OK,
	   "replacement generation creates under shared quota");
	bool replayed = true;
	for (uint64_t label = 1; label <= 33; label += 2) {
		float vector[] = {(float)label, 1};
		replayed &= insert(&ann_usearch_ops, new_graph, label,
				   vector) == ANN_OK;
	}
	ok(replayed, "live versions replay into replacement generation");
	ann_usearch_ops.destroy(old_graph);
	float query[] = {31, 1};
	struct ann_candidate found[2];
	uint32_t count = 0;
	ok(old_owner.blocks == 0 &&
	   search(&ann_usearch_ops, new_graph, query, found, &count) ==
	   ANN_OK && count > 0 && found[0].label == 31,
	   "old tombstones release while replacement stays searchable");
	ann_usearch_ops.destroy(new_graph);
	ok(new_owner.blocks == 0 && quota_used(&quota) == 0,
	   "both generations return their full quota charge");
	check_plan();
}

int
main(void)
{
	plan(12);
	test_lifecycle(ann_backend_find("flat"));
	test_lifecycle(&ann_usearch_ops);
	test_failed_insert();
	test_interrupted_search();
	test_failed_search();
	test_interrupted_insert();
	test_rollback_no_alloc();
	test_growth_and_retire();
	test_two_owners();
	test_shared_memtx_quota();
	test_quota_exhaustion();
	test_generation_rebuild();
	return check_plan();
}
