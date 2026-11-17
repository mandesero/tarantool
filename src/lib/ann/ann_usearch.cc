/*
 * SPDX-License-Identifier: BSD-2-Clause
 *
 * Copyright 2026, Tarantool AUTHORS, please see AUTHORS file.
 */
#include "ann_usearch.h"
#include "ann_numeric.h"
#include "ann_vector.h"
#include "ann_hnsw.h"
#include "ann_cow.h"

#include <usearch/index.hpp>

#include <cassert>
#include <cfloat>
#include <cstdint>
#include <cstring>
#include <limits>
#include <new>
#include <utility>

/** A stateful allocator shared by graph, context, and tape allocations. */
/** Allocation metadata immediately before an aligned USearch payload. */
struct ann_usearch_block {
	/** Pointer supplied by ann_memory. */
	void *base;
	/** Full size passed to ann_memory. */
	size_t total;
	/** Element count supplied by USearch. */
	size_t count;
	/** Graph and immutable record references to this tape. */
	uint32_t refs;
};

/** Rebindable allocator preserving the index's memory owner. */
template <class T> struct ann_usearch_allocator {
	using value_type = T;
	/** Underlying index allocator. */
	struct ann_memory memory{};
	/** All retained graph allocations, including alignment overhead. */
	uint64_t *retained{};
	/** Standard allocator rebinding preserves the owner. */
	template <class U> struct rebind {
		using other = ann_usearch_allocator<U>;
	};
	ann_usearch_allocator() noexcept = default;
	ann_usearch_allocator(struct ann_memory value, uint64_t *bytes) noexcept :
		memory(value), retained(bytes) {}
	template <class U>
	ann_usearch_allocator(const ann_usearch_allocator<U> &other) noexcept :
		memory(other.memory), retained(other.retained) {}
	T *allocate(size_t count) noexcept
	{
		if (memory.alloc == nullptr || count == 0 ||
		    count > SIZE_MAX / sizeof(T))
			return nullptr;
		size_t size = count * sizeof(T);
		size_t alignment = alignof(T) > alignof(ann_usearch_block) ?
			alignof(T) : alignof(ann_usearch_block);
		size_t overhead = sizeof(ann_usearch_block) + alignment - 1;
		if (size > SIZE_MAX - overhead)
			return nullptr;
		size_t total = size + overhead;
		void *base = memory.alloc(memory.ctx, total);
		if (base == nullptr)
			return nullptr;
		uintptr_t address = (uintptr_t)base + sizeof(ann_usearch_block);
		address = (address + alignment - 1) & ~(uintptr_t)(alignment - 1);
		ann_usearch_block *header =
			(ann_usearch_block *)(address - sizeof(*header));
		header->base = base;
		header->total = total;
		header->count = count;
		header->refs = 1;
		T *result = (T *)address;
		if (result != nullptr && retained != nullptr)
			*retained += total;
		return result;
	}
	void deallocate(T *ptr, size_t count) noexcept
	{
		if (ptr != nullptr) {
			ann_usearch_block *header =
				(ann_usearch_block *)((uintptr_t)ptr -
						     sizeof(*header));
			assert(header->count == count);
			if (--header->refs != 0)
				return;
			if (retained != nullptr)
				*retained -= header->total;
			memory.free(memory.ctx, header->base, header->total);
		}
	}
	size_t total_allocated() const noexcept { return 0; }
	size_t total_reserved() const noexcept { return 0; }
	size_t total_wasted() const noexcept { return 0; }
};

template <class T, class U>
bool operator==(const ann_usearch_allocator<T> &left,
		const ann_usearch_allocator<U> &right) noexcept
{
	return left.memory.ctx == right.memory.ctx &&
	       left.retained == right.retained;
}

template <class T, class U>
bool operator!=(const ann_usearch_allocator<T> &left,
		const ann_usearch_allocator<U> &right) noexcept
{
	return !(left == right);
}

using ann_graph_allocator = ann_usearch_allocator<unum::usearch::byte_t>;
using ann_graph = unum::usearch::index_gt<double, uint64_t, uint32_t,
					 ann_graph_allocator, ann_graph_allocator>;

/** One slot-stable graph entry. */
struct ann_usearch_entry {
	/** Owned native or borrowed packed coordinates. */
	const void *vector;
	/** Shared coordinate owner, or NULL for an owned test vector. */
	struct ann_vector *shared;
	/** Storage-layer version label. */
	uint64_t label;
	/** Current-view membership. */
	bool live;
	/** Visibility GC released the label while retaining graph navigation. */
	bool reclaimed;
};

/** Open-addressed label lookup with backward-shift deletion. */
struct ann_usearch_lookup_entry {
	/** Version label. */
	uint64_t label;
	/** Graph slot corresponding to the label. */
	uint32_t slot;
	/** Empty or occupied. */
	uint8_t state;
};

/** Immutable slot state shared by live and frozen radix roots. */
struct ann_usearch_record {
	/** Source allocator and tape reference counter. */
	struct ann_backend *owner;
	/** References from persistent leaf pages. */
	uint32_t refs;
	/** Frozen graph tape. */
	unum::usearch::byte_t *tape;
	/** Tape size required by the allocator. */
	size_t bytes;
	/** Frozen label and coordinate owner. */
	struct ann_usearch_entry entry;
};

/** One HNSW generation with owner-bound graph and lookup. */
struct ann_backend {
	/** Common scalar and dimensional configuration. */
	struct ann_config config;
	/** Copied allocation callbacks and owner. */
	struct ann_memory memory;
	/** Immutable HNSW settings. */
	struct ann_hnsw_config options;
	/** Charged allocation bytes including context buffers. */
	uint64_t retained_bytes;
	/** Patched low-level HNSW graph. */
	ann_graph graph;
	/** Slot-indexed vector lookup. */
	struct ann_usearch_entry *entries;
	/** Label-to-slot lookup without historical tombstones. */
	struct ann_usearch_lookup_entry *lookup;
	/** GC-safe slots reusable without growing the graph. */
	uint32_t *reclaim_queue;
	/** Number of reclaimed slots in the queue. */
	uint32_t reclaim_count;
	/** Power-of-two lookup table size. */
	size_t lookup_capacity;
	/** Allocated lookup and graph slots. */
	uint32_t capacity;
	/** Top of the change undo stack. */
	struct ann_change *pending;
	/** Immutable node tapes and coordinates indexed by slot. */
	struct ann_cow records;
	/** Live source retained by a frozen backend, otherwise NULL. */
	struct ann_backend *source;
	/** Live owner and frozen backend references. */
	uint32_t refs;

	ann_backend(const struct ann_config &cfg, struct ann_memory mem,
		    struct ann_hnsw_config opts) : config(cfg), memory(mem),
	    options(opts), retained_bytes(sizeof(*this)),
	    graph(unum::usearch::index_config_t{opts.connectivity},
		  ann_graph_allocator{mem, &retained_bytes},
		  ann_graph_allocator{mem, &retained_bytes}),
	    entries(nullptr), lookup(nullptr), reclaim_queue(nullptr),
	    reclaim_count(0), lookup_capacity(0),
	    capacity(0), pending(nullptr), records{}, source(nullptr),
	    refs(1) {}
};

/** Prepared version change with graph-level undo. */
struct ann_change {
	/** Owning backend generation. */
	struct ann_backend *backend;
	/** Older applied change. */
	struct ann_change *previous;
	/** Insert or retire. */
	enum ann_change_kind kind;
	/** Storage-layer version label. */
	uint64_t label;
	/** Vector awaiting transfer to the slot. */
	const void *vector;
	/** Shared coordinate owner awaiting transfer. */
	struct ann_vector *shared;
	/** Graph slot affected by this change. */
	uint32_t slot;
	/** Whether this change altered the current view. */
	bool applied;
	/** Whether allocation-free undo consumed this change. */
	bool rolled_back;
	/** Whether insertion replaces an externally reclaimed graph node. */
	bool reused;
	/** Whether this change exclusively reserves a reusable slot. */
	bool slot_reserved;
	/** Previous navigation owner retained until successful finish. */
	struct ann_usearch_entry old_entry;
	/** Own and reverse-link snapshots retained until finish. */
	ann_graph::add_undo_t undo;
	/** Persistent root used by allocation-free pointer undo. */
	struct ann_cow old_records;
};

static size_t
ann_vector_size(const struct ann_backend *backend)
{
	return (size_t)backend->config.dimension * sizeof(float);
}

static void *
ann_usearch_alloc(struct ann_backend *backend, size_t size)
{
	void *ptr = backend->memory.alloc(backend->memory.ctx, size);
	if (ptr != nullptr)
		backend->retained_bytes += size;
	return ptr;
}

static void
ann_usearch_free(struct ann_backend *backend, void *ptr, size_t size)
{
	if (ptr == nullptr)
		return;
	backend->retained_bytes -= size;
	backend->memory.free(backend->memory.ctx, ptr, size);
}

static void *
ann_usearch_cow_alloc(void *ctx, size_t size)
{
	return ann_usearch_alloc((struct ann_backend *)ctx, size);
}

static void
ann_usearch_cow_free(void *ctx, void *ptr, size_t size)
{
	ann_usearch_free((struct ann_backend *)ctx, ptr, size);
}

static ann_usearch_block *
ann_usearch_header(void *ptr)
{
	uintptr_t address = (uintptr_t)ptr - sizeof(ann_usearch_block);
	return (ann_usearch_block *)address;
}

static void
ann_usearch_tape_ref(void *ptr)
{
	auto *header = ann_usearch_header(ptr);
	assert(header->refs != 0 && header->refs < UINT32_MAX);
	++header->refs;
}

static void
ann_usearch_record_ref(void *ptr)
{
	auto *record = (struct ann_usearch_record *)ptr;
	assert(record->refs < UINT32_MAX);
	++record->refs;
}

/** Immutable records own both the tape and its shared coordinates. */
static void
ann_usearch_record_unref(void *ptr)
{
	auto *record = (struct ann_usearch_record *)ptr;
	if (--record->refs != 0)
		return;
	auto *owner = record->owner;
	ann_graph_allocator allocator{owner->memory, &owner->retained_bytes};
	allocator.deallocate(record->tape, record->bytes);
	ann_vector_unref(record->entry.shared);
	ann_usearch_free(owner, record, sizeof(*record));
}

/** Frozen lookups must not consult the mutable live entry array. */
static const struct ann_usearch_entry *
ann_usearch_entry_at(const struct ann_backend *backend, uint32_t slot)
{
	if (backend->source == nullptr)
		return &backend->entries[slot];
	auto *record = (struct ann_usearch_record *)
		ann_cow_get(&backend->records, slot);
	assert(record != nullptr);
	return &record->entry;
}

/** Resolve without locking or modifying the persistent root. */
static unum::usearch::byte_t *
ann_usearch_resolve(void *ctx, size_t slot)
{
	auto *backend = (struct ann_backend *)ctx;
	auto *record = (struct ann_usearch_record *)
		ann_cow_get(&backend->records, slot);
	assert(record != nullptr);
	return record->tape;
}

/** The write barrier never changes tapes reachable from a frozen root. */
static bool
ann_usearch_touch(void *ctx, size_t slot)
{
	auto *backend = (struct ann_backend *)ctx;
	auto bytes = backend->graph.node_bytes(slot);
	if (ann_usearch_header(bytes.data())->refs == 1)
		return true;
	ann_graph_allocator allocator{backend->memory,
		&backend->retained_bytes};
	auto *copy = allocator.allocate(bytes.size());
	if (copy == nullptr)
		return false;
	memcpy(copy, bytes.data(), bytes.size());
	backend->graph.replace_node(slot, copy);
	allocator.deallocate(bytes.data(), bytes.size());
	return true;
}

/** Publish one immutable slot only after all graph writes have completed. */
static enum ann_status
ann_usearch_publish(struct ann_backend *backend, uint32_t slot)
{
	auto bytes = backend->graph.node_bytes(slot);
	auto *old = (struct ann_usearch_record *)
		ann_cow_get(&backend->records, slot);
	if (old != nullptr && old->tape == bytes.data())
		return ANN_OK;
	struct ann_usearch_record *record = (struct ann_usearch_record *)
		ann_usearch_alloc(backend, sizeof(*record));
	if (record == nullptr)
		return ANN_OUT_OF_MEMORY;
	record->owner = backend;
	record->refs = 1;
	record->tape = bytes.data();
	record->bytes = bytes.size();
	record->entry = backend->entries[slot];
	assert(record->entry.shared != nullptr);
	ann_vector_ref(record->entry.shared);
	ann_usearch_tape_ref(record->tape);
	enum ann_status status = ann_cow_set(&backend->records, slot, record);
	ann_usearch_record_unref(record);
	return status;
}

/** Restore tape pointers instead of writing adjacency lists shared by views. */
static void
ann_usearch_undo_nodes(struct ann_change *change)
{
	auto *backend = change->backend;
	auto restore = [change, backend](uint32_t slot) {
		auto *record = (struct ann_usearch_record *)
			ann_cow_get(&change->old_records, slot);
		assert(record != nullptr);
		auto current = backend->graph.node_bytes(slot);
		ann_usearch_tape_ref(record->tape);
		backend->graph.replace_node(slot, record->tape);
		ann_graph_allocator allocator{backend->memory,
			&backend->retained_bytes};
		allocator.deallocate(current.data(), current.size());
	};
	change->undo.rollback_nodes(backend->graph, restore);
	ann_cow_destroy(&backend->records);
	ann_cow_copy(&backend->records, &change->old_records);
}

/** Coordinates can belong to this generation or an independent owner. */
static void
ann_usearch_release_vector(struct ann_backend *backend, const void *vector,
			   struct ann_vector *shared)
{
	if (shared != nullptr)
		ann_vector_unref(shared);
	else
		ann_usearch_free(backend, (void *)vector,
				 ann_vector_size(backend));
}

/** Mix pointer-like and sequential labels before masking to table capacity. */
static uint64_t
ann_usearch_hash(uint64_t value)
{
	value ^= value >> 30;
	value *= UINT64_C(0xbf58476d1ce4e5b9);
	value ^= value >> 27;
	value *= UINT64_C(0x94d049bb133111eb);
	return value ^ (value >> 31);
}

/** Find a label before reaching the first empty bucket. */
static size_t
ann_usearch_lookup_find(const struct ann_usearch_lookup_entry *lookup,
			size_t capacity, uint64_t label)
{
	if (capacity == 0)
		return SIZE_MAX;
	size_t offset = ann_usearch_hash(label) & (capacity - 1);
	for (size_t i = 0; i < capacity; ++i) {
		size_t at = (offset + i) & (capacity - 1);
		if (lookup[at].state == 0)
			return SIZE_MAX;
		if (lookup[at].state == 1 && lookup[at].label == label)
			return at;
	}
	return SIZE_MAX;
}

/** Insert into a table whose occupied load is at most one half. */
static void
ann_usearch_lookup_insert(struct ann_usearch_lookup_entry *lookup,
			  size_t capacity, uint64_t label, uint32_t slot)
{
	assert(capacity != 0);
	size_t at = ann_usearch_hash(label) & (capacity - 1);
	while (lookup[at].state != 0)
		at = (at + 1) & (capacity - 1);
	lookup[at].label = label;
	lookup[at].slot = slot;
	lookup[at].state = 1;
}

/** Finds a retained label; graph slots remain stable until rebuilding. */
static int64_t
ann_usearch_find(const struct ann_backend *backend, uint64_t label)
{
	size_t at = ann_usearch_lookup_find(backend->lookup,
					    backend->lookup_capacity, label);
	if (at == SIZE_MAX)
		return -1;
	return backend->lookup[at].slot;
}

/** Close the deleted probe gap without retaining historical tombstones. */
static void
ann_usearch_lookup_erase(struct ann_backend *backend, uint64_t label)
{
	struct ann_usearch_lookup_entry *lookup = backend->lookup;
	size_t capacity = backend->lookup_capacity;
	size_t mask = capacity - 1;
	size_t hole = ann_usearch_lookup_find(lookup, capacity, label);
	assert(hole != SIZE_MAX);
	for (size_t at = (hole + 1) & mask; lookup[at].state != 0;
	     at = (at + 1) & mask) {
		size_t home = ann_usearch_hash(lookup[at].label) & mask;
		if (((at - home) & mask) >= ((at - hole) & mask)) {
			lookup[hole] = lookup[at];
			hole = at;
		}
	}
	lookup[hole].state = 0;
}

/** A coordinate reference carries its storage byte order. */
struct ann_coordinate_ref {
	/** Coordinates retained by the caller or graph. */
	const void *data;
	/** Whether bytes use the packed little-endian format. */
	bool packed;
};

/** Distance bridge resolves graph members without copying coordinates. */
struct ann_usearch_metric {
	/** Graph and slot lookup owner. */
	const struct ann_backend *backend;
	/** Optional operation budget. */
	struct ann_search_control *control;
	/** First distance or stop failure. */
	enum ann_status *status;
	/** Byte order of a raw insertion input. */
	bool input_packed;

	ann_coordinate_ref vector(const float *value) const noexcept
	{
		return {value, false};
	}
	ann_coordinate_ref vector(const void *value) const noexcept
	{
		return {value, input_packed};
	}
	template <class K>
	ann_coordinate_ref vector(unum::usearch::member_cref_gt<K> value)
		const noexcept
	{
		const auto *entry = ann_usearch_entry_at(backend, value.slot);
		return {entry->vector, entry->shared != nullptr &&
			ann_vector_is_borrowed(entry->shared)};
	}
	template <class K>
	ann_coordinate_ref vector(unum::usearch::member_ref_gt<K> value)
		const noexcept
	{
		auto ref = static_cast<unum::usearch::member_cref_gt<K>>(value);
		return vector(ref);
	}
	template <class T>
	ann_coordinate_ref vector(const T &value) const noexcept
	{
		return vector(*value);
	}
	template <class A, class B>
	double operator()(const A &left, const B &right) const noexcept
	{
		if (*status != ANN_OK)
			return DBL_MAX;
		double distance = 0;
		auto a = vector(left);
		auto b = vector(right);
		*status = ann_distance_raw(
			a.data, a.packed, b.data, b.packed,
			backend->config.dimension, backend->config.metric,
			control, &distance);
		return *status == ANN_OK ? distance : DBL_MAX;
	}
};

/** Callback state shared by every graph traversal checkpoint. */
struct ann_usearch_stop {
	/** Operation budget to charge per candidate or edge. */
	struct ann_search_control *control;
	/** First non-success status. */
	enum ann_status *status;
};

static bool
ann_usearch_should_stop(void *ctx) noexcept
{
	struct ann_usearch_stop *stop = (struct ann_usearch_stop *)ctx;
	if (*stop->status != ANN_OK)
		return true;
	*stop->status = ann_search_control_step(stop->control);
	return *stop->status != ANN_OK;
}

static enum ann_status
ann_usearch_create(const struct ann_config *config,
		   const struct ann_memory *memory,
		   struct ann_backend **out)
{
	if (out == nullptr)
		return ANN_INVALID;
	*out = nullptr;
	if (config == nullptr || memory == nullptr ||
	    memory->alloc == nullptr || memory->free == nullptr ||
	    config->dimension == 0 ||
	    ((size_t)config->dimension * sizeof(float)) / sizeof(float) !=
		config->dimension ||
	    (unsigned)config->metric > ANN_IP)
		return ANN_INVALID;
	struct ann_hnsw_config options = {16, 200, 64};
	if (config->algorithm != nullptr)
		options = *(const struct ann_hnsw_config *)config->algorithm;
	if (options.connectivity < 2 || options.connectivity > UINT32_MAX / 2 ||
	    options.expansion_add == 0 || options.expansion_search == 0)
		return ANN_INVALID;
	void *storage = memory->alloc(memory->ctx, sizeof(struct ann_backend));
	if (storage == nullptr)
		return ANN_OUT_OF_MEMORY;
	struct ann_config copied = *config;
	copied.algorithm = nullptr;
	struct ann_backend *backend = new (storage)
		ann_backend(copied, *memory, options);
	struct ann_memory cow_memory = {backend, ann_usearch_cow_alloc,
		ann_usearch_cow_free};
	ann_cow_create(&backend->records, &cow_memory,
		       ann_usearch_record_ref, ann_usearch_record_unref);
	*out = backend;
	return ANN_OK;
}

/** A source header owns allocator counters until its last view is closed. */
static void
ann_usearch_unref(struct ann_backend *backend)
{
	if (--backend->refs != 0)
		return;
	struct ann_memory memory = backend->memory;
	backend->~ann_backend();
	memory.free(memory.ctx, backend, sizeof(*backend));
}

static void
ann_usearch_destroy(struct ann_backend *backend)
{
	if (backend == nullptr)
		return;
	if (backend->source != nullptr) {
		auto *source = backend->source;
		ann_cow_destroy(&backend->records);
		ann_usearch_unref(backend);
		ann_usearch_unref(source);
		return;
	}
	for (struct ann_change *change = backend->pending; change != nullptr;) {
		struct ann_change *previous = change->previous;
		ann_usearch_release_vector(backend, change->vector,
					   change->shared);
		if (change->reused && change->applied) {
			ann_usearch_release_vector(backend,
						   change->old_entry.vector,
						   change->old_entry.shared);
		}
		change->undo.commit();
		ann_cow_destroy(&change->old_records);
		change->~ann_change();
		ann_usearch_free(backend, change, sizeof(*change));
		change = previous;
	}
	for (uint32_t i = 0; i < backend->graph.size(); ++i)
		ann_usearch_release_vector(backend, backend->entries[i].vector,
					   backend->entries[i].shared);
	ann_usearch_free(backend, backend->entries,
			 (size_t)backend->capacity * sizeof(*backend->entries));
	ann_usearch_free(backend, backend->lookup,
		 backend->lookup_capacity * sizeof(*backend->lookup));
	ann_usearch_free(backend, backend->reclaim_queue,
		 (size_t)backend->capacity * sizeof(*backend->reclaim_queue));
	backend->entries = nullptr;
	backend->lookup = nullptr;
	backend->reclaim_queue = nullptr;
	backend->graph.reset();
	ann_cow_destroy(&backend->records);
	ann_usearch_unref(backend);
}

/** Root retention captures topology in constant time and space. */
static enum ann_status
ann_usearch_snapshot(struct ann_backend *backend,
		     const struct ann_memory *memory,
		     struct ann_backend **out)
{
	if (out == nullptr)
		return ANN_INVALID;
	*out = nullptr;
	if (backend == nullptr || backend->source != nullptr ||
	    memory == nullptr || memory->alloc == nullptr ||
	    memory->free == nullptr)
		return ANN_INVALID;
	struct ann_config config = backend->config;
	config.algorithm = &backend->options;
	struct ann_backend *view;
	enum ann_status status = ann_usearch_create(&config, memory, &view);
	if (status != ANN_OK)
		return status;
	if (!view->graph.snapshot_from(backend->graph,
				       ann_usearch_resolve, view, false)) {
		ann_usearch_unref(view);
		return ANN_OUT_OF_MEMORY;
	}
	ann_cow_copy(&view->records, &backend->records);
	assert(backend->refs < UINT32_MAX);
	++backend->refs;
	view->source = backend;
	*out = view;
	return ANN_OK;
}

static enum ann_status
ann_usearch_reserve(struct ann_backend *backend, uint32_t capacity)
{
	if (backend == nullptr || backend->source != nullptr)
		return ANN_INVALID;
	if (backend->pending != nullptr && !backend->pending->applied)
		return ANN_BUSY;
	if (capacity <= backend->capacity)
		return ANN_OK;
	if ((size_t)capacity > SIZE_MAX / sizeof(*backend->entries))
		return ANN_INVALID;
	size_t size = (size_t)capacity * sizeof(*backend->entries);
	struct ann_usearch_entry *entries =
		(struct ann_usearch_entry *)ann_usearch_alloc(backend, size);
	if (entries == nullptr)
		return ANN_OUT_OF_MEMORY;
	memset(entries, 0, size);
	if (backend->graph.size() != 0)
		memcpy(entries, backend->entries, backend->graph.size() *
		       sizeof(*entries));
	size_t lookup_capacity = 8;
	while (lookup_capacity / 2 < capacity) {
		if (lookup_capacity > SIZE_MAX / 2) {
			ann_usearch_free(backend, entries, size);
			return ANN_INVALID;
		}
		lookup_capacity *= 2;
	}
	if (lookup_capacity > SIZE_MAX / sizeof(*backend->lookup)) {
		ann_usearch_free(backend, entries, size);
		return ANN_INVALID;
	}
	size_t lookup_size = lookup_capacity * sizeof(*backend->lookup);
	struct ann_usearch_lookup_entry *lookup =
		(struct ann_usearch_lookup_entry *)ann_usearch_alloc(
			backend, lookup_size);
	if (lookup == nullptr) {
		ann_usearch_free(backend, entries, size);
		return ANN_OUT_OF_MEMORY;
	}
	memset(lookup, 0, lookup_size);
	size_t queue_size = (size_t)capacity * sizeof(*backend->reclaim_queue);
	uint32_t *queue = (uint32_t *)ann_usearch_alloc(backend, queue_size);
	if (queue == nullptr) {
		ann_usearch_free(backend, lookup, lookup_size);
		ann_usearch_free(backend, entries, size);
		return ANN_OUT_OF_MEMORY;
	}
	if (backend->reclaim_count != 0)
		memcpy(queue, backend->reclaim_queue, backend->reclaim_count *
		       sizeof(*queue));
	for (uint32_t i = 0; i < backend->graph.size(); ++i) {
		if (!entries[i].reclaimed)
			ann_usearch_lookup_insert(lookup, lookup_capacity,
						  entries[i].label, i);
	}
	if (!backend->graph.try_reserve(unum::usearch::index_limits_t{
		capacity, 1})) {
		ann_usearch_free(backend, queue, queue_size);
		ann_usearch_free(backend, lookup, lookup_size);
		ann_usearch_free(backend, entries, size);
		return ANN_OUT_OF_MEMORY;
	}
	ann_usearch_free(backend, backend->entries,
			 (size_t)backend->capacity * sizeof(*entries));
	ann_usearch_free(backend, backend->lookup,
		 backend->lookup_capacity * sizeof(*lookup));
	ann_usearch_free(backend, backend->reclaim_queue,
		 (size_t)backend->capacity * sizeof(*queue));
	backend->entries = entries;
	backend->lookup = lookup;
	backend->reclaim_queue = queue;
	backend->lookup_capacity = lookup_capacity;
	backend->capacity = capacity;
	return ANN_OK;
}

static enum ann_status
ann_usearch_prepare_impl(struct ann_backend *backend,
			 enum ann_change_kind kind,
		    uint64_t label, const void *vector, uint32_t dimension,
		    struct ann_change **out,
		    struct ann_vector *shared)
{
	if (out == nullptr)
		return ANN_INVALID;
	*out = nullptr;
	if (backend == nullptr || backend->source != nullptr ||
	    (kind != ANN_INSERT && kind != ANN_RETIRE))
		return ANN_INVALID;
	if (backend->pending != nullptr && !backend->pending->applied)
		return ANN_BUSY;
	int64_t slot = ann_usearch_find(backend, label);
	if (kind == ANN_INSERT) {
		if (slot >= 0)
			return ANN_EXISTS;
		enum ann_status valid = shared != nullptr &&
			ann_vector_is_borrowed(shared) ?
			ann_vector_validate_le(vector, dimension,
					       backend->config.metric) :
			ann_vector_validate_f32(
				(const float *)vector, dimension,
				backend->config.metric);
		if (dimension != backend->config.dimension || valid != ANN_OK)
			return ANN_INVALID;
		if (backend->reclaim_count == 0 &&
		    backend->graph.size() == UINT32_MAX)
			return ANN_INVALID;
		if (backend->reclaim_count == 0 &&
		    backend->graph.size() == backend->capacity) {
			uint32_t next = backend->capacity == 0 ? 4 :
				backend->capacity > UINT32_MAX / 2 ?
				UINT32_MAX : backend->capacity * 2;
			enum ann_status status = ann_usearch_reserve(backend, next);
			if (status != ANN_OK)
				return status;
		}
	} else if (vector != nullptr || dimension != 0) {
		return ANN_INVALID;
	} else if (slot < 0 || !backend->entries[slot].live) {
		return ANN_NOT_FOUND;
	}
	void *storage = ann_usearch_alloc(backend, sizeof(struct ann_change));
	if (storage == nullptr)
		return ANN_OUT_OF_MEMORY;
	struct ann_change *change = new (storage) ann_change{};
	change->backend = backend;
	change->previous = backend->pending;
	change->kind = kind;
	change->label = label;
	change->reused = kind == ANN_INSERT && backend->reclaim_count != 0;
	change->slot = kind == ANN_RETIRE ? slot : change->reused ?
		backend->reclaim_queue[backend->reclaim_count - 1] :
		backend->graph.size();
	if (kind == ANN_INSERT && shared != nullptr) {
		ann_vector_ref(shared);
		change->shared = shared;
		change->vector = ann_vector_data(shared);
	} else if (kind == ANN_INSERT) {
		struct ann_memory memory = {backend, ann_usearch_cow_alloc,
			ann_usearch_cow_free};
		change->shared = ann_vector_new(&memory, (const float *)vector,
						dimension);
		if (change->shared == nullptr) {
			change->~ann_change();
			ann_usearch_free(backend, change, sizeof(*change));
			return ANN_OUT_OF_MEMORY;
		}
		change->vector = ann_vector_data(change->shared);
	}
	if (change->reused) {
		change->old_entry = backend->entries[change->slot];
		assert(change->old_entry.reclaimed && !change->old_entry.live);
		--backend->reclaim_count;
		change->slot_reserved = true;
	}
	backend->pending = change;
	*out = change;
	return ANN_OK;
}

static enum ann_status
ann_usearch_prepare(struct ann_backend *backend, enum ann_change_kind kind,
		    uint64_t label, const float *vector, uint32_t dimension,
		    struct ann_change **out)
{
	return ann_usearch_prepare_impl(backend, kind, label, vector, dimension,
					out, nullptr);
}

static enum ann_status
ann_usearch_prepare_shared(struct ann_backend *backend, uint64_t label,
			   struct ann_vector *vector,
			   struct ann_change **out)
{
	if (vector == nullptr) {
		if (out != nullptr)
			*out = nullptr;
		return ANN_INVALID;
	}
	const void *data = ann_vector_data(vector);
	uint32_t dimension = ann_vector_dimension(vector);
	return ann_usearch_prepare_impl(backend, ANN_INSERT, label, data,
					dimension, out, vector);
}

static bool
ann_usearch_contains(const struct ann_backend *backend, uint64_t label)
{
	return backend != nullptr && ann_usearch_find(backend, label) >= 0;
}

static uint64_t
ann_usearch_reclaimable(const struct ann_backend *backend, uint64_t *size)
{
	*size = backend->graph.size();
	return backend->reclaim_count;
}

static enum ann_status
ann_usearch_apply(struct ann_change *change,
		  struct ann_search_control *control)
{
	if (change == nullptr || change->backend->pending != change ||
	    change->applied || change->rolled_back)
		return ANN_INVALID;
	if (control != nullptr) {
		enum ann_status status = ann_search_control_poll(control);
		if (status != ANN_OK)
			return status;
	}
	struct ann_backend *backend = change->backend;
	if (change->kind == ANN_RETIRE) {
		backend->entries[change->slot].live = false;
		change->applied = true;
		return ANN_OK;
	}
	ann_cow_destroy(&change->old_records);
	ann_cow_copy(&change->old_records, &backend->records);
	struct ann_usearch_entry *entry = &backend->entries[change->slot];
	entry->vector = change->vector;
	entry->shared = change->shared;
	entry->label = change->label;
	entry->live = true;
	entry->reclaimed = false;
	enum ann_status status = ANN_OK;
	ann_usearch_metric metric{backend, control, &status,
		change->shared != nullptr &&
		ann_vector_is_borrowed(change->shared)};
	struct ann_usearch_stop stop{control, &status};
	unum::usearch::index_update_config_t config;
	config.expansion = backend->options.expansion_add;
	config.before_write = ann_usearch_touch;
	config.write_context = backend;
	if (control != nullptr) {
		config.should_stop = ann_usearch_should_stop;
		config.stop_context = &stop;
	}
	auto result = change->reused ?
		backend->graph.update(backend->graph.iterator_at(change->slot),
			change->label, change->vector, metric, config,
			[](auto) {}, [](auto, auto) {}, &change->undo) :
		backend->graph.add(change->label, change->vector, metric,
			config, [](auto) {}, [](auto, auto) {}, &change->undo);
	if (result && status == ANN_OK) {
		auto publish = [backend, &status](uint32_t slot) {
			if (status == ANN_OK)
				status = ann_usearch_publish(backend, slot);
		};
		change->undo.for_each_changed(publish);
		if (status == ANN_OK)
			status = ann_usearch_publish(backend, change->slot);
	}
	if (result && status == ANN_OK && control != nullptr)
		status = ann_search_control_poll(control);
	if (!result || status != ANN_OK) {
		result.error.release();
		ann_usearch_undo_nodes(change);
		if (change->reused)
			*entry = change->old_entry;
		else
			memset(entry, 0, sizeof(*entry));
		return status != ANN_OK ? status : ANN_OUT_OF_MEMORY;
	}
	change->vector = nullptr;
	change->shared = nullptr;
	ann_usearch_lookup_insert(backend->lookup, backend->lookup_capacity,
				  change->label, change->slot);
	change->applied = true;
	return ANN_OK;
}

static void
ann_usearch_rollback(struct ann_change *change)
{
	if (change == nullptr || change->rolled_back)
		return;
	struct ann_backend *backend = change->backend;
	assert(backend->pending == change);
	change->rolled_back = true;
	if (change->slot_reserved) {
		assert(backend->reclaim_count < backend->capacity);
		backend->reclaim_queue[backend->reclaim_count++] = change->slot;
		change->slot_reserved = false;
	}
	if (!change->applied)
		return;
	if (change->kind == ANN_RETIRE) {
		backend->entries[change->slot].live = true;
	} else {
		ann_usearch_lookup_erase(backend, change->label);
		ann_usearch_undo_nodes(change);
		struct ann_usearch_entry *entry = &backend->entries[change->slot];
		ann_usearch_release_vector(backend, entry->vector,
					   entry->shared);
		if (change->reused)
			*entry = change->old_entry;
		else
			memset(entry, 0, sizeof(*entry));
	}
	change->applied = false;
}

static void
ann_usearch_finish(struct ann_change *change)
{
	if (change == nullptr)
		return;
	struct ann_backend *backend = change->backend;
	assert(backend->pending == change);
	backend->pending = change->previous;
	change->undo.commit();
	ann_cow_destroy(&change->old_records);
	if (change->reused && change->applied) {
		ann_usearch_release_vector(backend, change->old_entry.vector,
					   change->old_entry.shared);
	} else if (change->slot_reserved) {
		assert(backend->reclaim_count < backend->capacity);
		backend->reclaim_queue[backend->reclaim_count++] = change->slot;
	}
	ann_usearch_release_vector(backend, change->vector,
				   change->shared);
	change->~ann_change();
	ann_usearch_free(backend, change, sizeof(*change));
}

static enum ann_status
ann_usearch_set_live(struct ann_backend *backend, uint64_t label, bool live)
{
	if (backend == nullptr)
		return ANN_INVALID;
	int64_t slot = ann_usearch_find(backend, label);
	if (slot < 0)
		return ANN_NOT_FOUND;
	backend->entries[slot].live = live;
	return ANN_OK;
}

static bool
ann_usearch_is_live(const struct ann_backend *backend, uint64_t label)
{
	if (backend == nullptr)
		return false;
	int64_t slot = ann_usearch_find(backend, label);
	return slot >= 0 && backend->entries[slot].live;
}

static enum ann_status
ann_usearch_reclaim(struct ann_backend *backend, uint64_t label)
{
	if (backend == nullptr)
		return ANN_INVALID;
	if (backend->pending != nullptr)
		return ANN_BUSY;
	int64_t slot = ann_usearch_find(backend, label);
	if (slot < 0 || backend->entries[slot].live)
		return ANN_NOT_FOUND;
	/* Navigation remains valid until a successful slot update. */
	ann_usearch_lookup_erase(backend, label);
	backend->entries[slot].reclaimed = true;
	assert(backend->reclaim_count < backend->capacity);
	backend->reclaim_queue[backend->reclaim_count++] = slot;
	return ANN_OK;
}

static enum ann_status
ann_usearch_search(struct ann_backend *backend, const float *query,
		   const struct ann_search_opts *opts,
		   struct ann_candidate *out, uint32_t *out_count)
{
	if (out_count != nullptr)
		*out_count = 0;
	if (backend == nullptr || query == nullptr || opts == nullptr ||
	    opts->control == nullptr || out_count == nullptr ||
	    (opts->candidate_limit != 0 && out == nullptr) ||
	    opts->query_dimension != backend->config.dimension ||
	    (opts->filter != nullptr && opts->filter->accept == nullptr) ||
	    (backend->source != nullptr && opts->filter == nullptr))
		return ANN_INVALID;
	enum ann_status status = ann_vector_validate_f32(query,
		backend->config.dimension, backend->config.metric);
	if (status != ANN_OK)
		return status;
	status = ann_search_control_poll(opts->control);
	if (status != ANN_OK || opts->candidate_limit == 0)
		return status;
	uint32_t expansion = backend->options.expansion_search;
	if (opts->algorithm != nullptr) {
		expansion = ((const struct ann_hnsw_search_opts *)
			     opts->algorithm)->expansion;
		if (expansion == 0)
			return ANN_INVALID;
	}
	ann_usearch_metric metric{backend, opts->control, &status, false};
	struct ann_usearch_stop stop{opts->control, &status};
	unum::usearch::index_search_config_t config;
	config.expansion = expansion;
	config.should_stop = ann_usearch_should_stop;
	config.stop_context = &stop;
	auto predicate = [backend, opts](ann_graph::member_cref_t member) {
		const struct ann_usearch_entry *entry =
			ann_usearch_entry_at(backend, member.slot);
		if (backend->source == nullptr && entry->reclaimed)
			return false;
		return opts->filter == nullptr ? entry->live :
			opts->filter->accept(entry->label, entry->shared,
				     opts->filter->ctx);
	};
	/* Scratch state belongs to this search, including on other cords. */
	uint64_t scratch_bytes = 0;
	struct ann_memory memory = opts->memory == nullptr ? backend->memory :
		*opts->memory;
	ann_graph frozen(unum::usearch::index_config_t{
		backend->options.connectivity},
		ann_graph_allocator{memory, &scratch_bytes},
		ann_graph_allocator{memory, &scratch_bytes});
	ann_graph *graph = &backend->graph;
	if (backend->source != nullptr) {
		if (!frozen.snapshot_from(backend->graph,
					  ann_usearch_resolve, backend))
			return ANN_OUT_OF_MEMORY;
		graph = &frozen;
	}
	auto result = graph->search(query, opts->candidate_limit,
					    metric, config, predicate);
	if (!result) {
		result.error.release();
		return status != ANN_OK ? status : ANN_OUT_OF_MEMORY;
	}
	if (status != ANN_OK)
		return status;
	for (size_t i = 0; i < result.size(); ++i) {
		status = ann_search_control_step(opts->control);
		if (status != ANN_OK)
			return status;
		auto match = result[i];
		const auto *entry =
			ann_usearch_entry_at(backend, match.member.slot);
		out[i].label = entry->label;
		out[i].vector = entry->shared;
		out[i].distance = match.distance;
	}
	status = ann_search_control_poll(opts->control);
	if (status != ANN_OK)
		return status;
	*out_count = result.size();
	return ANN_OK;
}

static void
ann_usearch_stat(const struct ann_backend *backend,
		 struct ann_backend_stats *out)
{
	memset(out, 0, sizeof(*out));
	out->capacity = backend->capacity;
	out->reclaimable = backend->reclaim_count;
	out->resident_bytes = backend->retained_bytes;
	out->lookup_bytes = (uint64_t)backend->capacity *
		(sizeof(*backend->entries) + sizeof(*backend->reclaim_queue)) +
		(uint64_t)backend->lookup_capacity * sizeof(*backend->lookup);
	for (uint32_t i = 0; i < backend->graph.size(); ++i) {
		const auto *entry = ann_usearch_entry_at(backend, i);
		if (entry->vector != nullptr && entry->shared == nullptr)
			out->vector_bytes += (uint64_t)backend->config.dimension *
					     sizeof(float);
		if (entry->live)
			++out->live;
		else
			++out->retired;
	}
	out->graph_bytes = out->resident_bytes - out->lookup_bytes -
			   out->vector_bytes;
}

extern "C" const struct ann_backend_ops ann_usearch_ops = {
	"usearch", ann_usearch_create, ann_usearch_destroy,
	ann_usearch_reserve, ann_usearch_prepare, ann_usearch_prepare_shared,
	ann_usearch_contains, ann_usearch_reclaimable, ann_usearch_apply,
	ann_usearch_rollback, ann_usearch_finish, ann_usearch_set_live,
	ann_usearch_is_live,
	ann_usearch_reclaim,
	ann_usearch_search, ann_usearch_stat, ann_usearch_snapshot,
};
