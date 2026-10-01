/*
 * SPDX-License-Identifier: BSD-2-Clause
 *
 * Copyright 2026, Tarantool AUTHORS, please see AUTHORS file.
 */
#include "ann_usearch.h"
#include "ann_numeric.h"

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
	/** Owned canonical float32 vector. */
	float *vector;
	/** Storage-layer version label. */
	uint64_t label;
	/** Current-view membership. */
	bool live;
	/** Visibility GC released the label while retaining graph navigation. */
	bool reclaimed;
};

/** Open-addressed label lookup; state 2 preserves probe chains. */
struct ann_usearch_lookup_entry {
	/** Version label. */
	uint64_t label;
	/** Graph slot corresponding to the label. */
	uint32_t slot;
	/** Empty, occupied, or deleted. */
	uint8_t state;
};

/** One HNSW generation with owner-bound graph and lookup. */
struct ann_backend {
	/** Common scalar and dimensional configuration. */
	struct ann_config config;
	/** Copied allocation callbacks and owner. */
	struct ann_memory memory;
	/** Immutable HNSW settings. */
	struct ann_usearch_config options;
	/** Charged allocation bytes including context buffers. */
	uint64_t retained_bytes;
	/** Patched low-level HNSW graph. */
	ann_graph graph;
	/** Slot-indexed vector lookup. */
	struct ann_usearch_entry *entries;
	/** Label-to-slot lookup with tombstones. */
	struct ann_usearch_lookup_entry *lookup;
	/** Reclaimed graph slots awaiting generation rebuild. */
	uint32_t *reclaim_queue;
	/** Number of reclaimed slots in the queue. */
	uint32_t reclaim_count;
	/** Power-of-two lookup table size. */
	size_t lookup_capacity;
	/** Allocated lookup and graph slots. */
	uint32_t capacity;
	/** Top of the change undo stack. */
	struct ann_change *pending;

	ann_backend(const struct ann_config &cfg, struct ann_memory mem,
		    struct ann_usearch_config opts) : config(cfg), memory(mem),
	    options(opts), retained_bytes(sizeof(*this)),
	    graph(unum::usearch::index_config_t{opts.connectivity},
		  ann_graph_allocator{mem, &retained_bytes},
		  ann_graph_allocator{mem, &retained_bytes}),
	    entries(nullptr), lookup(nullptr), reclaim_queue(nullptr),
	    reclaim_count(0), lookup_capacity(0),
	    capacity(0), pending(nullptr) {}
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
	float *vector;
	/** Graph slot affected by this change. */
	uint32_t slot;
	/** Whether this change altered the current view. */
	bool applied;
	/** Whether allocation-free undo consumed this change. */
	bool rolled_back;
	/** Reverse-link snapshots retained until finish. */
	ann_graph::add_undo_t undo;
};

static size_t
ann_usearch_vector_size(const struct ann_backend *backend)
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

/** Find one occupied hash slot, preserving deleted probe chains. */
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

/** Insert into a table whose live load is at most one half. */
static void
ann_usearch_lookup_insert(struct ann_usearch_lookup_entry *lookup,
			  size_t capacity, uint64_t label, uint32_t slot)
{
	assert(capacity != 0);
	size_t offset = ann_usearch_hash(label) & (capacity - 1);
	size_t deleted = SIZE_MAX;
	for (size_t i = 0; i < capacity; ++i) {
		size_t at = (offset + i) & (capacity - 1);
		if (lookup[at].state == 2 && deleted == SIZE_MAX)
			deleted = at;
		if (lookup[at].state != 0)
			continue;
		at = deleted == SIZE_MAX ? at : deleted;
		lookup[at].label = label;
		lookup[at].slot = slot;
		lookup[at].state = 1;
		return;
	}
	assert(deleted != SIZE_MAX);
	lookup[deleted].label = label;
	lookup[deleted].slot = slot;
	lookup[deleted].state = 1;
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

/** Forget a label while the graph retains its navigation vector. */
static void
ann_usearch_lookup_erase(struct ann_backend *backend, uint64_t label)
{
	size_t at = ann_usearch_lookup_find(backend->lookup,
					    backend->lookup_capacity, label);
	assert(at != SIZE_MAX);
	backend->lookup[at].state = 2;
}

/** Distance bridge resolving USearch members into float32 vectors. */
struct ann_usearch_metric {
	/** Graph and slot lookup owner. */
	const struct ann_backend *backend;
	/** Optional operation budget. */
	struct ann_search_control *control;
	/** First distance or stop failure. */
	enum ann_status *status;

	const float *vector(const float *value) const noexcept { return value; }
	const float *vector(float *value) const noexcept { return value; }
	template <class K>
	const float *vector(unum::usearch::member_cref_gt<K> value) const noexcept
	{
		return backend->entries[value.slot].vector;
	}
	template <class K>
	const float *vector(unum::usearch::member_ref_gt<K> value) const noexcept
	{
		return backend->entries[value.slot].vector;
	}
	template <class T>
	const float *vector(const T &value) const noexcept
	{
		return vector(*value);
	}
	template <class A, class B>
	double operator()(const A &left, const B &right) const noexcept
	{
		if (*status != ANN_OK)
			return DBL_MAX;
		double distance = 0;
		*status = ann_distance(vector(left), vector(right),
					backend->config.dimension,
					backend->config.metric, control,
					&distance);
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
	struct ann_usearch_config options = {16, 200, 64};
	if (config->algorithm != nullptr)
		options = *(const struct ann_usearch_config *)config->algorithm;
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
	*out = backend;
	return ANN_OK;
}

static void
ann_usearch_destroy(struct ann_backend *backend)
{
	if (backend == nullptr)
		return;
	for (struct ann_change *change = backend->pending; change != nullptr;) {
		struct ann_change *previous = change->previous;
		ann_usearch_free(backend, change->vector,
				 ann_usearch_vector_size(backend));
		change->undo.commit();
		change->~ann_change();
		ann_usearch_free(backend, change, sizeof(*change));
		change = previous;
	}
	for (uint32_t i = 0; i < backend->graph.size(); ++i)
		ann_usearch_free(backend, backend->entries[i].vector,
				 ann_usearch_vector_size(backend));
	ann_usearch_free(backend, backend->entries,
			 (size_t)backend->capacity * sizeof(*backend->entries));
	ann_usearch_free(backend, backend->lookup,
		 backend->lookup_capacity * sizeof(*backend->lookup));
	ann_usearch_free(backend, backend->reclaim_queue,
		 (size_t)backend->capacity * sizeof(*backend->reclaim_queue));
	struct ann_memory memory = backend->memory;
	backend->~ann_backend();
	memory.free(memory.ctx, backend, sizeof(*backend));
}

static enum ann_status
ann_usearch_reserve(struct ann_backend *backend, uint32_t capacity)
{
	if (backend == nullptr)
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
ann_usearch_prepare(struct ann_backend *backend, enum ann_change_kind kind,
		    uint64_t label, const float *vector, uint32_t dimension,
		    struct ann_change **out)
{
	if (out == nullptr)
		return ANN_INVALID;
	*out = nullptr;
	if (backend == nullptr || (kind != ANN_INSERT && kind != ANN_RETIRE))
		return ANN_INVALID;
	if (backend->pending != nullptr && !backend->pending->applied)
		return ANN_BUSY;
	int64_t slot = ann_usearch_find(backend, label);
	if (kind == ANN_INSERT) {
		if (slot >= 0)
			return ANN_EXISTS;
		if (dimension != backend->config.dimension ||
		    ann_vector_validate_f32(vector, dimension,
					   backend->config.metric) != ANN_OK)
			return ANN_INVALID;
		if (backend->graph.size() == UINT32_MAX)
			return ANN_INVALID;
		if (backend->graph.size() == backend->capacity) {
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
	change->slot = kind == ANN_RETIRE ? slot : backend->graph.size();
	if (kind == ANN_INSERT) {
		change->vector = (float *)ann_usearch_alloc(
			backend, ann_usearch_vector_size(backend));
		if (change->vector == nullptr) {
			change->~ann_change();
			ann_usearch_free(backend, change, sizeof(*change));
			return ANN_OUT_OF_MEMORY;
		}
		memcpy(change->vector, vector, ann_usearch_vector_size(backend));
	}
	backend->pending = change;
	*out = change;
	return ANN_OK;
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
	struct ann_usearch_entry *entry = &backend->entries[change->slot];
	entry->vector = change->vector;
	entry->label = change->label;
	entry->live = true;
	entry->reclaimed = false;
	enum ann_status status = ANN_OK;
	ann_usearch_metric metric{backend, control, &status};
	struct ann_usearch_stop stop{control, &status};
	unum::usearch::index_update_config_t config;
	config.expansion = backend->options.expansion_add;
	if (control != nullptr) {
		config.should_stop = ann_usearch_should_stop;
		config.stop_context = &stop;
	}
	auto result = backend->graph.add(change->label, change->vector, metric,
		config, [](auto) {}, [](auto, auto) {}, &change->undo);
	if (result && status == ANN_OK && control != nullptr)
		status = ann_search_control_poll(control);
	if (!result || status != ANN_OK) {
		result.error.release();
		change->undo.rollback(backend->graph);
		memset(entry, 0, sizeof(*entry));
		return status != ANN_OK ? status : ANN_OUT_OF_MEMORY;
	}
	change->vector = nullptr;
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
	if (!change->applied)
		return;
	if (change->kind == ANN_RETIRE) {
		backend->entries[change->slot].live = true;
	} else {
		ann_usearch_lookup_erase(backend, change->label);
		change->undo.rollback(backend->graph);
		struct ann_usearch_entry *entry = &backend->entries[change->slot];
		ann_usearch_free(backend, entry->vector,
				 ann_usearch_vector_size(backend));
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
	ann_usearch_free(backend, change->vector,
			 ann_usearch_vector_size(backend));
	change->~ann_change();
	ann_usearch_free(backend, change, sizeof(*change));
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
	/* The graph still navigates through this slot until generation rebuild. */
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
	    (opts->filter != nullptr && opts->filter->accept == nullptr))
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
		expansion = ((const struct ann_usearch_search_opts *)
			     opts->algorithm)->expansion;
		if (expansion == 0)
			return ANN_INVALID;
	}
	ann_usearch_metric metric{backend, opts->control, &status};
	struct ann_usearch_stop stop{opts->control, &status};
	unum::usearch::index_search_config_t config;
	config.expansion = expansion;
	config.should_stop = ann_usearch_should_stop;
	config.stop_context = &stop;
	auto predicate = [backend, opts](ann_graph::member_cref_t member) {
		const struct ann_usearch_entry *entry =
			&backend->entries[member.slot];
		if (entry->reclaimed)
			return false;
		return opts->filter == nullptr ? entry->live :
			opts->filter->accept(entry->label, opts->filter->ctx);
	};
	auto result = backend->graph.search(query, opts->candidate_limit,
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
		out[i].label = backend->entries[match.member.slot].label;
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
	for (uint32_t i = 0; i < backend->graph.size(); ++i) {
		if (backend->entries[i].live)
			++out->live;
		else
			++out->retired;
	}
}

extern "C" const struct ann_backend_ops ann_usearch_ops = {
	"usearch", ann_usearch_create, ann_usearch_destroy,
	ann_usearch_reserve, ann_usearch_prepare, ann_usearch_apply,
	ann_usearch_rollback, ann_usearch_finish, ann_usearch_reclaim,
	ann_usearch_search, ann_usearch_stat,
};
