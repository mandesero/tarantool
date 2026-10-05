/*
 * SPDX-License-Identifier: BSD-2-Clause
 *
 * Copyright 2026, Tarantool AUTHORS, please see AUTHORS file.
 */
#include "lib/ann/ann_backend.h"
#include "lib/ann/ann_usearch.h"

#include <math.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

/** Count backend-owned allocations. */
struct owner {
	/** Retained bytes. */
	size_t bytes;
	/** Highest retained byte count. */
	size_t peak;
};

/** Preserve the charged size and payload alignment. */
struct block {
	/** Bytes charged for this allocation. */
	size_t size;
	/** Align the following payload. */
	max_align_t alignment;
};

static void *
allocate(size_t size)
{
	void *ptr = NULL;
	if (posix_memalign(&ptr, alignof(max_align_t), size) != 0)
		return NULL;
	return ptr;
}

static void *
alloc(void *ctx, size_t size)
{
	struct block *block =
		(struct block *)allocate(sizeof(*block) + size);
	if (block == NULL)
		return NULL;
	block->size = size;
	struct owner *owner = (struct owner *)ctx;
	owner->bytes += size;
	if (owner->bytes > owner->peak)
		owner->peak = owner->bytes;
	return block + 1;
}

static void
release(void *ctx, void *ptr, size_t size)
{
	if (ptr == NULL)
		return;
	struct block *block = (struct block *)ptr - 1;
	if (block->size != size)
		abort();
	((struct owner *)ctx)->bytes -= size;
	free(block);
}

static uint32_t state = 20260930;

static uint32_t
next_random(void)
{
	state ^= state << 13;
	state ^= state >> 17;
	state ^= state << 5;
	return state;
}

static float
random_float(void)
{
	return (float)((next_random() & 0x7fffffff) / 1073741824.0 - 1);
}

static double
now(void)
{
	struct timespec ts;
	clock_gettime(CLOCK_MONOTONIC, &ts);
	return ts.tv_sec + ts.tv_nsec / 1e9;
}

static int
compare_double(const void *a, const void *b)
{
	double x = *(const double *)a;
	double y = *(const double *)b;
	return (x > y) - (x < y);
}

/** Deterministic unsigned bucket predicate. */
struct filter_context {
	/** Number of buckets in this profile. */
	uint32_t bucket_count;
	/** Selected bucket. */
	uint32_t bucket;
};

static uint64_t
checksum(const float *values, size_t count)
{
	const unsigned char *bytes = (const unsigned char *)values;
	uint64_t hash = UINT64_C(14695981039346656037);
	for (size_t i = 0; i < count * sizeof(*values); ++i) {
		hash ^= bytes[i];
		hash *= UINT64_C(1099511628211);
	}
	return hash;
}

static bool
accept(uint64_t label, void *ctx)
{
	struct filter_context *filter = (struct filter_context *)ctx;
	return label % filter->bucket_count == filter->bucket;
}

static void
check(enum ann_status status)
{
	if (status != ANN_OK) {
		fprintf(stderr, "backend status %d\n", (int)status);
		exit(1);
	}
}

static void
load(const struct ann_backend_ops *ops, struct ann_backend *backend,
     const float *vectors, uint32_t count, uint32_t dimension)
{
	check(ops->reserve(backend, count));
	for (uint32_t i = 0; i < count; ++i) {
		struct ann_change *change = NULL;
		check(ops->prepare(backend, ANN_INSERT, i + 1,
				   vectors + i * dimension, dimension,
				   &change));
		check(ops->apply(change, NULL));
		ops->finish(change);
	}
}

static uint32_t
search(const struct ann_backend_ops *ops, struct ann_backend *backend,
       const float *query, uint32_t dimension, uint32_t limit,
       struct filter_context *context, struct ann_candidate *out)
{
	struct ann_search_control control = {};
	control.work_limit = UINT64_MAX;
	struct ann_filter filter = {accept, context};
	struct ann_usearch_search_opts algorithm = {128};
	struct ann_search_opts opts = {};
	opts.candidate_limit = limit;
	opts.query_dimension = dimension;
	opts.control = &control;
	if (context != NULL)
		opts.filter = &filter;
	if (ops == &ann_usearch_ops)
		opts.algorithm = &algorithm;
	uint32_t result = 0;
	check(ops->search(backend, query, &opts, out, &result));
	return result;
}

int
main(int argc, char **argv)
{
	if (argc != 5) {
		fprintf(stderr, "usage: %s N d queries buckets\n", argv[0]);
		return 2;
	}
	uint32_t count = (uint32_t)strtoul(argv[1], NULL, 10);
	uint32_t dimension = (uint32_t)strtoul(argv[2], NULL, 10);
	uint32_t query_count = (uint32_t)strtoul(argv[3], NULL, 10);
	uint32_t buckets = (uint32_t)strtoul(argv[4], NULL, 10);
	const uint32_t limit = 10;
	if (count > 100000 || dimension == 0 || dimension > 4096 ||
	    query_count == 0 || query_count > 10000 ||
	    buckets == 0 || buckets > 1000 || count < limit * buckets)
		return 2;
	float *vectors =
		(float *)allocate(sizeof(float) * count * dimension);
	float *queries =
		(float *)allocate(sizeof(float) * query_count * dimension);
	double *times = (double *)allocate(sizeof(double) * query_count);
	if (vectors == NULL || queries == NULL || times == NULL)
		return 1;
	for (uint32_t i = 0; i < count * dimension; ++i)
		vectors[i] = random_float();
	for (uint32_t i = 0; i < query_count * dimension; ++i)
		queries[i] = random_float();
	uint64_t data_hash = checksum(vectors, (size_t)count * dimension);
	uint64_t query_hash = checksum(queries,
				       (size_t)query_count * dimension);
	struct ann_candidate truth[10];
	struct ann_candidate answer[10];
	struct ann_usearch_config settings = {16, 128, 128};
	struct ann_config config = {dimension, ANN_L2, NULL};
	struct owner flat_owner = {};
	struct owner graph_owner = {};
	struct ann_memory flat_memory = {&flat_owner, alloc, release};
	struct ann_memory graph_memory = {&graph_owner, alloc, release};
	struct ann_backend *flat = NULL;
	struct ann_backend *graph = NULL;
	const struct ann_backend_ops *flat_ops = ann_backend_find("flat");
	check(flat_ops->create(&config, &flat_memory, &flat));
	double start = now();
	load(flat_ops, flat, vectors, count, dimension);
	double flat_build = now() - start;
	config.algorithm = &settings;
	check(ann_usearch_ops.create(&config, &graph_memory, &graph));
	start = now();
	load(&ann_usearch_ops, graph, vectors, count, dimension);
	double graph_build = now() - start;
	uint64_t hits = 0;
	uint64_t possible = 0;
	uint32_t short_count = 0;
	for (int backend = 0; backend < 2; ++backend) {
		const struct ann_backend_ops *ops =
			backend == 0 ? flat_ops : &ann_usearch_ops;
		struct ann_backend *index = backend == 0 ? flat : graph;
		double total = 0;
		for (uint32_t i = 0; i < query_count; ++i) {
			struct filter_context context = {buckets, i % buckets};
			struct filter_context *filter =
				i % 4 == 0 ? &context : NULL;
			const float *query = queries + i * dimension;
			uint32_t expected =
				search(flat_ops, flat, query, dimension,
				       limit, filter, truth);
			start = now();
			uint32_t actual =
				search(ops, index, query, dimension,
				       limit, filter, answer);
			times[i] = now() - start;
			total += times[i];
			if (backend == 1) {
				possible += expected;
				short_count += actual < expected;
				for (uint32_t j = 0; j < actual; ++j) {
					for (uint32_t l = 0; l < expected; ++l)
						hits += answer[j].label ==
							truth[l].label;
				}
			}
		}
		qsort(times, query_count, sizeof(*times), compare_double);
		struct ann_backend_stats stats;
		ops->stat(index, &stats);
		printf("{\"backend\":\"%s\",\"count\":%u,"
		       "\"dimension\":%u,\"queries\":%u,"
		       "\"buckets\":%u,\"seed\":20260930,"
		       "\"data_fnv64\":\"%016llx\","
		       "\"query_fnv64\":\"%016llx\","
		       "\"build_seconds\":%.6f,\"p50_ms\":%.4f,"
		       "\"p95_ms\":%.4f,\"p99_ms\":%.4f,"
		       "\"throughput_per_s\":%.2f,"
		       "\"resident_bytes\":%llu,\"peak_bytes\":%zu,"
		       "\"recall_at_10\":%.6f,\"short_fraction\":%.6f}\n",
		       ops->name, count, dimension, query_count, buckets,
		       (unsigned long long)data_hash,
		       (unsigned long long)query_hash,
		       backend == 0 ? flat_build : graph_build,
		       times[(query_count - 1) * 50 / 100] * 1000,
		       times[(query_count - 1) * 95 / 100] * 1000,
		       times[(query_count - 1) * 99 / 100] * 1000,
		       query_count / total,
		       (unsigned long long)stats.resident_bytes,
		       backend == 0 ? flat_owner.peak : graph_owner.peak,
		       backend == 0 ? 1.0 : (double)hits / possible,
		       backend == 0 ? 0.0 :
		       (double)short_count / query_count);
	}
	flat_ops->destroy(flat);
	ann_usearch_ops.destroy(graph);
	if (flat_owner.bytes != 0 || graph_owner.bytes != 0)
		return 1;
	free(vectors);
	free(queries);
	free(times);
	return 0;
}
