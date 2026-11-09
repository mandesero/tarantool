# VECTOR index implementation report

Date: 2026-11-09. Base: `cee48f4b03c96a1f200644a455952b3d65d06fbb`.
Native code: `080b5e174d08c2d5bc5d7c959d43e78fcf09f2c3`.
The normative input is the supplied November 9 Confluence export, SHA-256
`b96306dcdd4fcb3688c9fb8bf9982fa3f2cbc6180b9220a632785ff238c1be54`.
The minimally revised Word artifact is `vector-index-rfc-lifecycle-red.docx`,
SHA-256
`77809021279c328f2599668c8dc4548ea7bf802a42e74b9e414496157b58652a`.
It changes 24 paragraphs and removes one duplicate C field. All other text,
seven tables and three original formula images are preserved. Insertions and
replacements are red; all 20 rendered pages and internal links were checked.
Both Word files are external to this repository. The implemented contract is
recorded in [contract.md](contract.md), with examples in [README.md](README.md).

## Delivered code

The memtx VECTOR index stores dense f32 vectors, computes distances in
f64, and leaves tuple values unchanged. A storage-neutral C ANN API has
an exact Flat backend and a USearch HNSW backend. The adapter charges
retained allocations to the memtx quota, supports cancellation and
work limits, and can undo an insertion without allocating. The USearch
submodule points to `mandesero/USearch` commit
`0355958c08b22c642fc38ece00cd30646243b141`. Its header contains the
allocator-owner, graph-undo, and stop hooks directly.

Memtx updates retain stable labels for transaction undo and MVCC read
views. Garbage collection releases labels after old readers finish;
navigation nodes remain until a generation rebuild. DDL validates
VECTOR-specific options. Ordinary local and IProto NEIGHBOR SELECT
return tuples with an explicit limit. Rich local `search(query, opts)`
takes a flat query and returns parallel tuple/distance arrays when
requested. Timeout, filter and HNSW request options belong to search.
Both APIs share the native statement, ACL, MVCC and ANN path. Dedicated
VECTOR diagnostics distinguish invalid input, unsupported operations,
timeouts, and work limits. `index:stat()` reports configuration and
memory; `index:rebuild()` switches generations only after success.

The distributed implementation is maintained in the companion
[CRUD patchset](https://github.com/mandesero/crud/tree/codex/vector-search).
It exposes `crud.vector_search` through ordinary CRUD roles. The generic
protected-call support remains in vshard commit
`674cda708d957cc145aa4f0622804bb5c2f8007a`.

The November 2 update separated SELECT and rich search (`78e502126f`) and
made build/rebuild cooperative (`43cff08243`). The November 9 lifecycle update
adds reserved maintenance quota (`ab77ec3dd8`), shared coordinates
(`22ae9dde42`) and concurrent compaction in the native code commit above.
The current change needs no CRUD, vshard or USearch submodule modification.

Tuple versions with the same primary key and canonical coordinates share
one immutable embedding and graph node. Visibility and filters still select
the exact tuple version. GC recycles bindings and labels independently of
navigation slots. A failed later secondary index releases the failed tuple's
VECTOR binding. Visibility-list scans consume the query's work/time budget.

Rebuild migrates a finite initial range of labels while new embeddings enter
both graphs atomically. DML, rollback and tuple GC preserve its progress;
replacement or DROP invalidates the index. Automatic maintenance compacts
navigation tombstones. Both generations share coordinates. Reserved quota
and bounded concurrent admission protect the builder from write overload;
excess growth returns OutOfMemory. After publication, writes use the new graph
even while the old one is reclaimed, and cancellation cannot revert it.

The yielding rebuild now enters Lua C: a warmed JIT caller could crash
when the old FFI entry yielded. Rich search also uses Lua C. A backend
initialization failure now releases the duplicated index definition, and
malformed vectors use VECTOR diagnostics instead of an R-tree error.

## Reproduce the checks

On Ubuntu 24.04 with GCC 13.3, from a checkout of this branch:

```sh
git submodule update --init --recursive
cmake -S . -B build -DCMAKE_BUILD_TYPE=Debug
cmake --build build --target tarantool ann.test ann_memory.test \
  ann_tuple_map.test ann_usearch.test -j 8
ctest --test-dir build -R '^test/unit/ann.*\.test$' --output-on-failure
python3 test/test-run.py --builddir "$PWD/build" \
  --executable "$PWD/build/src/tarantool" \
  --vardir /tmp/vector-index-tests \
  box-luatest/memtx_vector_index_test.lua \
  box-luatest/memtx_vector_atomic_test.lua \
  box-luatest/memtx_vector_mvcc_test.lua \
  box-luatest/memtx_vector_ddl_test.lua \
  box-luatest/memtx_vector_select_test.lua \
  box-luatest/memtx_vector_protocol_test.lua \
  box-luatest/memtx_vector_lifecycle_test.lua \
  box-luatest/memtx_vector_sync_test.lua \
  replication-luatest/vector_index_test.lua \
  box-luatest/box_read_ffi_disable_test.lua
```

The CI workflow `.github/workflows/vector_index.yml` runs only native
VECTOR checks and uses the pinned USearch submodule. The companion CRUD
workflow builds this native code commit and runs its distributed checks.

The final code built on Ubuntu 24.04 x86_64 with GCC 13.3 in Debug mode.
All 13 CTest entries passed: four ANN unit executables, eight native VECTOR
Lua suites and the replication suite. They cover shared embeddings, payload
visibility, retained readers, filters, uncommitted versions, work limits,
allocation-failure atomicity in both graphs, automatic compaction, bounded
admission, cooperative build and rebuild, and writes/cancellation after
publication. The existing WAL, DDL, recovery and protocol cases also passed.

A separate Clang 19 Debug build with AddressSanitizer, leak detection and
UndefinedBehaviorSanitizer passed all six selected entries: three ANN units
and the atomicity, lifecycle and MVCC Lua suites. LuaCheck 0.26.0 reported
zero warnings and errors in both changed Lua files. Checkpatch validates the
new commits. Full hosted CI and the complete Tarantool matrix were not run.

The host reuses its task build tree. Its binary reports the older
`8bd63fc90` git-describe label, which is not the source revision of these
results. The native code commit and measurement context above identify the
source. The previous CRUD integration runs belong to the November 2 revision;
this update preserves the search API and wire format but does not claim a new
CRUD cluster run.

## November 9 lifecycle probe

[The resource/progress probe](bench-results/lifecycle-20261109.json) uses
1000 vectors of dimension 16. After 10000 payload-only updates, dense
coordinates still occupy 64000 bytes and graph capacity remains 1024.
A rebuild completes in 1.437 s with 250 successful concurrent vector writes,
32 admission rejections and 532 heartbeat iterations. Its new generation has
1000 bound embeddings, 250 navigation tombstones and no transient owner.
This is one synthetic Debug observation, not a throughput or latency promise.
Rejections are intentional bounded admission, not successful partial writes.

The automated lifecycle suite also runs repeated updates and automatic
compactions, checks bounded graph/vector growth, and verifies both generations
after injected allocation failures. The maintenance trigger uses constant-time
backend counters, avoiding a full graph scan on the write path.

## November 2 control measurement

[The new result](bench-results/rfc-20261102-native.json) uses 1000 vectors,
dimension 16, 100 queries, k=10, ten buckets and seed 20260930. Every fourth
query filters one bucket. Recall@10 is 1.00, ANN p99 is 10.11 ms, maximum
heartbeat gap is 10.40 ms, build takes 1.68 s and cooperative rebuild 1.65 s.
The current generation accounts for 415253 bytes after rebuild, with zero
transient bytes. This is a small synthetic Debug run; it does not establish
production throughput or a performance change relative to earlier runs.

## Historical measurements

The earlier accepted measurements used Ubuntu 24.04, eight virtual Icelake
CPUs, GCC 13.3, Debug Tarantool, seed `20260930`, f32 L2 input, and
an exact Flat oracle. The [thresholds](bench-thresholds.md) were fixed
before the final reruns. Accepted machine-readable results are in
[bench-results](bench-results/). The scripts in `perf/` generate every
dataset. Every fourth query applies a one-bucket filter.

| Profile | Flat p99 ms | USearch p99 ms | Recall@10 |
| --- | ---: | ---: | ---: |
| 1000 x 2, 1 bucket | 0.081 | 0.243 | 1.00 |
| 1000 x 16, 10 buckets | 0.332 | 1.736 | 1.00 |
| 1000 x 16, 100 buckets | 0.302 | 1.692 | 1.00 |
| 5000 x 16, 10 buckets | 1.462 | 4.484 | 1.00 |
| 5000 x 16, 200 buckets | 1.397 | 10.158 | 1.00 |

These are C ABI calls outside Tarantool. Flat has a lower p99 on each
small profile; at 5000 x 16 with ten buckets, USearch has a lower
median (0.863 versus 1.291 ms) but a higher tail and memory use. The
numbers do not establish a general algorithm ranking.

The memtx p99 was 5.00 ms at 1000 x 2, 9.65 ms at 1000 x 16 with a
10% filter, and 16.49 ms at 5000 x 16 with a 10% filter. All five
accepted memtx profiles reached Recall@10 of 1. The 200-bucket
profile has five records per selected bucket; 25% short answers are
expected with k=10 because one in four queries is bucket-filtered.
At 1000 x 16 with a 10% filter, throughput was 364 searches/s and
`index:stat().memory.total` after build was 415253 bytes. Snapshot
recovery took 0.971 s at 1000 x 16 and 7.030 s at 5000 x 16; those
times include fixed `box.cfg()` startup work.

The measurements above were collected before the CRUD split on unchanged
native code. They are historical results, not a new performance rerun.
Distributed measurements now reside in CRUD. During local ANN work, the
worst measured 1 ms heartbeat gap was 12.55 ms at 1000 x 16 and 37.04 ms
at 5000 x 16. Scheduling delay needs workload-specific evaluation.

## Earlier RFC revision 4 alignment

The two additional code commits expose the numeric contract, production
request limits and cumulative C-search duration, and rename the inaccurate
`slots.reusable` counter to `slots.pending_rebuild`. The latter counts nodes
whose tuple bindings have already been released; their navigation data and
dense vectors remain until an explicit generation rebuild.

Ordinary memtx now releases obsolete VECTOR tuple bindings on commit and
discarded bindings on rollback. MVCC retains its story-GC lifecycle. This
fix allows rebuild to omit retired nodes in both modes without discarding
versions needed by pending transactions or readers.

Before the November update, the corrected code built on the same host.
All 67 tests in the eight
native Lua suite files and four ANN CTest cases passed. Search duration
includes admitted failed and zero-limit operations; Lua validation and
post-call conversion are outside the measured interval. Performance
measurements were not repeated for these diagnostic and lifecycle fixes.

Historical JSON measurements retain the old `slots.reusable` field name;
their interpretation is pending rebuild, not insert-time slot reuse.

## Verification limits

Hosted GitHub Actions and the full Tarantool matrix have not run for this
update. The sanitizer results above cover the selected native units and Lua
integration suites, not every subsystem. The complete historical benchmark
matrix was not repeated and no external VK Tech dataset was available.
Maintenance constants are conservative internal policy, not measured optimal
values. Completion needs allocator success and builder scheduling; long MVCC
transactions can retain old versions. Quota exhaustion rejects new allocations.
Yield checks target 1 ms portions but do not bound a single HNSW operation.
