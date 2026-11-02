# VECTOR index implementation report

Date: 2026-11-02. Base: `cee48f4b03c96a1f200644a455952b3d65d06fbb`.
Native code: `43cff08243335275bb1cea5712a9964c0a96d25a`.
The normative input is `vector-index-rfc-revised-red.docx`, dated
2026-11-02, SHA-256
`bda65fdb4477b70013bf9a278a8d266f92c1042f02aea9ebe39ac8df301f034c`.
The supplied DOCX is external to this repository. The resulting API
contract is recorded in [contract.md](contract.md), and installation
examples are in [README.md](README.md).

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

The November update adds two code commits after `48fc827807`:
`78e502126f` separates SELECT and rich search, and `43cff08243` makes
build/rebuild cooperative. Each code commit was built and its applicable
VECTOR tests passed on the selected Ubuntu host. CRUD consumes the new
search arrays in its companion branch; vshard and USearch need no changes.

Rebuild pins the index and replays into a private generation, yielding
between portions bounded by elapsed time. DML attempts, rollback, tuple
GC or index replacement invalidate replay; `MODIFY_INDEX` asks the caller
to retry. Publication is atomic. Before publication, cancellation or OOM
preserves the current generation; afterward private cleanup completes even
if the requesting fiber is cancelled. Both generation owners are stable
for the index lifetime and charge the same memtx quota. Statistics separate
current and transient bytes. Nonempty HASH-primary builds are rejected.

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

The final code built on Ubuntu 24.04 x86_64 with GCC 13.3 in Debug mode:

- Four ANN CTest executables passed.
- All nine VECTOR Lua suite files and the standard FFI-disable suite passed.
- Protocol tests cover both local read paths, schema-aware and schema-free
  net.box SELECT, async requests, malformed queries and exact return arity.
- Lifecycle tests cover allocation failures, short slices, concurrent DML,
  rollback, MVCC GC, truncate/drop, cancellation and simultaneous rebuilds.
  Build and rebuild of 4000 vectors complete with a 100 ms fiber slice while
  a heartbeat runs, without an artificial yield injection.
- Both native patch commits passed checkpatch with zero errors.
- LuaCheck 0.23.0 passed all 16 affected native Lua files with no diagnostics.
- CRUD's 12 focused tests and full three-replicaset fixture passed with the
  final binary. Distributed measurements are recorded in CRUD.

The host reused its configured build tree after task-source synchronization.
Its binary still reports the older `8bd63fc90` git-describe label; that label
is not the source revision of these results. The source commit is recorded
above and in the new measurement artifact. No hosted CI run is claimed.

## November control measurement

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
update. The project-pinned LuaCheck 0.26.1 was unavailable; the installed
0.23.0 check is recorded separately above. No new sanitizer run is claimed.
Earlier full ASan/UBSan builds stopped in bundled LuaJIT before VECTOR;
earlier focused ANN units passed sanitizers. That does not establish
sanitizer cleanliness of the Lua integration. The complete historical
benchmark matrix was not repeated, and no external VK Tech dataset was
available. Current measurements use deterministic synthetic inputs.
