# VECTOR index implementation report

Date: 2026-10-05. Base: `cee48f4b03c96a1f200644a455952b3d65d06fbb`.
The implementation follows `vector-index-rfc-v3.docx`, revision 3
(SHA-256 `e7599a312ffb3dd83b9f90438c575e263d8d1b06a62334ce3b7fc3a7876fe5f3`).
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
VECTOR-specific options. NEIGHBOR selection has an explicit limit,
filter, distance output, timeout, and work and memory bounds. Dedicated
VECTOR diagnostics distinguish invalid input, unsupported operations,
timeouts, and work limits. `index:stat()` reports configuration and
memory; `index:rebuild()` switches generations only after success.

The distributed implementation is maintained in the companion
[CRUD patchset](https://github.com/mandesero/crud/tree/codex/vector-search).
It exposes `crud.vector_search` through ordinary CRUD roles. The generic
protected-call support remains in vshard commit
`674cda708d957cc145aa4f0622804bb5c2f8007a`.

This reconstructed series keeps the first nine native commits unchanged,
then adds the ICU metadata helper, snapshot recovery fix, native benchmarks,
and native CI. Its final commit contains this report and API guides.
Embedded Lua router/storage/wire modules, their registration and changelog,
distributed tests, benchmark and result artifact are removed from Tarantool.
The three-replicaset fixture, distributed benchmark, CI and report are in
CRUD. Native source behavior is unchanged by this split.

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
  box-luatest/memtx_vector_lifecycle_test.lua \
  box-luatest/memtx_vector_sync_test.lua \
  replication-luatest/vector_index_test.lua
```

The CI workflow `.github/workflows/vector_index.yml` runs only native
VECTOR checks and uses the pinned USearch submodule. The companion CRUD
workflow builds this native code commit and runs its distributed checks.

The final native checkout `fae93a25e8` built on Ubuntu 24.04 x86_64 with
GCC 13.3 in Debug mode. Four ANN CTest cases and all eight Lua suite files
listed above passed, including synchronous writes and replication. CRUD's
12 focused cases and its full three-replicaset fixture also passed using
this cleaned binary. The tested tree is identical to this code commit;
only commit messages were normalized afterward. The build no longer
embeds `vector_search.*`.
Native performance measurements were not repeated for this split.
The previous reconstruction also compiled each core commit with macOS
Clang 17 and passed applicable ANN unit tests. That per-commit build was
not repeated for this split: the first nine commits are unchanged.

## Measurements

The accepted measurements used Ubuntu 24.04, eight virtual Icelake
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

## RFC revision 4 alignment

The two additional code commits expose the numeric contract, production
request limits and cumulative C-search duration, and rename the inaccurate
`slots.reusable` counter to `slots.pending_rebuild`. The latter counts nodes
whose tuple bindings have already been released; their navigation data and
dense vectors remain until an explicit generation rebuild.

Ordinary memtx now releases obsolete VECTOR tuple bindings on commit and
discarded bindings on rollback. MVCC retains its story-GC lifecycle. This
fix allows rebuild to omit retired nodes in both modes without discarding
versions needed by pending transactions or readers.

The corrected code built on the same Ubuntu host. All 67 tests in the eight
native Lua suite files and four ANN CTest cases passed. Search duration
includes admitted failed and zero-limit operations; Lua validation and
post-call conversion are outside the measured interval. Performance
measurements were not repeated for these diagnostic and lifecycle fixes.

Historical JSON measurements retain the old `slots.reusable` field name;
their interpretation is pending rebuild, not insert-time slot reuse.

## Verification limits

The hosted GitHub Actions workflow has not run. A full Tarantool
ASan/UBSan build stops in bundled LuaJIT `dynasm_x86.h` before VECTOR
is compiled; focused ANN unit builds passed both sanitizers in the
implementation checks. This does not establish sanitizer cleanliness
of the Lua integration. The project-pinned luacheck 0.26.1 has not
run. The four native benchmark Lua files passed a Tarantool `loadfile()`
syntax check; the original native benchmark acceptance did not run
LuaCheck. No external VK Tech dataset was available,
so the accepted data is synthetic with fixed seeds and checksums.
