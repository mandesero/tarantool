# VECTOR index implementation report

Date: 2026-10-01. Base: `cee48f4b03c96a1f200644a455952b3d65d06fbb`.
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

The `vector_search` Lua package exposes a protected storage procedure
and a router that validates wire metadata and merges shard candidates.
It rejects incomplete coverage and malformed responses. The companion
vshard fork is pinned at
`674cda708d957cc145aa4f0622804bb5c2f8007a`; its call-context API
protects bucket scope and passes the remaining timeout after refs are
acquired. The router uses local MVCC visibility at each storage. It
does not offer one cluster-wide snapshot.

The patch series separates backend, atomic DML, MVCC, DDL, selection,
diagnostics, rebuild, storage, router, recovery, cluster tests, and
benchmarks. Its last commit contains this report and the API guides.
Intermediate feasibility probes and stage logs are omitted from the
new series; production code and functional tests retain their final
contents. The vshard cluster fixture lives in `test/vshard-vector/`.

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
  box-luatest/vector_storage_test.lua \
  box-luatest/vector_router_test.lua
```

The CI workflow in `.github/workflows/vector_index.yml` runs these
checks and copies `test/vshard-vector/` into the pinned vshard test
runner. The cluster fixture covers global, one-bucket, bucket-set, and
empty-set scopes; migration, concurrent DML, timeouts, remote errors,
schema mismatch, response overflow, master promotion, snapshot, and
restart. Three fixed queries use a global exact f32 oracle and require
Recall@5 of at least 0.8. This is a deterministic contract test, not
a sustained-load test.

The local macOS Clang 17 Debug build passed the Tarantool target and
all applicable ANN CTest cases at each reconstructed core-code commit. On
the selected Ubuntu host, the reconstructed checkout passed the GCC
13.3 Debug build, four ANN unit tests, all eight Luatest files listed
above, the VECTOR synchronous-write and replication Luatests, and the
one vshard cluster fixture. The C ABI benchmark compiled and completed
a 1000 x 2 smoke profile for both backends. All five Lua benchmark
files passed `loadfile()` syntax checks against that build.

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

The three-replicaset run reached global p99 of 21.33 ms on uniform
data with L=10, 29.83 ms on skewed data with L=10, and 11.14 ms on
skewed data with L=20. One-bucket and three-bucket-set p99 were 2.46
and 13.18 ms. These runs use fixed sequences and 30 requests per
profile; they do not model production concurrency. During local ANN
work, the worst measured 1 ms heartbeat gap reached 12.55 ms at
1000 x 16 and 37.04 ms at 5000 x 16. Cooperative scheduling delay
deserves workload-specific evaluation.

## Verification limits

The hosted GitHub Actions workflow has not run. A full Tarantool
ASan/UBSan build stops in bundled LuaJIT `dynasm_x86.h` before VECTOR
is compiled; focused ANN unit builds passed both sanitizers in the
implementation checks. This does not establish sanitizer cleanliness
of the Lua integration. The project-pinned luacheck 0.26.1 has not
run. The five benchmark Lua files passed a Tarantool `loadfile()`
syntax check; luacheck was unavailable on the selected Ubuntu host
for the final reassembly. No external VK Tech dataset was available,
so the accepted data is synthetic with fixed seeds and checksums.
