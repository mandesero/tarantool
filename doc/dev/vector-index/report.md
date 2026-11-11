# VECTOR index implementation report

Date: 2026-11-11. Base: `cee48f4b03c96a1f200644a455952b3d65d06fbb`.
Native code: `a0c699201bc8c92eca046da479736b0813f3f101`.
The input is the supplied Word export with 12 review notes, SHA-256
`741a3d395e3fe27ce6f0e7f70f00fb4b96712d24419de480d6711a99e7e48d05`.
The revised Word artifact is `vector-index-rfc-review-red.docx`, SHA-256
`af536c41fb62acc6a1120d141ecc6a283ee248217865216407311debf9aca14c`.
It changes 36 paragraphs, adds resource-estimate and ownership-benchmark
paragraphs, and removes one duplicate C field. Other prose is preserved.
All seven tables remain,
with one updated search-result cell; three original formula images are intact.
Insertions and replacements are red. All 12 original comments, their authors,
dates and anchors remain native Word annotations. All 23 rendered pages and
internal links were checked. Export-only layout and broken links were repaired.
Both Word files are external to this repository. The implemented contract is
recorded in [contract.md](contract.md), with examples in [README.md](README.md).

The subsequent RFC-only discussion proposes omitting ordinary index:pairs:
ALL duplicates a primary scan, while exact unbounded NEIGHBOR repeatedly
rescans and is expensive. The tested code below still exposes both cursors;
that API removal has not been implemented. The Word revision also separates
field format from coordinate ownership and plans a comparison of ARRAY with
copied coordinates, VARBINARY with copied coordinates, and VARBINARY borrowing
a tuple. The second variant is not implemented. Borrowing pins the whole
source tuple, so lower total memory use is not guaranteed. Payload-only
updates share the ANN coordinate owner but still create a new tuple with its
vector field. These are documented limitations and proposed follow-ups,
not additional implementation or benchmark results.

## Delivered code

The memtx VECTOR index reads dense f32 vectors, computes distances in
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
takes a flat query and returns `tuples, nil, distances` when requested.
Timeout, filter and HNSW request options belong to search.
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
(`22ae9dde42`) and concurrent compaction (`080b5e174d`).
The November 11 update adds packed coordinate owners (`02186fc55c`) and
native cursors/read views in the code commit above. Companion CRUD commit
`0920d8b963e4bc62018ef74fdb4a1c9cf1d98f23` consumes the third distance
result and unpacks varbinary fields into the unchanged numeric-array wire
format. The vshard and USearch submodule pins are unchanged.

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

## November 11 review changes

Packed VECTOR parts use the existing varbinary type: exactly d IEEE 754 f32
little-endian components, without a header. box.vector.pack/unpack convert
arrays. The graph borrows immutable coordinates and retains their source
tuple until the last coordinate reference ends. This includes navigation
nodes and both generations. ARRAY fields still use a dense canonical copy.
Payload-only updates share the existing owner. Packed inserts validate and
read coordinates directly without an intermediate dense copy.

SELECT consumes a next/free index cursor with a bounded HNSW batch. Generic
create_iterator supports streaming ALL and exact unbounded NEIGHBOR. The
exact cursor keeps 32 results and its last emitted bound, then rescans the
primary index. For R results its work is O(N*d*ceil(R/32)); memory is bounded
by the query, batch and retained tuple sizes. It yields between rows in TX
outside non-MVCC transactions and rechecks visibility after caller yields.
Yielding pairs/count use Lua C to avoid the warmed FFI stack failure.

Public core read views retain frozen primary memtx storage, not a copied
HNSW graph. ALL streams the snapshot; NEIGHBOR uses the same exact batch
selection on stable raw tuples. DML, GC, rebuild and DROP do not change it.
This establishes stable read-view behavior, not ANN graph snapshots or
off-thread ANN support. Snapshot tuple retention remains memtx's concern.

One graph admits at most 2*K+64 nodes, where K is the maximum simultaneously
needed embedding count, including retained MVCC versions. This admission
bound applies even if background rebuild is delayed or fails. At the bound,
new embeddings return OutOfMemory; payload-only updates need no new node.
The existing maintenance trigger requires both 64 navigation-only nodes and
25% of graph nodes, checked during GC. Both graph generations and reserved
maintenance quota remain subject to the memtx memory policy.

Stat exposes nodes, embedding_peak and node_limit. Owned vector payloads
remain in vectors/retained. borrowed_vectors reports packed coordinate bytes
for bound objects only; it is excluded from total/bsize because source tuple
storage is already accounted by memtx. Header bytes remain in lookup.

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
  box-luatest/memtx_vector_review_test.lua \
  box-luatest/memtx_vector_sync_test.lua \
  replication-luatest/vector_index_test.lua \
  box-luatest/box_read_ffi_disable_test.lua
```

The CI workflow `.github/workflows/vector_index.yml` runs only native
VECTOR checks and uses the pinned USearch submodule. The companion CRUD
workflow builds this native code commit and runs its distributed checks.

The final code built on Ubuntu 24.04 x86_64 with GCC 13.3 in Debug mode.
All 14 CTest entries passed: four ANN unit executables, nine native VECTOR
Lua suites and the replication suite. They cover shared embeddings, payload
visibility, retained readers, filters, uncommitted versions, work limits,
allocation-failure atomicity in both graphs, automatic compaction, bounded
admission, cooperative build and rebuild, and writes/cancellation after
publication. The existing WAL, DDL, recovery and protocol cases also passed.

A separate Clang 19 Debug build with AddressSanitizer, leak detection and
UndefinedBehaviorSanitizer passed all seven selected entries: three ANN
units and the atomicity, lifecycle, MVCC and review Lua suites. The existing
read_view_test.lua passed all 129 tests in an isolated runner directory.
All 12 focused CRUD tests passed, including packed projection. LuaCheck
0.26.0 reported zero warnings/errors in the seven changed native Lua files
and two changed CRUD Lua files. Checkpatch validates the new native commits.
Full hosted CI and the complete Tarantool matrix were not run.

The new review suite covers result positions, unbounded ordered iteration,
caller yields, cancellation, DROP, own uncommitted changes, rollback,
snapshot stability with TREE/HASH primaries, delayed maintenance, packed
coordinates and fail-nth allocation atomicity. The ANN unit verifies an
unaligned borrowed pointer is retained directly and released exactly once.

The host had two Luatest installations; child processes initially selected
an older globally installed server helper. Checkout-local dependency links
made runner and children use the same version. The final read-view run used
a fresh VARDIR. These dependency files are not part of either patchset.

The host reuses its task build tree. Its binary reports the older
`8bd63fc90` git-describe label, which is not the source revision of these
results. The native code commit and measurement context above identify the
source. SHA-256 checks confirmed all 25 changed native implementation/test
files and both changed CRUD Lua files match the tested Ubuntu sources.
The three-replicaset CRUD fixture and performance measurements belong
to the November 2 revision; this update does not claim a new full cluster
fixture or benchmark run. The focused CRUD suite used the current sources.

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
values. Exact unbounded cursors rescan and can consume substantial CPU; the
new tests establish correctness and bounded batching, not production latency.
Packed source tuples can retain non-vector fields until their coordinate
owner is released. Completion needs allocator success and builder scheduling;
long MVCC transactions can retain old versions. Quota exhaustion rejects new
allocations.
Yield checks target 1 ms portions but do not bound a single HNSW operation.
