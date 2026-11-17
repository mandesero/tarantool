# VECTOR CoW read-view implementation report

Date: 2026-11-18. This report supersedes the
[November 17 report](report-20261117.md), whose exact-scan read-view contract
is no longer active. The current native contract is [contract.md](contract.md).

## Source revisions

- Starting native revision: `fa07b3aa5ae8d94e571e55dec1355e9e59793b9b`.
- Backend snapshots: `2d7247e94486ee87a8be01594300663c101a6ff5`.
- Memtx read-view integration: `ae48f57d5c6245909cacf135760ae9ff8f1662f9`.
- USearch hooks: `378b4f3d7474bd98ef8f7225041f558545a07382`, on top of
  `5c9595ed9a8d019570973c8647b62e6ba416ca6a`.
- Unchanged CRUD: `1065bd56de3a9eff59a7f7bb18a60e092c2ae26e`.
- Unchanged vshard: `674cda708d957cc145aa4f0622804bb5c2f8007a`.

The new native and USearch commits are local. No push was requested for this
follow-up. The native submodule pin and exported usearch-cow.patch contain
the same hooks; reverse application of the export was checked.

## Implementation

Public VECTOR read views retain an immutable HNSW root and the normal memtx
primary snapshot. Opening a view does not walk graph nodes, copy their
pointer array or copy coordinates. A seven-level, 32-way persistent radix
array maps physical slots to immutable records. Each record owns its node
tape and a reference to canonical coordinates.

Before changing a shared node, the USearch adapter copies its tape. It
publishes updated records through private radix paths only after graph work
succeeds. Untouched nodes, paths and coordinates remain shared. Existing
internal graph undo restores private tapes after an interrupted graph call.
Published change undo instead restores retained tape pointers and a radix
root without allocation. A snapshot opened after apply but before rollback
therefore retains its own topology, labels and coordinates.

Snapshot searches allocate private scratch contexts per request. They do
not change the shared root or acquire mutable graph-node locks. The C
backend contract now includes snapshot creation, borrowed coordinate handles
and a request-local scratch allocator. Flat remains the test-only search
oracle and does not implement production snapshots.

The coordinate owner supplies a primary key for a point lookup in frozen
primary storage. The predicate admits only coordinates matching that tuple
version. The bounded result is ordered by distance and primary key and
deduplicated. No full primary scan or exact-search fallback remains in
NEIGHBOR. Like ordinary HNSW, the result is approximate and may be short.

Read-view NEIGHBOR select requires an explicit limit from 0 through 1024.
It uses the captured index search width, a one-second request budget, the
work limit, cancellation and fiber-slice checks. Unbounded NEIGHBOR pairs,
offset and cursor options are rejected. ALL still streams frozen primary
storage; it does not invoke HNSW. Ordinary VECTOR APIs and the reserved
tuples, nil, distances return positions are unchanged.

## Lifetime and memory

Read views pin the index's coordinate allocation context. Generation quota
owners are allocated independently instead of rotating two embedded owner
objects. They survive rebuild while any snapshot still references their
pages. Retiring a backend releases its live arrays, contexts and root;
its allocator counters remain until the last shared record is released.
DML, GC, slot reuse, rollback, rebuild and DROP cannot mutate an open view.

All backend tapes, radix pages, immutable records and snapshot backend
headers use the shared memtx quota. Shared allocations are counted once at
their owner. memory.read_views and memory.generations.read_views include
snapshot headers and retired generations retained by views. Allocations
shared with a current or transient generation stay in that category.
memory.total and bsize sum current, transient and read_views. Tuple storage
and per-request materialization remain outside index bsize.

The existing node cap bounds each working graph. It does not bound the
number of old CoW versions pinned by readers. Long-lived read views can
retain extra node tapes, pages and source tuples until closure. Quota
exhaustion rejects new allocations instead of discarding required snapshots.
The cost of additional write barriers and allocations needs production-scale
measurement; these correctness checks are not a performance comparison.

## Verification

GCC 13.3 Debug and Clang 19 ASan+UBSan builds completed on the selected
Ubuntu host. The final clean verification run passed:

- All 18 focused CTest entries: ANN and embedding units, VECTOR/type Lua
  suites and vector replication.
- All 12 focused ASan+UBSan entries with leak detection and fail-fast UBSan.
- All four general read-view CTest entries, including pagination and stats.
- LuaCheck for both changed Lua files: zero warnings and zero errors.
- git diff --check. Tarantool checkpatch has one C++ destructor syntax
  false positive at backend->~ann_backend(); excluding SPACING reports
  zero errors. No other diagnostics were suppressed.

New unit checks exercise constant-size root capture, physical slot reuse,
snapshots created before rollback, allocation-free undo, source destruction,
and 80 allocation-failure positions. A multi-level graph test reads a frozen
snapshot from another pthread while the writer reuses nodes and destroys
the original graph. Reader scratch ownership and final allocator release
are checked. This does not introduce an asynchronous public search API.

Luatest covers C and FFI paths with ordinary memtx and MVCC, HASH primary
lookup, committed snapshot visibility during rollback, DML, DROP, multiple
simultaneous views across two rebuilds, memory totals, allocation failure,
work exhaustion, zero/absent/oversized limits and unsupported pagination.

SHA-256 comparison verified all 19 transferred files against the local
sources, including the USearch header. The remote binary's old git-describe
label does not identify this source state. Logs, the exact manifest and
document checks are retained externally beside the Word artifact under
verification/. Earlier failing runs are not represented as green runs:
they exposed an immutable-node lock access, a test assertion helper typo,
and missing enum/aggregate initializers, all fixed before the final run.

## Word revision

Input is the user's third numbered Word export, SHA-256
`f0b2baed8a659a1736bea986189e37e51c7d165403fcc95a5bb60b5204b8b971`.
Output is vector-index-rfc-cow-red.docx, SHA-256
`839f4455629859174763e2e62ddbd6d90b8d67a2d4b8f5241798344c6745bb46`.
The files are external to the repository.

The CoW follow-up changes 11 prose paragraphs and adds two internal C
members relative to the previous backend edition. It replaces the read-view
scan description and adjusts memory accounting, rebuild lifetime and the
backend contract. Previously accepted edits remain. Insertions and
replacements are red relative to the user's original export.

All six native Word comments, seven tables, three original formula images
and editable Mermaid source are preserved. Comment ranges and image bytes
were checked. All 22 final pages were visually verified; the first seven
are byte-identical to already inspected pages of the initial render.

## Reproduction

Use the pinned recursive submodules and separate Debug and sanitizer builds.

```sh
ann_units='test/unit/(ann[._]|embedding[._])'
vector_lua='embedding_type|memtx_vector|replication-luatest/vector_index'
cmake --build build -j 4
ctest --test-dir build --timeout 180 --output-on-failure -E '/\._' \
  -R "($ann_units|$vector_lua)"
cmake --build build-vector-lifecycle-clang -j 4
asan_lua='memtx_vector_(atomic|embedding|lifecycle|mvcc|read_contract|review)'
ASAN_OPTIONS=detect_leaks=1 UBSAN_OPTIONS=halt_on_error=1 \
  ctest --test-dir build-vector-lifecycle-clang --timeout 180 \
  --output-on-failure -E '/\._' \
  -R "($ann_units|embedding_type|$asan_lua)"
ctest --test-dir build --timeout 180 --output-on-failure \
  -E '/\._' -R read_view
```
