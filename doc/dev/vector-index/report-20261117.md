# VECTOR review implementation report

Date: 2026-11-17. This report supersedes the
[November 11 report](report-20261111.md), which describes older contracts.
The active native contract is [contract.md](contract.md).

## Source revisions

- Starting native revision: `42ea5c3da6eeaff4a5822b10921ccf031fb75c64`.
- Embedding type: `2de44f8a1cb8882f191613b40d9d5641a96c662e`.
- Native VECTOR: `aa2d8bf5ec6d6667b34a633012c9fa846879699f`.
- Backend isolation: `7b83b972f2704d20b07e4ff8af320a5a57648b1a`.
- USearch: `5c9595ed9a8d019570973c8647b62e6ba416ca6a`.
- Companion CRUD code: `e2b4c86883c37a0aa9277639a163eca95fd0dc6a`.
- Unchanged vshard: `674cda708d957cc145aa4f0622804bb5c2f8007a`.

These revisions and this report are local commits, not a push. The native
commit pins the USearch revision; the CRUD workflow pins native VECTOR.
The exported USearch slot-reuse patch applies in reverse to that revision.

## Review decisions and implementation

The read-API, embedding and slot-lifecycle reviewers agreed on the RFC
changes before implementation, then checked their respective code paths.
The six Word notes are addressed as follows.

1. HNSW is the approximate algorithm; USearch is its replaceable initial
   library. There is no comparative performance claim. Ordinary VECTOR
   pairs is unsupported: ALL duplicates primary iteration and the current
   top-k search exposes no resumable cursor. This is not a claim that
   HNSW can never support pagination. Unkeyed count counts visible primary
   rows; keyed count is unsupported. Core frozen read-view iteration
   remains a separate exact interface. Filtering before the local limit
   excludes foreign buckets without consuming CRUD's result budget.
2. The note attached to "For example" concerns timeout. Rich search takes
   the remaining distributed deadline from CRUD. A client wait timeout
   alone cannot stop synchronous work in the TX thread; a fiber slice is
   not the request's remaining budget. Ordinary select retains its internal
   one-second search budget and has no new server timeout option.
3. The unreleased prototype uses only embedding fields and index parts.
   ARRAY/BIN storage fallbacks and box.vector pack/unpack are removed.
   Queries accept arrays or embedding values. Embedding uses extension 9
   with 1..4096 finite IEEE 754 f32 coordinates in little-endian order.
   Shared conversion rounds ties to even. C/FFI MsgPack, JSON/YAML, tuple
   fields, recovery and IProto support the type. Unsupported SQL and
   comparable-key uses fail explicitly. CRUD returns portable arrays and
   loads embedding lazily to preserve startup on a core without VECTOR.
4. A label identifies a vector group, independently of a primary key,
   tuple address or graph slot. Tuple-version bindings select the exact
   MVCC-visible result. Coordinates can be shared across versions and
   graph generations. The new RFC diagram is editable Mermaid code.
5. GC-safe graph slots are reused before growth. USearch index_gt is a
   low-level class, while ann_usearch.cc is the Tarantool adapter. The dense
   wrapper uses the same update operation and does not supply MVCC or
   allocation-free rollback. Prepared snapshots restore affected outgoing
   and reverse links, labels and keys after failure. Repeated reuse avoids
   duplicate reverse edges. Old coordinates remain owned until success.
6. Current and rebuilding generations change atomically. Reuse remains
   possible at the append cap; automatic compaction starts at both 64 free
   slots and a 25 percent free ratio. The finite initial rebuild range and
   bounded concurrent admission remain in place. Backshift deletion avoids
   accumulating historical lookup tombstones. Only slots.reusable is
   exposed; the unreleased pending_rebuild alias is removed.

The reserved pagination result is preserved: with_distance returns
tuples, nil, distances. Without it, search returns tuples only.
Additional fixes cover typed query validation, linearizable count,
JSON/YAML without metatables, and the expanded field compatibility matrix.

## Backend isolation follow-up

USearch was chosen initially for its convenient C-compatible API. The current
adapter uses the lower-level C++ graph to supply Tarantool's required numeric,
allocation, undo and stop behavior. Other libraries still need integration for
MVCC, rollback, memory quota and cooperative execution. Total integration and
maintenance costs have not been measured comparatively.

Memtx now obtains the implementation from the algorithm registry and routes
all backend operations through its vtable. Shared insertion, membership and
physical slot reclamation are part of the common C contract. Shared coordinate
ownership moved to ann_vector.c, and HNSW configuration moved to ann_hnsw.h.
Neither requires USearch types. The registry and library adapter determine
which implementation serves hnsw; changing that mapping does not change Lua
or CRUD interfaces. Public algorithm selection still accepts only hnsw.

Flat implements the extended contract, including borrowed unaligned coordinates.
The same 36 assertions run on each backend: allocation failure, visibility,
numeric results, cancellation, rollback, GC and external-owner lifetime.
This verifies the shared boundary, not the integration cost of another ANN
library. No new continuing-search interface or public pairs support is claimed.
The RFC explicitly separates that current limitation from HNSW itself.

## Word artifact verification

Input: the user's third numbered Word export, SHA-256
`f0b2baed8a659a1736bea986189e37e51c7d165403fcc95a5bb60b5204b8b971`.
Output: vector-index-rfc-backend-red.docx, SHA-256
`8921b3ba0b062698fb7fda595e0dd5e334d9c9317f0df41d07186411142dc3dd`.
Both files are external to the repository.

The revision changes 33 paragraphs, adds a Mermaid block, and removes one
duplicate C field. Insertions and replacements are red relative to this
input. Other source prose is preserved. All six native Word comments,
seven tables and three original formula images remain. Internal links,
image embedding, comment ranges and unchanged text were checked. Exactly three
paragraphs differ from the preceding revision: backend choice, the common
interface and the pairs rationale. The 22 rendered pages were verified: six
are byte-identical to previously inspected pages, and the other 16 were
inspected again. The earlier export layout and link repairs remain intact.

## Verification on Ubuntu

After backend isolation, the full GCC Debug build and all 18 focused CTest
entries passed in one run. The Clang 19 ASan+UBSan build and all 12 focused
entries also passed in one run. The sets include the two common backend
contract subtests, quota/rollback/lifetime units, VECTOR functional tests and
regular-build replication. Checkpatch and git diff --check reported no errors.
Additional isolated AppleClang Debug and sanitizer contract checks passed.
The 14 changed source/build/test files match the Ubuntu copies by SHA-256.
Two new headers had only trailing blank lines removed after the test run;
that formatting was synchronized before the final source comparison.
Logs, the source manifest and document audit are retained externally beside
the revised Word artifact, under verification/.

The following broader results describe the preceding embedding review:

GCC 13.3 Debug and Clang 19 ASan+UBSan builds completed on the selected
Ubuntu host. Its existing checkout was synchronized without resetting
unrelated work. The binary's old git-describe label does not identify these
sources. SHA-256 comparison verified all 59 changed code, test and benchmark
files against their local versions, including the USearch header.

- All 18 focused native CTest entries passed across the initial run and
  repaired-fixture rerun: ANN/embedding units, VECTOR/type Lua suites and
  replication. Fixture repairs concerned SQL full-scan permission and a
  zero vector in a cosine read-view test.
- All 134 core unit entries passed across the initial run and the repaired
  tuple-format fixture rerun. The compatibility matrix includes the new
  field type. A final clean build and seven selected unit entries passed.
- ASan+UBSan passed 12 focused entries; after the final source adjustments,
  the sanitizer build and all five ANN/embedding unit entries passed again.
- All 129 core read-view Lua tests passed.
- All 12 focused CRUD tests passed, including lazy unavailable-feature
  handling, storage privileges, routing and the real cluster path.
- The complete three-replicaset CRUD fixture passed, including migration,
  permissions, timeout and schema checks. It passed again after the final
  fixture cleanup. Five benchmark profiles ran 30 calls each with three
  participants; these are smoke profiles, not scalability evidence.
- Native benchmark smoke with 256 rows, dimension 8 and 10 queries reported
  Recall@10 = 1, no short results and successful rebuild. Transient memory
  returned to zero; memory.vectors and memory.retained were zero as expected.
- LuaCheck reported no warnings or errors for the changed Lua files.
  git diff --check passed. Tarantool checkpatch reported zero errors after
  excluding the required volatile f32 conversion and a C++ reference
  spacing false positive; later benchmark/CI changes passed separately.

The initial aggregate runs and their fixture reruns are recorded separately;
they are not represented as single all-green executions. Logs, benchmark
JSON, document checks and the source hash manifest are retained locally
beside the external Word artifact, under verification/.

## Reproduction

Build Debug with recursive submodules, including the pinned USearch fork.
For the native focused set:

```sh
ctest --test-dir build --timeout 180 --output-on-failure -E '/\._' \
  -R '(test/unit/(ann[._]|embedding[._])|embedding_type|memtx_vector|replication-luatest/vector_index)'
ctest --test-dir build -L tarantool-unit -j 4 --timeout 120 -E '/\._'
```

Build the sanitizer tree with Clang, ENABLE_ASAN=ON and ENABLE_UB_SANITIZER=ON;
use ASAN_OPTIONS=detect_leaks=1 and UBSAN_OPTIONS=halt_on_error=1 for its
focused CTest set. The companion [CRUD report][crud-report] records cluster
setup and commands. Select a matching Luatest checkout for runner and child
processes. Benchmark scripts are perf/lua/vector_index.lua and the companion
perf/vector_search.lua.

## Limits

Coordinates are inline in a tuple. Borrowing them pins the whole tuple;
it does not retain only an independent vector blob. Payload-only updates
share the ANN coordinate owner but create a new tuple containing its field.
The RFC plans a copied-versus-borrowed ownership comparison using the same
embedding format. That comparison and production-scale load measurements
have not been performed. Live data and MVCC readers still consume quota;
slot reuse bounds historical churn, not the amount of retained live data.

[crud-report]: https://github.com/mandesero/crud/blob/codex/vector-search/doc/vector-search-report.md
