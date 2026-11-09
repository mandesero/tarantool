## Feature

- Add `index:stat()` counters and memory accounting for VECTOR indexes.
  Add explicit `index:rebuild()` to replace the graph generation and
  reclaim slots without changing tuples or the index definition.

- Make VECTOR rebuild cooperative with atomic generation publication and
  atomic dual-graph writes during concurrent DML. Index replacement
  still reports a retryable conflict.
  Account active and transient generations separately. Reject online VECTOR
  creation on a nonempty space with a HASH primary key.

- Share immutable VECTOR coordinates and graph nodes across tuple versions
  when the embedding and primary key are unchanged. Rebuild shares coordinates
  between generations, preserves progress under writes and version GC, and
  automatically compacts navigation tombstones. Protect maintenance quota and
  bound concurrent graph growth; admission exhaustion reports OutOfMemory.
  Expose distinct embeddings, maintenance progress and reserved quota in stat.
