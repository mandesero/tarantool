## Feature

- Add `index:stat()` counters and memory accounting for VECTOR indexes.
  Add explicit `index:rebuild()` to replace the graph generation and
  reclaim slots without changing tuples or the index definition.

- Make VECTOR rebuild cooperative with atomic generation publication and
  retryable conflicts with concurrent writes, GC or index replacement.
  Account active and transient generations separately. Reject online VECTOR
  creation on a nonempty space with a HASH primary key.
