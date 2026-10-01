## Bugfix

- Recover large memtx VECTOR indexes from snapshots without applying a
  client fiber slice to the index rebuild performed during `box.cfg`.
  Normal client searches and writes remain bounded by their fiber slice.
