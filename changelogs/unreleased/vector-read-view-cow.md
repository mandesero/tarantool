## feature/core

* VECTOR read views use copy-on-write HNSW snapshots instead of exact scans
  of frozen primary storage. NEIGHBOR select requires an explicit limit of
  at most 1024 and rejects NEIGHBOR pairs and pagination. Snapshots survive
  DML, rollback, slot reuse, rebuild and DROP. Retained CoW storage is charged
  to memtx quota and included in index memory statistics.
