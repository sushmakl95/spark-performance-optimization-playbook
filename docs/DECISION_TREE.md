# Optimization Decision Tree

A step-by-step guide for diagnosing Spark performance problems. Work top to bottom — each step has a measurement, a common cause, and the technique to apply.

## Step 1 — Is the job slow end-to-end or just one stage?

Open the Spark UI → **Stages** tab. Find the stage with the longest duration.

- **One stage dominates (> 70% of total time)** → continue to Step 2 for that stage
- **Multiple stages have similar times** → Step 7 (executor tuning) or Step 8 (caching)

## Step 2 — Is the slow stage I/O-bound or compute-bound?

In the Spark UI, check the stage's **Input Size / Records** column.

- **Huge input, few records** (reading much more data than needed) → Step 3
- **Records roughly match input** → Step 4 (shuffle/join problem)

## Step 3 — I/O-bound: are you reading more than necessary?

Run `EXPLAIN` on the query and look for:

| Plan phrase | Meaning | Technique |
|---|---|---|
| `FileScan parquet [...] PartitionFilters: []` on a partitioned table | You're reading ALL partitions | **partition_pruning** — check WHERE clause hits the partition column directly (no function wrapping) |
| `DataFilters: [...]` but `PushedFilters: []` | Predicates not pushed to Parquet reader | **predicate_pushdown** — remove function wrapping (`UPPER(col)`) |
| 1000+ tiny files in input | Small-files problem | **file_sizing** — rewrite with `coalesce(N)` where N targets 128-256MB per file |
| Predicates span multiple columns but only one is partitioned | Partition key doesn't match query shape | **zorder** — Delta Z-ORDER on 2-4 filter columns |
| Reading ORC when Parquet is available | Format mismatch | **storage_formats** — prefer Parquet or Delta for modern stacks |

## Step 4 — Compute-bound stage: is it a join?

In Spark UI → SQL tab, view the query plan. Find the slow stage's operator.

- **`SortMergeJoin`** → Step 5
- **`BroadcastHashJoin`** → probably fine; check for OOM on the broadcast side
- **`ShuffledHashJoin`** → Step 5
- **No joins, just `Aggregate` / `Window`** → Step 7 (executor sizing) + Step 8 (caching)

## Step 5 — Shuffle join: is one side "small" (< 100MB after filters)?

Run the query and inspect the shuffle write size in Spark UI.

- **Yes (small side < 100MB)** → **broadcast_join**
  - Use `F.broadcast(small_df)` or raise `spark.sql.autoBroadcastJoinThreshold`
  - 5-20× speedup typical
- **No (both sides large)** → Step 6

## Step 6 — Big × big join: is there skew?

In Spark UI, look at the slow stage's **task duration distribution**:

- **One/few tasks much slower than others (10x+ longer)** → skew
  - **First try: AQE** (`spark.sql.adaptive.enabled=true` + `spark.sql.adaptive.skewJoin.enabled=true`)
  - **AQE insufficient** → **skew_handling** (manual salting)
- **Uniform task durations** → the join itself is just big; try:
  - Pre-filter either side as much as possible
  - Check if a sort-merge can become a bucketed sort-merge
  - Consider if the join is necessary at all (denormalization)

## Step 7 — Executor/memory tuning

Check Spark UI → **Executors** tab for:

| Symptom | Fix |
|---|---|
| Many small executors, each < 4 cores | Merge: 4-5 cores per executor is sweet spot |
| GC time > 20% of task time | Increase executor memory; ensure heap < 32GB |
| Task deserialization time > task run time | Enable Kryo serializer |
| Executors idle while driver works | Add `.collect()` sparingly; use `.write` or `.count()` to force distributed execution |

## Step 8 — Caching

Profile your job graph:

- **Does the same DF get computed multiple times downstream?**
  → Yes: `.persist(MEMORY_AND_DISK)` + `.count()` to materialize before the split
  → No: don't cache (cache has a cost)
- **Is the cached DF being evicted?** (Spark UI shows cache misses)
  → Use `MEMORY_AND_DISK` instead of `MEMORY_ONLY`
  → Reduce the DF's size (filter, select only needed columns) before caching

## Common anti-patterns (avoid)

- **Calling `.count()` on an expensive DF for "validation"** — materializes it just to throw the result away
- **Using `.collect()` on the driver with > 100k rows** — kills driver memory
- **Filter AFTER a join** when you could filter BEFORE — multiplies shuffle size
- **Nesting UDFs** — Python UDFs are 10-100× slower than native Spark expressions
- **Writing 1-row-per-partition** (common after `groupBy` + `write`) — creates millions of tiny files

## When to stop tuning

Stop when the job is "fast enough" for the SLA and the cost is below the threshold you care about. Premature optimization steals time from problems that matter more.

A reasonable target for nightly batch ETL: **under 30 min on a right-sized cluster, cost < $10/run**.
