# Performance: measurements and tuning

Every number on this page was measured, and the raw results are in `audit.benchmark_results`. Nothing here is an estimate. Where a change did not help, it says so.

## How it was measured

- **Environment:** Docker Desktop VM with **4 CPUs and 3.8 GB RAM**, shared by Spark (`local[2]`, 2 GB driver), Postgres 15, Prefect and the dashboard. Timings vary noticeably between runs on this machine, so every case was repeated **3 times**. The tables show the **median**, with min–max where the spread matters. Every case had one untimed warm-up run first. Without it, the first variant also pays the JVM/JDBC warm-up: in a smoke test, that made a JDBC read look 4x slower than it was.
- **Scales:** **1x** is today's data (20k orders, 25k order items). **10x** is 200k orders, made from the real rows with new keys.
- **Isolation:** benchmarks run the real pipeline functions on copies: a `lake/_bench` folder and `bench*` Postgres schemas, all removed afterwards. They never touch the real lake, source, staging or Gold.
- **How to run them:**
  ```bash
  docker compose exec -w /app/src -e LAKE_PATH=/app/lake/_bench pipeline python benchmark.py --label mytest --scales 1 10
  docker compose exec -w /app/src pipeline python benchmark_dbt.py --label mytest --scales 1 10
  ```
  ```sql
  SELECT benchmark, variant, scale, median_seconds FROM audit.benchmark_results WHERE suite_run LIKE 'mytest%';
  ```

## Results and decisions

### 1. Full vs incremental loads

**Silver orders** (`process_incremental`): a batch of 1% updated + 0.5% new orders, read from Bronze through Change Data Feed, vs a full pass over all of Bronze.

| Scale | Incremental | Full pass | Skip (nothing new) |
|---|---|---|---|
| 1x (312 changed rows) | 36.7 s | 44.9 s | 1.66 s |
| 10x (3,053 changed rows) | 44.6 s | 55.9 s | 2.37 s → **0.59 s** after tuning |

- The incremental pass is about **20% faster** at both scales. It isn't more, because the fixed cost of a Spark job and Delta MERGE (planning, file listing, writing a new version) dominates at these sizes. The gap grows with table size, since the full pass reads everything while the incremental pass reads only the changes.
- A second "after tuning" run gave 33.2 s incremental and 60.5 s full at 10x. Those differences are within this machine's run-to-run spread, so **no speed-up is claimed for them**.
- The **skip path** (nothing new in Bronze) went from 2.37 s to **0.59 s**. It no longer scans the whole Delta history for the CDF start version when the checkpoint already equals the Bronze version.
- In a real pipeline run, the first Delta operation of each Spark process also pays about 37 s of JVM/Delta warm-up (measured in Phase 3). That's why a "skip" in the daily run looked like 59 s: it is per process, not per table.

**Bronze extract** (JDBC read from the source and write to Delta):

| Variant | 1x | 10x |
|---|---|---|
| Full read, `fetchsize` default | 6.5 s | 26.0 s (14.3–60.3) |
| Full read, `fetchsize=10000` | 11.8 s (8.8–19.6) | **13.2 s** (10.5–13.3) |
| Incremental window (10 min, 2 / 20 rows) | 12.3 s | 17.6 s |

- **Decision: `JDBC_FETCH_SIZE=10000`.** At 10x it was **2x faster and far more stable**. Without a fetch size, the Postgres driver buffers the entire result in the driver's memory. At 1x the default was faster, so the benefit only appears with volume.
- The incremental extract is **not faster than a full read at these sizes**. A few rows still cost a JDBC connection, a query and a Delta MERGE. Its value is that the work stays constant as the source grows, while the full read keeps growing.

**dbt facts** (`fact_orders` and `fact_payments`; the time is dbt's model execution time, without dbt's own startup of about 5 s):

| Scale | Full refresh | Incremental, no changes | Incremental, 1% of orders changed |
|---|---|---|---|
| 1x (25k items) | 1.4 s | 1.0 s | 1.0 s |
| 10x (250k items), first version | 8.7 s | **did not finish in 17 min** | not reached |
| 10x, fixed | 11.3 s | 5.3 s | **3.3 s** |

**A real bug found by measuring.** The first incremental filter (Phase 4) was `updated_at >= max(...) OR NOT EXISTS (key in target)`:
- Because of the `OR`, Postgres can't turn `NOT EXISTS` into an anti-join. It ran it as a correlated subquery once per row, and without an index each one scans the whole fact table.
- The planner's cost estimate was **3.5 billion** (against about 21k for the hash joins).
- At 1x it showed up as one 481 s outlier, on the first incremental run after a full refresh.

[`incremental_predicate.sql`](../dbt_project/macros/incremental_predicate.sql) now uses `LEFT JOIN target ... WHERE key IS NULL`. Postgres runs that as one hash anti-join, with a planner cost of 6.7k on today's data. The model output is unchanged; the completeness test and the full-refresh comparison from Phase 4 both still pass.

**An infrastructure limit found the same way.** With the fixed plan, the 10x build failed with `could not resize shared memory segment ... No space left on device`:
- Postgres parallel hash joins use `/dev/shm`, and Docker gives a container only 64 MB.
- Production would hit this at about 250k rows.
- Fixed with `shm_size: 256mb` on the Postgres service in `docker-compose.yml`.

### 2. Spark partitioning

Validate + deduplicate + write of the orders table with different `spark.sql.shuffle.partitions`:

| Shuffle partitions | 1x | 10x | 10x with `local[4]` | Files written |
|---|---|---|---|---|
| 2 | 6.9 s | 18.6 s | 14.6 s | 2 |
| 8 (current) | 7.1 s | 22.0 s | 17.6 s | 8 |
| 200 (Spark default) | 39.9 s | 46.3 s | 43.4 s | **200** |

- **The Spark default of 200 is 2–6x slower** here and writes 200 tiny files. Keeping it at 8 is right. Fewer partitions (2) were 15–20% faster at 10x, but 8 leaves room for growth. The setting is configurable (`SPARK_SHUFFLE_PARTITIONS`).
- **AQE did not merge the partitions.** Spark's adaptive execution normally merges small shuffle partitions, but `validate()` caches its result, and Spark doesn't let AQE change a cached plan's partitioning. That's why the file count followed the setting exactly.
- **`local[4]` vs `local[2]`:** the shuffle step was about 20% faster with 4 cores, but end-to-end Silver was not (incremental 48.7 s, full 59.4 s at 10x). **Decision: keep `local[2]`.** Postgres runs in the same 4-CPU VM during every pipeline step. The setting is configurable (`SPARK_MASTER`).

### 3. Small files and compaction

A table written in 100 small appends (what many small incremental loads leave behind), read with a group-by:

| Scale | 100 files | After OPTIMIZE (1 file) | OPTIMIZE itself |
|---|---|---|---|
| 1x | 16.2 s | 14.8 s | 48.7 s |
| 10x | 25.7 s | **10.9 s** | 42.8 s |

- Compaction pays off once tables have volume: reads are **2.4x faster at 10x**, and the 43 s rewrite is paid back after about 3 reads. At 1x the gain is small.
- **Silver was creating small files on every run.** Before this phase, each full-snapshot Silver table was written as **8 files**, one per shuffle partition, even `sellers` with 200 rows (3 KB). Writes are now sized to about 1M rows per file (`DELTA_ROWS_PER_FILE`), so these tables are 1 file each.
- **Weekly maintenance:** `ecommerce-lake-maintenance/weekly` (Sunday 03:00, [`maintenance.py`](../src/maintenance.py)) compacts tables with at least 16 files averaging under 32 MB. Incremental MERGEs into `bronze/orders` add files on every run with new data. VACUUM is a dry run unless `vacuum=true`. It always keeps each table's retention (7 days), so time travel within that window keeps working.
- **The maintenance job was itself slow at first.** Its first real run took **2,718 s for 18 tables with nothing to do**. Timing one table showed the VACUUM dry run took **128–164 s on a 3 KB table**, while reading the table details took 2 s. Delta lists files with a Spark job of `spark.sql.sources.parallelPartitionDiscovery.parallelism` tasks, which defaults to **10,000**. With 8 tasks the dry run took 11–13 s, and the whole job **281 s**, with the same result. The setting applies only to the maintenance session (`VACUUM_LISTING_PARALLELISM`).

### 4. Spark JDBC writes (Silver → Postgres staging)

200k orders at 10x, writing with different batch sizes and parallel connections:

| Partitions \ batch size | 1,000 (Spark default) | 10,000 | 50,000 |
|---|---|---|---|
| 1 (what the pipeline did) | 10.6 s | 5.4 s | 6.1 s |
| 2 | 5.8 s | 5.5 s | 6.4 s |
| 4 | 4.6 s | **4.5 s** | 6.6 s |

- **Decision:** batch size **10,000**, and tables over 50k rows are written over up to **4 connections** (`JDBC_BATCH_SIZE`, `JDBC_WRITE_PARTITIONS`). That's **2.3x faster** than before at 10x.
- 50,000 was slower than 10,000 at every parallelism level.
- At 1x every variant took about 1–2 s, so smaller tables stay on one connection and skip the extra repartition.

### 5. PostgreSQL indexes and queries

- **Fact indexes:** compared with and without indexes, using the fixed anti-join filter at 10x:
  - with indexes, incremental (1% changed) took 3.2 s vs 3.3 s without, so **no speed gain**;
  - full refresh was about 12% slower with them (12.6 s vs 11.3 s).
  
  Only a **unique index on each fact key** is kept, for integrity (Postgres rejects a duplicate key at write time). An `order_updated_at` index was tried and removed.
- **Staging indexes** on `order_id` were part of the "with indexes" runs and gave no measurable gain. The marts read whole staging tables with hash joins, so they weren't added.
- **Indexes that do earn their place:**
  - `source.orders(updated_at)` serves the incremental extract window;
  - the audit tables are indexed on run id and start time, used by the run-summary artifact, resume and the dashboard.
- **Query optimization:** the important query fix is the incremental anti-join above. The other heavy queries (reconciliation, scorecard and dashboard views) run in well under a second on today's data.

## Summary of changes

| Change | Measured effect |
|---|---|
| dbt incremental filter rewritten as an anti-join | 10x incremental: did not finish in 17 min → 3.3 s |
| Postgres `shm_size: 256mb` | 10x dbt build no longer fails |
| JDBC batch size 10,000 + up to 4 write connections | 10x staging write 10.6 s → 4.5 s |
| JDBC fetch size 10,000 | 10x extract read 26.0 s → 13.2 s, and stable |
| Silver writes sized to about 1M rows per file | 8 files → 1 per small Silver table |
| Weekly OPTIMIZE of small-file tables | 10x read 25.7 s → 10.9 s after compaction |
| VACUUM file listing with 8 tasks instead of 10,000 | maintenance job 2,718 s → 281 s (18 tables) |
| Cheaper Silver skip check | 2.37 s → 0.59 s |
| Kept: `local[2]`, 8 shuffle partitions, only the unique fact index | no measured gain from the alternatives here |

## Known limits of these measurements

- The largest scale is 200k orders. Beyond that, especially for anything that doesn't fit one machine, the right choices change: more shuffle partitions, partitioned Delta tables, a cluster.
- The machine is shared and noisy: medians move by 10–30% between suites. Differences smaller than that are reported but not claimed.
- Delta tables are not partitioned. At about 1 MB per table that is correct, since partitioning would create more small files. Partitioning `orders` by purchase month is the first step once a table reaches many GB.
