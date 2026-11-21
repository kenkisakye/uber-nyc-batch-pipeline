# Pipeline run report

Generated 2026-11-21T14:18:47+01:00 from the run records in `pipeline-state`.

| Stage | Unit | Status | Outcome | Rows in | Valid / pickups | Rejected | Seconds | Breakdown (s) |
|---|---|---|---|---:|---:|---:|---:|---|
| ingestion | 2014-04 | succeeded | ingested | 564,516 |  |  | 1.89 | check 1.21, upload 0.45 |
| ingestion | 2014-05 | succeeded | ingested | 652,435 |  |  | 1.77 | check 1.35, upload 0.4 |
| ingestion | 2014-06 | succeeded | ingested | 663,844 |  |  | 1.76 | check 1.36, upload 0.39 |
| ingestion | 2014-07 | succeeded | ingested | 796,121 |  |  | 2.16 | check 1.66, upload 0.48 |
| ingestion | 2014-08 | succeeded | ingested | 829,275 |  |  | 2.13 | check 1.65, upload 0.46 |
| ingestion | 2014-09 | succeeded | ingested | 1,028,136 |  |  | 2.64 | check 2.12, upload 0.51 |
| validation | 2014-04 | succeeded | validated | 564,516 | 564,512 | 4 | 9.15 | read_and_check 5.93, write 3.19 |
| validation | 2014-05 | succeeded | validated | 652,435 | 652,435 | 0 | 8.59 | read_and_check 5.75, write 2.82 |
| validation | 2014-06 | succeeded | validated | 663,844 | 663,844 | 0 | 8.06 | read_and_check 5.36, write 2.68 |
| validation | 2014-07 | succeeded | validated | 796,121 | 796,121 | 0 | 7.71 | read_and_check 4.94, write 2.74 |
| validation | 2014-08 | succeeded | validated | 829,275 | 829,275 | 0 | 7.86 | read_and_check 5.03, write 2.82 |
| validation | 2014-09 | succeeded | validated | 1,028,136 | 1,028,136 | 0 | 8.84 | read_and_check 5.72, write 3.1 |
| release | 2014-Q2 | succeeded | published |  | 1,880,791 |  | 7.69 | read_and_aggregate 4.46, write_and_check 3.2 |
| release | 2014-Q3 | succeeded | published |  | 2,653,532 |  | 7.97 | read_and_aggregate 4.8, write_and_check 3.15 |

| Stage | Total job seconds |
|---|---:|
| ingestion | 12.4 |
| validation | 50.2 |
| release | 15.7 |

First job started 2026-11-21T13:03:07+00:00, last job finished 2026-11-21T13:05:06+00:00: **2.0 minutes** end to end, including Spark start-up for every job and the scheduler's pauses between jobs.

All stages succeeded.
