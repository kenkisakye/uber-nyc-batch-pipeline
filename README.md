# Uber NYC batch pipeline

A batch-processing data architecture that turns raw Uber pickup records into
quarterly demand tables for a forecasting application. Every component runs
as an isolated Docker container, and the whole system is defined in this
repository (Infrastructure as Code).

*Project: Data Engineering*  (DLMDSEDE02),
IU International University of Applied Sciences.

![Architecture](docs/architecture.png)

## Build status

| Step | Component | Status |
|---|---|---|
| 1 | Repository skeleton | Done |
| 2 | Storage: SeaweedFS (master, 2 volume servers, filer, S3 gateway, buckets, access keys) | Done |
| 3 | Ingestion service (monthly CSV to `raw`, with checksum) | Planned |
| 4 | PySpark monthly validation (`processed` and `quarantine`) | Planned |
| 5 | PySpark quarterly job (staging, checks, publish manifest last) | Planned |
| 6 | Scheduler: APScheduler with retries and run state | Planned |
| 7 | Failure tests, run times, documentation | Planned |

## Data

[Uber Pickups in New York City](https://www.kaggle.com/datasets/fivethirtyeight/uber-pickups-in-new-york-city)
(Kaggle, FiveThirtyEight). The pipeline uses the six monthly files from April
to September 2014, about 4.5 million pickups. Each row holds a pickup
timestamp, latitude, longitude and base code.

The files are not part of this repository. Download them from Kaggle and
place `uber-raw-data-apr14.csv` to `uber-raw-data-sep14.csv` in `data/`.

## Requirements

- Docker Desktop with Docker Compose v2
- About 6 GB of memory assigned to Docker (all images run natively on
  Apple Silicon and on Intel/AMD)

## Quick start

```bash
git clone <repository-url>
cd uber-nyc-batch-pipeline
cp .env.example .env          # then replace every "change-me" value
docker compose up -d
```

## Check that storage works

```bash
# All services healthy; storage-init has exited with code 0
docker compose ps -a

# Buckets created
docker compose logs storage-init

# Two volume servers registered with the master
curl -s "http://localhost:9333/dir/status?pretty=y"

# Every access key can do exactly what it should
docker compose --profile check run --rm access-check
```

Web interfaces (this machine only): master <http://localhost:9333>,
filer <http://localhost:8888>.

## Stop and reset

```bash
# stop, keep the data
docker compose down
# stop and delete all stored data        
docker compose down -v    
```

## Repository layout

```
docker-compose.yml   all services, networks and volumes
.env.example         pinned versions and access keys template
storage/             S3 gateway start script, bucket setup
tools/               access check
data/                landing folder for the Kaggle files
docs/                architecture diagram
```

## Buckets

| Bucket | Content | Written by | Read by |
|---|---|---|---|
| `raw` | Monthly CSV files exactly as delivered, with checksum | Ingestion | PySpark |
| `processed` | Valid rows as Parquet, one partition per month | PySpark | PySpark |
| `quarantine` | Rejected rows with the reason for rejection | PySpark | Operator |
| `published` | Quarterly releases; the manifest is written last | PySpark | Forecasting app |
| `pipeline-state` | Status of every monthly and quarterly run | Ingestion, PySpark | Scheduler |

## Failure handling

Measures added after the Phase 1 feedback. Status column is updated as each
step is built and tested.

| Failure | Measure | Status |
|---|---|---|
| A container crashes | `restart: unless-stopped` and health checks; services start only when the ones they depend on are healthy | In place |
| A volume server is lost | Replication `001`: every file is stored on both volume servers | In place |
| A task fails mid-run | Scheduler retries with back-off; only one run of a job at a time | Step 6 |
| A run is repeated | Each month and quarter overwrites its own output, so no duplicates | Steps 4–5 |
| The scheduler restarts | Run state per month in `pipeline-state`; finished months are skipped, failed ones repeated | Step 6 |
| Bad input rows | Rejected rows go to `quarantine`; the quarterly job stops if raw rows ≠ valid + quarantined rows | Steps 4–5 |
| A quarterly run fails | Output goes to staging first; the manifest is written last, so the previous release stays live | Step 5 |

## Maintenance

**Docker**
- All image versions pinned in `.env`; upgrade one service at a time, then run the checks again.
- Memory limit and log rotation for every container.
- Start, check and reset each take one command (see above).

**PySpark** (from step 4)
- Job code in this repository, parameterised by month or quarter.
- One fixed schema definition instead of schema guessing.
- Unit tests on a small sample of rows.
- Rows in, rows out and run time logged for every run.

## Security and data protection

- All services share one private Docker network; only the master and filer
  web interfaces and the S3 gateway are published, and only on `127.0.0.1`.
- Each service has its own S3 access key limited to the buckets it needs;
  the forecasting application can only read `published`.
- Access keys live in `.env`, which is never committed; the S3 identity file
  is generated inside the container at start-up.
- Exact pickup coordinates stay in `raw`; processed data holds only rounded
  grid cells.
