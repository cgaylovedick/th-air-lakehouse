# Thailand Air Quality & Weather Lakehouse

An end-to-end data engineering project that collects hourly air-quality and weather
data for Thai provinces and turns it into analytics- and ML-ready tables in a
Lakehouse.

> **Status:** Step 1 of 5 — raw (bronze) ingestion with Airflow + RustFS is working.

## Architecture (target)

```
Open-Meteo APIs ──► Kafka ──► Spark ──► Apache Iceberg on S3 (RustFS)
                                         ├─ bronze : raw JSON, as received
                                         ├─ silver : cleaned, typed, validated
                                         └─ gold   : star schema for BI / ML

Orchestration: Apache Airflow      Data quality: Great Expectations
Observability: Prometheus + Grafana   Runtime: Docker Compose
```

## What runs today (Step 1)

| Service    | Purpose                                   | URL                    |
|------------|-------------------------------------------|------------------------|
| Airflow    | Schedules and runs the pipeline           | http://localhost:8080  |
| RustFS     | S3-compatible object storage (the lake)   | http://localhost:9001/rustfs/console/ |
| PostgreSQL | Airflow metadata database                 | internal               |

DAG `ingest_open_meteo` runs **every hour** and:

1. Creates the `lakehouse` bucket if it does not exist.
2. Calls the Open-Meteo air-quality and weather APIs **in parallel**
   (dynamic task mapping) for 8 provinces in one request each.
3. Validates that one result came back per province.
4. Writes the raw response plus ingestion metadata to:

```
s3://lakehouse/bronze/open_meteo/<source>/dt=YYYY-MM-DD/hr=HH/data.json
```

### Design decisions

- **Idempotent loads** — the object key is derived from the run's data interval,
  not the wall-clock time, so re-runs and backfills overwrite the same hour
  instead of creating duplicates.
- **Hive-style partitions (`dt=` / `hr=`)** — lets later Spark/Iceberg jobs read
  only the hours they need.
- **Raw data is kept unchanged in bronze** — if a cleaning rule changes later,
  silver can be rebuilt from bronze without calling the API again.
- **Storage is accessed only through the S3 API** — the original plan used MinIO,
  but its images were removed from Docker Hub in 2026. Because the code only
  talks to a generic S3 endpoint, switching to RustFS was a config change, and
  moving to AWS S3 later needs no code change either.
- **Retries with delay** — transient API errors are retried 3× before failing.

## How to run

Requirements: Docker Desktop (≥ 4 GB RAM allocated), Git.

```bash
cp .env.example .env          # Windows PowerShell: copy .env.example .env
docker compose up -d
```

Wait 1–2 minutes, then:

1. Open http://localhost:8080 (user `admin`, password `admin`).
2. Un-pause the `ingest_open_meteo` DAG and trigger a run.
3. Open the RustFS console at http://localhost:9001/rustfs/console/
   (`lakeadmin` / `lakeadmin123`) and browse the `lakehouse` bucket.

To stop: `docker compose down` (add `-v` to delete all data).

## Roadmap

- [x] Step 1 — Bronze ingestion (Airflow + RustFS)
- [ ] Step 2 — Spark + Apache Iceberg: bronze → silver → gold
- [ ] Step 3 — Data quality gates with Great Expectations
- [ ] Step 4 — Pipeline observability with Prometheus + Grafana
- [ ] Step 5 — Streaming ingestion with Kafka
- [ ] Stretch — Move storage to AWS S3

## Data source

Weather and air-quality data from [Open-Meteo](https://open-meteo.com/)
(free for non-commercial use; see their terms).
