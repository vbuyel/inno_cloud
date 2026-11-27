# Weather Data Pipeline (GCP)

A pipeline that ingests and processes weather data: **bronze → silver → gold**.

## Overview

```text
 ┌──────────────────────────── GCP (provisioned by Terraform) ────────────────────────┐
 │                                                                                    │
 │  Cloud Scheduler ──(every 10 min, HTTP + OIDC)──► Cloud Function Gen2              │
 │                                                     │  1. reads the API key from   │
 │                                                     │     Secret Manager           │
 │                                                     │  2. GET tomorrow.io/realtime │
 │                                                     ▼  3. stores the JSON as is    │
 │                         GCS: bronze/weather/realtime/<loc>/YYYY/MM/DD/HH/<ts>.json │
 │                                                     ▲                              │
 └─────────────────────────────────────────────────────┼──────────────────────────────┘
                                                       │ reads
 ┌──────────── Your laptop (Docker) ───────────────────┼─────────────┐
 │  Airflow DAG "weather_silver_processing"            │             │
 │   1. list .json files for the previous hour ────────┘             │
 │   2. validate + flatten + dedup                                   │
 │   3. write CSV ───────────► GCS: silver/.../HH/<run_ts>.csv       │
 │   4. GCSToBigQueryOperator ─► BigQuery: gold table (append)       │
 └───────────────────────────────────────────────────────────────────┘
```

- **Process A (ingestion)** runs entirely in the cloud. Cloud Scheduler triggers the function every 10 minutes, and the function writes a file to bronze. Once `terraform apply` has run, it works on its own; your computer is not needed.
- **Process B (processing)** is Airflow running on your computer in Docker. You trigger the DAG manually (it has no schedule). It talks to GCS as a regular client: your own ADC (`gcloud auth application-default login`) is mounted read-only and used only to impersonate the `weather-dev-airflow` service account, so every GCP call is made as that account.

By default the DAG processes the previous UTC hour of all locations and writes one silver CSV per location-hour. The optional `source_path` parameter (`gs://bucket/prefix` or a prefix in the data bucket) overrides this.

Terraform provisions everything in the cloud: the buckets, the function, the scheduler job, the secret, the BigQuery table, the service accounts and their permissions. It does not manage Airflow.

## Repository structure

```text
inno_cloud/
├── .gitignore                    # .env, *.tfstate*, .terraform/, *.tfvars, keys, logs
├── .env                          # local variables (NOT in git)
├── pyproject.toml                # pytest settings (pythonpath: function/, airflow/)
├── requirements-dev.txt          # dependencies for local tests
│
├── terraform/                    # all cloud infrastructure
│   ├── versions.tf               # Terraform and provider versions (google, archive)
│   ├── .terraform.lock.hcl       # provider lock file
│   ├── providers.tf              # google provider + default_labels
│   ├── variables.tf              # input variables with validation
│   ├── locals.tf                 # resource names, labels, bronze/silver prefixes, API list
│   ├── terraform.tfvars          # your values (NOT in git)
│   ├── terraform.tfvars.example  # values template
│   ├── main.tf                   # main flow: Cloud Scheduler (*/10) → Cloud Function Gen2
│   ├── function_source.tf        # zip of function/ sources → object in the source bucket
│   ├── apis.tf                   # enables GCP APIs
│   ├── storage.tf                # data bucket (bronze/, silver/) + function source bucket
│   ├── secrets.tf                # tomorrow.io API key secret, tomorrow-io-api-key by default (no value)
│   ├── iam.tf                    # service accounts and least-privilege bindings (incl. run.invoker)
│   ├── bigquery.tf               # dataset + gold table (schema from the assignment)
│   └── outputs.tf                # function URL, buckets, table, SA emails
│
├── function/                     # Cloud Function: tomorrow.io → bronze
│   ├── main.py                   # HTTP entry point ingest()
│   ├── requirements.txt
│   └── weather_ingest/
│       ├── config.py             # IngestConfig: reads and validates env
│       ├── errors.py             # domain exceptions
│       ├── sources.py            # WeatherSource + TomorrowIoClient
│       ├── storage.py            # RawStorage + GcsRawStorage (no overwrites)
│       ├── paths.py              # BronzePathBuilder: <loc>/YYYY/MM/DD/HH/<ts>.json
│       ├── report.py             # IngestionReport: run summary, HTTP status
│       ├── service.py            # WeatherIngestionService: fetch → path → store
│       └── factory.py            # build_service(): wires the concrete implementations
│
├── airflow/                      # local Airflow (Docker)
│   ├── docker-compose.yaml       # official compose + GCP access via impersonation
│   ├── .env                      # Airflow secrets and pipeline settings (NOT in git)
│   ├── .env.example              # .env template
│   ├── config/airflow.cfg        # Airflow config (airflow-init creates it if missing)
│   ├── dags/
│   │   ├── weather_silver_processing.py  # bronze → silver CSV → gold BigQuery
│   │   └── gcp_connection_check.py       # GCP access smoke test
│   └── include/                  # logic with no Airflow/GCP dependencies
│       ├── transform.py          # SilverTransformer: parse → validate → flatten → dedup → partition
│       └── weather/
│           ├── schema.py         # CSV columns = BigQuery schema, SilverRecord
│           ├── validator.py      # validation rules
│           ├── flattener.py      # nested JSON → flat record
│           ├── deduplicator.py   # deduplication by (location_name, event_time)
│           ├── partitioning.py   # bronze/silver paths, "previous hour", source_path
│           └── csv_writer.py     # records → CSV
│
└── tests/
    ├── cloud_function/           # tests for function/ (fake HTTP and GCS)
    └── airflow_pipeline/         # tests for include/ and the DAGs (test_dags.py runs in the Airflow container)
```

> [!NOTE]
> The paths in the assignment start with `bronze/...` and `silver/...`, so a **single data bucket** with these prefixes is used. Permissions are separated with prefix-based IAM Conditions: the function can create objects only under `bronze/`, and Airflow only under `silver/`.

## Tests

```bash
# locally: everything except test_dags.py (skipped when Airflow is not installed)
.venv/bin/python -m pytest

# Airflow tests (including the DAGs) in a throwaway container
cd airflow
docker compose run --rm --no-deps -v "$(cd .. && pwd):/project:ro" -w /project \
  --entrypoint bash airflow-cli -c 'pip install -q pytest==8.3.4 && python -m pytest tests/airflow_pipeline'
```

## Service accounts

Names follow the pattern `<name_prefix>-<env>-<role>`. The table shows the default values (`weather`, `dev`).

| SA | Permissions |
|---|---|
| `weather-dev-ingest` (function) | create objects **only** under `bronze/` + read **only** the API key secret |
| `weather-dev-scheduler` | only invoke this function (`run.invoker`) |
| `weather-dev-airflow` | read bucket objects, create objects **only** under `silver/`, `bigquery.dataEditor` on the gold table, `bigquery.jobUser` on the project; users listed in `airflow_impersonators` get `iam.serviceAccountTokenCreator` on it |
| `weather-dev-fn-build` | build the function image in Cloud Build (`logging.logWriter`, `artifactregistry.writer`, `storage.objectViewer`) |
