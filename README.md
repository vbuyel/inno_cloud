# Weather Data Pipeline (GCP)

Пайплайн загрузки и обработки погодных данных: **bronze → silver → gold**.

## Общая картина

```text
 ┌──────────────────────────── GCP (создаётся Terraform) ─────────────────────────────┐
 │                                                                                    │
 │  Cloud Scheduler ──(каждые 10 мин, HTTP + OIDC)──► Cloud Function Gen2             │
 │                                                     │  1. берёт API-ключ из        │
 │                                                     │     Secret Manager           │
 │                                                     │  2. GET tomorrow.io/realtime │
 │                                                     ▼  3. кладёт JSON как есть     │
 │                         GCS: bronze/weather/realtime/<loc>/YYYY/MM/DD/HH/<ts>.json │
 │                                                     ▲                              │
 └─────────────────────────────────────────────────────┼──────────────────────────────┘
                                                       │ читает
 ┌──────────── Ваш ноутбук (Docker) ───────────────────┼─────────────┐
 │  Airflow DAG "weather_silver_processing"            │             │
 │   1. list .json за прошлый час  ────────────────────┘             │
 │   2. flatten + validate + dedup                                   │
 │   3. пишет CSV ───────────► GCS: silver/.../HH/<run_ts>.csv       │
 │   4. GCSToBigQueryOperator ─► BigQuery: gold table (append)       │
 └───────────────────────────────────────────────────────────────────┘
```

- **Процесс A (ingestion)** полностью живёт в облаке. Scheduler раз в 10 минут дёргает функцию, функция пишет файл в bronze. После `terraform apply` он работает сам, ваш компьютер для этого не нужен.
- **Процесс B (processing)** — Airflow на вашем компьютере в Docker. Вы запускаете DAG вручную. Он идёт в GCS как обычный клиент, используя credentials service account'а `weather-dev-airflow`.

Terraform создаёт всё, что в облаке: бакет(ы), функцию, Scheduler, секрет, BigQuery-таблицу, service accounts и права. Airflow Terraform не трогает.

## Как DAG понимает, что такое «прошлый час»

Если DAG запущен в 14:23 UTC, «прошлый час» — это 13:00–13:59. Значит, нужен префикс:

```text
bronze/weather/realtime/<loc>/2026/11/18/13/
```

Функция за этот час должна была положить туда около 6 файлов (по одному каждые 10 минут). DAG их читает, превращает в строки и записывает **один** CSV:

```text
silver/weather/realtime/<loc>/2026/11/18/13/<run_ts>.csv
```

Затем этот же CSV загружается в BigQuery.

Если передать параметр `source_path`, DAG берёт этот префикс вместо вычисленного. Это удобно для тестов.

## Структура репозитория

```text
inno_cloud/
├── .gitignore                    # .env, *.tfstate*, .terraform/, *.tfvars, ключи, логи
├── .env                          # локальные переменные (НЕ в git)
├── pyproject.toml                # настройки pytest (pythonpath: function/, airflow/)
├── requirements-dev.txt          # зависимости для локальных тестов
│
├── terraform/                    # вся облачная инфраструктура
│   ├── versions.tf               # версии Terraform и провайдеров (google, archive)
│   ├── providers.tf              # провайдер google + default_labels
│   ├── variables.tf              # входные переменные с validation
│   ├── locals.tf                 # имена ресурсов, метки, префиксы bronze/silver, список API
│   ├── terraform.tfvars          # ваши значения (НЕ в git)
│   ├── terraform.tfvars.example  # шаблон значений
│   ├── main.tf                   # основной поток: Cloud Scheduler (*/10) → Cloud Function Gen2
│   ├── function_source.tf        # zip исходников function/ → объект в бакете исходников
│   ├── apis.tf                   # включение GCP API
│   ├── storage.tf                # data-бакет (bronze/, silver/) + бакет исходников функции
│   ├── secrets.tf                # секрет tomorrow-io-api-key (без значения)
│   ├── iam.tf                    # service accounts и least-privilege биндинги (вкл. run.invoker)
│   ├── bigquery.tf               # dataset + gold-таблица (схема из задания)
│   └── outputs.tf                # URL функции, бакеты, таблица, email'ы SA
│
├── function/                     # Cloud Function: tomorrow.io → bronze
│   ├── main.py                   # HTTP entry point ingest()
│   ├── requirements.txt
│   └── weather_ingest/
│       ├── config.py             # IngestConfig: чтение и проверка env
│       ├── errors.py             # доменные исключения
│       ├── sources.py            # WeatherSource + TomorrowIoClient
│       ├── storage.py            # RawStorage + GcsRawStorage (без перезаписи)
│       ├── paths.py              # BronzePathBuilder: <loc>/YYYY/MM/DD/HH/<ts>.json
│       ├── report.py             # IngestionReport: итог запуска, HTTP-статус
│       ├── service.py            # WeatherIngestionService: fetch → path → store
│       └── factory.py            # build_service(): выбор конкретных реализаций
│
├── airflow/                      # локальный Airflow (Docker)
│   ├── docker-compose.yaml       # официальный compose + доступ к GCP через impersonation
│   ├── .env                      # секреты Airflow и настройки пайплайна (НЕ в git)
│   ├── .env.example              # шаблон .env
│   ├── config/airflow.cfg        # создаётся airflow-init
│   ├── dags/
│   │   ├── weather_silver_processing.py  # bronze → silver CSV → gold BigQuery
│   │   └── gcp_connection_check.py       # smoke-тест доступа к GCP
│   └── include/                  # логика без зависимостей от Airflow/GCP
│       ├── transform.py          # SilverTransformer: parse → validate → flatten → dedup → partition
│       └── weather/
│           ├── schema.py         # колонки CSV = схема BigQuery, SilverRecord
│           ├── validator.py      # правила валидации
│           ├── flattener.py      # вложенный JSON → плоская запись
│           ├── deduplicator.py   # дедупликация по (location_name, event_time)
│           ├── partitioning.py   # пути bronze/silver, «прошлый час», source_path
│           └── csv_writer.py     # записи → CSV
│
└── tests/
    ├── cloud_function/           # тесты function/ (фейковые HTTP и GCS)
    └── airflow_pipeline/         # тесты include/ и DAG (test_dags.py — в контейнере Airflow)
```

> [!NOTE]
> Пути в задании начинаются с `bronze/...` и `silver/...`, поэтому используется **один data-бакет** с этими префиксами. Права разделены через IAM Conditions по префиксу: функция может создавать объекты только в `bronze/`, Airflow — только в `silver/`.

### Тесты

```bash
# локально: всё, кроме test_dags.py
.venv/bin/python -m pytest

# тесты Airflow (включая DAG) в одноразовом контейнере
cd airflow
docker compose run --rm --no-deps -v "$(cd .. && pwd):/project:ro" -w /project \
  --entrypoint bash airflow-cli -c 'pip install -q pytest==8.3.4 && python -m pytest tests/airflow_pipeline'
```

## Порядок работы

Каждый шаг можно проверить отдельно.

### Шаг 1. Подготовка

- Выполнить `gcloud auth login` и `gcloud auth application-default login`.
- Получить API-ключ на [tomorrow.io](https://www.tomorrow.io/).
- Создать `.gitignore`. Сейчас `terraform.tfstate` лежит в папке, и он не должен попасть в git.

### Шаг 2. Terraform: базовая инфраструктура

- Описать APIs, бакет, BigQuery dataset и таблицу, секрет (только «контейнер», без значения), service accounts.
- Выполнить `terraform apply`.
- Положить значение ключа руками, чтобы оно не попало в код и state:

  ```bash
  echo -n "KEY" | gcloud secrets versions add tomorrow-io-api-key --data-file=-
  ```

- **Проверка:** в консоли видны бакет, пустая таблица и секрет.

### Шаг 3. Функция

- Написать `function/main.py`: прочитать ключ из env (Terraform пробросит его из секрета) → `GET https://api.tomorrow.io/v4/weather/realtime?location=<loc>&apikey=...` → `blob.upload_from_string(response.text)` в нужный путь.
- Описать функцию в Terraform: `archive_file` → upload zip → `google_cloudfunctions2_function` с `service_account_email = weather-dev-ingest`.
- **Проверка:** вызвать функцию вручную, в бакете должен появиться JSON:

  ```bash
  curl -H "Authorization: Bearer $(gcloud auth print-identity-token)" <function_url>
  ```

### Шаг 4. Scheduler

- `google_cloud_scheduler_job` со `schedule = "*/10 * * * *"`, `http_target` на URL функции и `oidc_token` от `weather-dev-scheduler`.
- **Проверка:** через 20–30 минут в bronze лежит несколько файлов. Дальше функция работает сама, а данные копятся, пока вы пишете DAG.

### Шаг 5. Локальный Airflow

- Скачать официальный `docker-compose.yaml` Airflow и выполнить `docker compose up`.
- Дать контейнерам доступ к GCP. Рекомендуемый вариант: смонтировать `~/.config/gcloud` в контейнер, а в connection `google_cloud_default` указать `impersonation_chain = weather-dev-airflow@...`. Для этого вашему пользователю нужна роль `roles/iam.serviceAccountTokenCreator` на `weather-dev-airflow` — её тоже выдаёт Terraform.
- Вариант проще, но хуже: JSON-ключ `weather-dev-airflow`, который лежит в `.gitignore`.
- **Проверка:** тестовый DAG выводит список файлов в bronze.

### Шаг 6. DAG

Задачи DAG:

1. **`resolve_source`** — взять `params.source_path` или вычислить префикс прошлого часа.
2. **`list_bronze_files`** — если `.json` нет, задача падает.
3. **`transform_to_silver`** — скачать файлы, распарсить и проверить:
   - структуру: `data`, `location`, `data.values` — это dict;
   - числовые поля и диапазоны.

   Невалидные записи логировать как warning, затем сделать dedup по `(location_name, event_time)`. Если валидных строк не осталось, задача падает. Результат записать в CSV с колонками строго как в схеме BigQuery.
4. **`load_to_gold`** (`GCSToBigQueryOperator`) — `source_format="CSV"`, `skip_leading_rows=1`, `write_disposition="WRITE_APPEND"`.

- **Проверка:** запрос возвращает строки:

  ```sql
  SELECT * FROM gold ORDER BY event_time DESC
  ```

### Шаг 7. Evidence

Собрать скриншоты:

- успешный `terraform apply`;
- файлы в bronze;
- логи функции;
- успешный DAG run в Airflow UI;
- CSV в silver;
- результат запроса в BigQuery.

## Service accounts

Имена строятся как `<name_prefix>-<env>-<роль>`. Ниже приведены значения по умолчанию (`weather`, `dev`).

| SA | Что ему можно |
|---|---|
| `weather-dev-ingest` (функция) | создавать объекты **только** под `bronze/` + читать **только** секрет с API-ключом |
| `weather-dev-scheduler` | только вызывать эту функцию (`run.invoker`) |
| `weather-dev-airflow` | читать объекты бакета, создавать **только** под `silver/`, `bigquery.dataEditor` на gold-таблицу, `bigquery.jobUser` на проект |
| `weather-dev-fn-build` | собирать образ функции в Cloud Build (`logging.logWriter`, `artifactregistry.writer`, `storage.objectViewer`) |
