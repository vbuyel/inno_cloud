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
- **Процесс B (processing)** — Airflow на вашем компьютере в Docker. Вы запускаете DAG вручную. Он идёт в GCS как обычный клиент, используя credentials service account'а `sa-airflow`.

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
├── .gitignore              # .env, *.tfstate*, .terraform/, *.json-ключи, terraform.tfvars
├── .env                    # локальные переменные (НЕ в git)
├── terraform/
│   ├── versions.tf         # terraform + провайдер (pinned)
│   ├── providers.tf
│   ├── variables.tf        # project_id, region, env, owner, locations… с validation
│   ├── terraform.tfvars    # ваши значения (в git не коммитить или коммитить example)
│   ├── apis.tf             # google_project_service
│   ├── storage.tf          # data-бакет (bronze/ и silver/ префиксы) + бакет для исходников функции
│   ├── secrets.tf          # секрет tomorrow-api-key (без значения!)
│   ├── iam.tf              # SA + биндинги
│   ├── function.tf         # archive_file → zip → cloudfunctions2_function
│   ├── scheduler.tf        # cloud_scheduler_job
│   ├── bigquery.tf         # dataset + gold table (схема из задания)
│   └── outputs.tf          # function_url, bucket, table_id, sa emails
├── function/
│   ├── main.py             # HTTP-обработчик
│   └── requirements.txt
└── airflow/
    ├── docker-compose.yaml # официальный compose от Airflow
    ├── dags/weather_silver_processing.py
    └── include/transform.py  # flatten/validate/dedup — отдельно, чтобы тестировать
```

> [!NOTE]
> Пути в задании начинаются с `bronze/...` и `silver/...`, поэтому проще всего сделать **один data-бакет** с этими префиксами. Разделение прав всё равно можно сделать через IAM Conditions по префиксу, но для учебного проекта достаточно прав на уровне бакета.

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
  echo -n "KEY" | gcloud secrets versions add tomorrow-api-key --data-file=-
  ```

- ✅ **Проверка:** в консоли видны бакет, пустая таблица и секрет.

### Шаг 3. Функция

- Написать `function/main.py`: прочитать ключ из env (Terraform пробросит его из секрета) → `GET https://api.tomorrow.io/v4/weather/realtime?location=<loc>&apikey=...` → `blob.upload_from_string(response.text)` в нужный путь.
- Описать функцию в Terraform: `archive_file` → upload zip → `google_cloudfunctions2_function` с `service_account_email = sa-weather-ingest`.
- ✅ **Проверка:** вызвать функцию вручную, в бакете должен появиться JSON:

  ```bash
  curl -H "Authorization: Bearer $(gcloud auth print-identity-token)" <function_url>
  ```

### Шаг 4. Scheduler

- `google_cloud_scheduler_job` со `schedule = "*/10 * * * *"`, `http_target` на URL функции и `oidc_token` от `sa-scheduler-invoker`.
- ✅ **Проверка:** через 20–30 минут в bronze лежит несколько файлов. Дальше функция работает сама, а данные копятся, пока вы пишете DAG.

### Шаг 5. Локальный Airflow

- Скачать официальный `docker-compose.yaml` Airflow и выполнить `docker compose up`.
- Дать контейнерам доступ к GCP. Рекомендуемый вариант: смонтировать `~/.config/gcloud` в контейнер, а в connection `google_cloud_default` указать `impersonation_chain = sa-airflow@...`. Для этого вашему пользователю нужна роль `roles/iam.serviceAccountTokenCreator` на `sa-airflow` — её тоже выдаёт Terraform.
- Вариант проще, но хуже: JSON-ключ `sa-airflow`, который лежит в `.gitignore`.
- ✅ **Проверка:** тестовый DAG выводит список файлов в bronze.

### Шаг 6. DAG

Задачи DAG:

1. **`resolve_prefix`** — взять `params.source_path` или вычислить префикс прошлого часа.
2. **`list_files`** — если `.json` нет, задача падает.
3. **`transform`** — скачать файлы, распарсить и проверить:
   - структуру: `data`, `location`, `data.values` — это dict;
   - числовые поля и диапазоны.

   Невалидные записи логировать как warning, затем сделать dedup по `(location_name, time)`. Если валидных строк не осталось, задача падает. Результат записать в CSV с колонками строго как в схеме BigQuery.
4. **`GCSToBigQueryOperator`** — `source_format="CSV"`, `skip_leading_rows=1`, `write_disposition="WRITE_APPEND"`.

- ✅ **Проверка:** запрос возвращает строки:

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

| SA | Что ему можно |
|---|---|
| `sa-weather-ingest` (функция) | создавать объекты в бакете + читать **только** секрет с API-ключом |
| `sa-scheduler-invoker` | только вызывать эту функцию (`run.invoker`) |
| `sa-airflow` | читать и писать объекты в бакете, `bigquery.dataEditor` на dataset, `bigquery.jobUser` на проект |
