"""Silver-layer weather processing building blocks (pure Python, no Airflow/GCP imports).

Modules (one responsibility each):
    schema        - silver/gold column contract and the SilverRecord model
    validator     - validation rules for raw bronze payloads
    flattener     - nested bronze payload -> flat SilverRecord
    deduplicator  - removes duplicates by (location_name, event_time)
    partitioning  - bronze/silver path conventions and hour partitions
    csv_writer    - SilverRecord list -> CSV text
"""
