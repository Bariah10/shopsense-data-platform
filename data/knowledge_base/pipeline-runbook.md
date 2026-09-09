# Data Pipeline Runbook

The pipeline has five stages: Kafka ingestion, Bronze landing, Silver upsert, Gold aggregation,
and the RAG index refresh.
Records that fail the Pydantic data contract are written to the dead-letter topic orders.dlq
with a rejection_reason field, and to the quarantine folder on disk. They are never dropped silently.
If the quarantine rate for a run exceeds 25 percent, the on-call engineer inspects the producer
before allowing the Silver merge to run.
The Silver layer is maintained with a Delta MERGE keyed on order_id, so replaying the same batch
is safe and does not create duplicates.
Gold tables are rebuilt in full on every run because they are small.
