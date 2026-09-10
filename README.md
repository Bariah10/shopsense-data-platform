# ShopSense — A Real-Time Lakehouse and RAG Assistant for E‑Commerce Support

An end-to-end data platform that takes raw order events from a live Kafka broker, curates them
into a Delta Lake medallion architecture, and serves a retrieval-augmented support assistant on
top of the company knowledge base — with a quality gate and lineage wrapped around the whole run.

> **Training program:** Modern Data Engineering for AI Systems — SDAIA Academy (delivered via Learning Space)
> **Cohort / session dates:** `<FILL IN: e.g. 7–11 September 2026>` · **Trainer:** Mohammed Albeladi
> **Trainee:** `<FILL IN: your full name>`
> SDAIA Academy on GitHub: <https://github.com/SDAIAAcademy>

---

## 1. The problem

A mid-sized Saudi e-commerce retailer has two problems that look unrelated but share one root cause:

1. **Nobody trusts the order numbers.** Order events arrive continuously from the checkout service.
   Some of them are broken — a missing customer id, a negative quantity, a currency the finance team
   does not support. Today those bad records either crash the nightly job or, worse, land silently in
   the warehouse and quietly distort revenue reporting.
2. **Support agents cannot find answers.** Return windows, shipping SLAs, warranty terms and payment
   rules live in a dozen scattered documents. Agents guess, and the guesses are inconsistent.

Both problems are data-engineering problems. ShopSense solves them with one pipeline.

## 2. What this project does

| Stage | What happens |
|---|---|
| **Ingestion** | A Kafka producer emits order events onto `orders.raw`. A consumer validates every message against a Pydantic **data contract** at the ingestion boundary. Valid records go to `orders.valid` and to the Bronze landing zone; invalid records go to the **dead-letter topic** `orders.dlq` and a quarantine folder, each carrying the reason it was rejected. |
| **Bronze** | Validated events landed in Delta Lake exactly as received, plus ingestion audit columns. Append-only, replayable. |
| **Silver** | Typed, cleaned, one row per `order_id`. Maintained with a real Delta **MERGE (upsert)** on that business key, so late-arriving status updates correct the existing row instead of duplicating it. Schema enforcement refuses any write that does not match the table. |
| **Gold** | Genuine aggregates at a different grain from Silver: daily revenue by category and city, and per-customer lifetime value with segment labels. |
| **RAG** | The knowledge base is chunked, embedded, and indexed in ChromaDB. Retrieval is **hybrid** — dense vector search plus BM25, fused with **Reciprocal Rank Fusion** — then reordered by a **cross-encoder reranker**. Answers are grounded in retrieved text, carry citations, and the assistant refuses to answer when confidence is below the floor. |
| **Quality + lineage** | Great Expectations suites gate each layer. `gate()` raises on failure, so bad data cannot reach the next stage. Every stage emits OpenLineage `START` / `COMPLETE` / `FAIL` events, with the failure reason carried in an `errorMessage` facet. |
| **Orchestration** | An Airflow DAG wires all of it together: `ingest → gate → silver → gate → (gold ‖ rag refresh) → complete`. A run against a deliberately corrupted batch fails the bronze gate and leaves every downstream task `upstream_failed` — proof the gate halts the pipeline rather than just logging a warning. |

## 3. Scope

**In scope:** the full batch/streaming path from event ingestion to a queryable Gold layer and a
grounded question-answering interface over the support knowledge base, with data quality enforced at
two points (the ingestion contract and the pre-Gold expectation suite).

**Out of scope:** a production UI, multi-node cluster deployment, authentication, and real customer
data. Order events are synthetic; the knowledge base is written for this project. Everything runs in
a single Google Colab session so it can be reproduced by anyone with a browser.

## 4. Architecture

```
 checkout service (simulated)
            │
            ▼
   ┌──────────────────┐        invalid + reason
   │  Kafka 3.7 KRaft │──────────────────────────────► orders.dlq  ──►  data/quarantine/
   │   orders.raw     │
   └────────┬─────────┘
            │ Pydantic data contract (OrderEvent)
            ▼ valid
      orders.valid  ──►  data/bronze_landing/*.jsonl
            │
            ▼
   ┌────────────────────────── Delta Lake ──────────────────────────┐
   │  BRONZE  as-received, append-only                              │
   │     │  dedupe on order_id, cast, derive line_total             │
   │     ▼                                                          │
   │  SILVER  MERGE (upsert) on order_id · schema enforcement       │
   │     │  aggregate                                               │
   │     ▼                                                          │
   │  GOLD    daily_category_revenue · customer_segments            │
   └────────────────────────────────────────────────────────────────┘
            │
            ▼
   knowledge base (markdown)
            │ chunk → embed (all-MiniLM-L6-v2) → ChromaDB
            ▼
   dense kNN ─┐
              ├─► Reciprocal Rank Fusion ─► cross-encoder rerank ─► grounded answer + citations
   BM25     ─┘
```

Full component-by-component detail, including configuration and environment variables, is in
[`docs/architecture.md`](docs/architecture.md).

## 5. Repository structure

```
.
├── README.md
├── .gitignore
├── docs/
│   ├── architecture.md          # pipeline overview, components, configuration
│   └── github_guide_ar.md       # الدليل العربي: GitHub من الصفر + الرفع من Colab
├── notebooks/
│   ├── 00_push_to_github.ipynb  # helper: commit and push from Colab
│   ├── 01_ingestion_kafka.ipynb # Kafka producer/consumer + data contract + DLQ
│   ├── 02_delta_lakehouse.ipynb # Bronze / Silver (MERGE) / Gold + schema enforcement
│   └── 03_rag_pipeline.ipynb    # chunking, embeddings, hybrid search, RRF, reranking
├── data/
│   ├── contracts/               # the JSON schema of the ingestion contract
│   ├── knowledge_base/          # the RAG corpus (12 markdown documents)
│   └── quarantine/              # rejected records + rejection reasons (sample committed)
└── reports/                     # per-stage JSON run reports (the evidence of a real run)
```

Large generated artefacts — Delta table files and the Chroma vector store — are **not** committed.
They are rebuilt by running the notebooks, and they live in Google Drive between sessions. See
`.gitignore`.

## 6. Prerequisites

| Requirement | Notes |
|---|---|
| Google account with Google Drive | the notebooks mount Drive at `/content/drive` |
| Google Colab | free tier is enough; no GPU required |
| ~2 GB free space in Drive | Delta tables, the vector store, and the model cache |
| A GitHub account | to publish the repository |

No local installation is needed. Every library is installed by the first cells of each notebook:

```
kafka-python >= 2.2         pydantic >= 2.7          faker
pyspark == 3.5.3            delta-spark == 3.3.0     deltalake >= 1.0
sentence-transformers       chromadb >= 1.0          rank-bm25
apache-airflow == 3.1.8     great-expectations == 1.22.0
openlineage-python == 1.53.0
```

Apache Kafka 3.7.1 is downloaded and started **inside the Colab VM** in KRaft mode — a real broker,
not a simulation.

## 7. How to run

Run the notebooks **in order**. Each one writes its output to Google Drive, and the next one reads it
from there.

1. **Open `notebooks/01_ingestion_kafka.ipynb` in Colab** → `Runtime` → `Run all`.
   Takes about 5 minutes, most of it waiting for the Kafka broker to come up.
2. **Open `notebooks/02_delta_lakehouse.ipynb`** → `Runtime` → `Run all`.
   About 6 minutes; the Delta JARs are downloaded from Maven the first time the Spark session starts.
3. **Open `notebooks/03_rag_pipeline.ipynb`** → `Runtime` → `Run all`.
   About 4 minutes, most of it downloading the embedding and reranker models.
4. **Open `notebooks/04_quality_gate_lineage.ipynb`** → `Runtime` → `Run all`.
   About 3 minutes. Writes `src/` — notebook 05 imports it.
5. **Open `notebooks/05_airflow_orchestration.ipynb`** → `Runtime` → `Run all`.
   About 10 minutes; installing Airflow is the slow part.

> Airflow 2.x does not support Python 3.13, which is what Colab runs, so the DAG targets
> **Airflow 3.1.8** with the official constraint file for the running interpreter.

Then `File` → `Save`, and push the executed notebooks (see
[`docs/github_guide_ar.md`](docs/github_guide_ar.md) or `notebooks/00_push_to_github.ipynb`).

> **Save the outputs.** The rubric is graded on evidence of a real run, so the notebooks must be
> committed *with* their cell output, not cleared.

### Shared project folder

All three notebooks read and write under:

```
Google Drive / MyDrive / sdaia_capstone /
├── data/
│   ├── contracts/order_event.schema.json
│   ├── bronze_landing/orders_valid_<run_id>.jsonl
│   ├── quarantine/orders_rejected_<run_id>.jsonl
│   └── knowledge_base/*.md
├── lakehouse/{bronze,silver,gold}/…          (Delta tables)
├── vector_store/                             (ChromaDB)
└── reports/*.json
```

## 8. Output from the committed run

These are the actual figures captured in the notebooks in this repository, not an illustrative
example. The event generator is random, so a fresh run will differ.

**Notebook 01 — ingestion**

```
consumed          : 425
accepted (bronze) : 347   (81.7%)
rejected  (dlq)   :  78   (18.3%)

rejections by rule: status 13 · currency 12 · unit_price 10 · record 10
                    category 7 · customer_email 7 · customer_id 7
                    discount_hack 6 · quantity 6
messages sitting in orders.dlq: 78
```

**Notebook 02 — lakehouse**

```
bronze rows : 347      distinct order_id: 322
silver rows after batch 1 : 225
silver rows after MERGE   : 322      distinct order_id: 322

MERGE metrics:  numTargetRowsInserted 97 · numTargetRowsUpdated 15 · numSourceRows 322

[extra_undeclared_column] write REFUSED by Delta
    [_LEGACY_ERROR_TEMP_DELTA_0007] A schema mismatch detected when writing to the Delta table
[wrong_type_quantity]     write REFUSED by Delta
    [DELTA_FAILED_TO_MERGE_FIELDS] Failed to merge fields 'quantity' and 'quantity'
silver rows after the two bad writes : 322   <- unchanged

silver grain : one row per order      -> 322 rows
gold   grain : date x category x city -> 166 rows
gold   grain : one row per customer   -> 112 rows
```

**Notebook 03 — RAG**

```
12 documents -> 24 chunks     embedding dim: 384     vectors stored: 24
```

The refusal threshold is calibrated at run time (section 8.1) rather than hard-coded, and the
four-way retrieval comparison over 18 labelled questions is reported on Hit@1, Hit@3 and MRR in
section 9. The measured table for the committed run is exported to
[`reports/rag_eval_table.md`](reports/rag_eval_table.md) so this README cannot drift out of step
with it. Read Hit@1: over a 12-document corpus Hit@3 saturates and cannot separate the
configurations.

**Notebook 04 — quality gate and lineage**

```
GATE PASSED on bronze: 8/8 expectations, 347 rows
GATE PASSED on silver: 6/6 expectations, 322 rows

Quality gate FAILED on bronze: 4/8 expectations failed
  - expect_column_values_to_not_be_null  on customer_id: 1 unexpected value(s)
  - expect_column_values_to_be_between   on quantity:    1 unexpected value(s)
  - expect_column_values_to_be_in_set    on currency:    1 unexpected value(s)
  - expect_column_values_to_be_in_set    on category:    1 unexpected value(s)
```

**Notebook 05 — orchestration**

```
DAG RUN 1  ->  success              DAG RUN 2  ->  failed
  ingest_orders        success        ingest_orders        success
  quality_gate_bronze  success        quality_gate_bronze  failed
  build_silver         success        build_silver         upstream_failed
  quality_gate_silver  success        quality_gate_silver  upstream_failed
  build_gold           success        build_gold           upstream_failed
  refresh_rag_index    success        refresh_rag_index    upstream_failed
  pipeline_complete    success        pipeline_complete    upstream_failed

tasks that never ran because the gate failed: 5
event types: {'START': 9, 'COMPLETE': 8, 'FAIL': 1}

kafka.orders.valid            --[ingest_orders]-->  lakehouse_dag.bronze.orders
lakehouse_dag.bronze.orders   --[build_silver]-->   lakehouse_dag.silver.orders
lakehouse_dag.silver.orders   --[build_gold]-->     lakehouse_dag.gold.daily_category_revenue
```

## 9. Rubric coverage

| # | Deliverable | Pts | Where |
|---|---|---|---|
| 1 | Ingestion — Kafka + schema contract + DLQ | 20 | `notebooks/01_ingestion_kafka.ipynb` |
| 2 | Delta lakehouse — Bronze/Silver/Gold + MERGE + schema enforcement | 25 | `notebooks/02_delta_lakehouse.ipynb` |
| 3 | RAG — chunking, embeddings, vector store, hybrid + RRF, reranking, citations | 25 | `notebooks/03_rag_pipeline.ipynb` |
| 4 | Orchestration — Airflow DAG halting on a failed gate | 15 | `notebooks/05_airflow_orchestration.ipynb`, `dags/shopsense_pipeline.py` |
| 5 | Quality gate + lineage — Great Expectations + OpenLineage | 15 | `notebooks/04_quality_gate_lineage.ipynb`, `src/quality.py`, `src/lineage.py` |

## 10. Attribution

Completed as the capstone project for **Modern Data Engineering for AI Systems**, delivered by
**SDAIA Academy** via Learning Space. Trainer: Mohammed Albeladi.
Cohort / session dates: `<FILL IN>`.

SDAIA Academy on GitHub: <https://github.com/SDAIAAcademy>

## 11. License

Released under the MIT License for educational use.
