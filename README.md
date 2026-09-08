# ShopSense — A Real-Time Lakehouse and RAG Assistant for E‑Commerce Support

An end-to-end data platform that takes raw order events from a live Kafka broker, curates them
into a Delta Lake medallion architecture, and serves a retrieval-augmented support assistant on
top of the company knowledge base — with a quality gate and lineage wrapped around the whole run.

> **Training program:** Modern Data Engineering for AI Systems — SDAIA Academy (delivered via Learning Space)
> **Cohort / session dates:** `6–10 September 2026` · **Trainer:** Mohammed Albeladi
> **Trainee:** `Bariah Altayar`
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
| **Orchestration** *(day 4)* | An Airflow DAG wires the stages together so a failed quality gate halts the run before downstream stages execute. |
| **Quality + lineage** *(day 5)* | Great Expectations suites that actually gate the pipeline, and OpenLineage `START` / `COMPLETE` / `FAIL` events per stage. |

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
kafka-python >= 2.2      pydantic >= 2.7        faker
pyspark == 3.5.3         delta-spark == 3.3.0
sentence-transformers    chromadb >= 1.0        rank-bm25
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

## 8. Expected output

**Notebook 01 — ingestion**

```
consumed          : 425
accepted (bronze) : 331   (77.9%)
rejected  (dlq)   :  94   (22.1%)

rejections by field / rule:
    14  category
    12  currency
    11  quantity
    ...
messages sitting in orders.dlq: 94
```
Plus a printed sample of rejected payloads, each with the rule it broke.

**Notebook 02 — lakehouse**

```
bronze rows : 331     distinct order_id: 306
silver rows after batch 1 : 214
silver rows after MERGE   : 306   distinct order_id: 306

MERGE metrics:
  numTargetRowsInserted   92
  numTargetRowsUpdated    17

[extra_undeclared_column] write REFUSED by Delta
[wrong_type_quantity]     write REFUSED by Delta
silver rows after the two bad writes : 306   <- unchanged

silver grain : one row per order      -> 306 rows
gold   grain : date x category x city ->  88 rows
```

**Notebook 03 — RAG**

```
12 documents -> 24 chunks     embedding dim: 384     vectors stored: 24

strategy           Hit@3   MRR
dense only         0.800  0.717
bm25 only          0.700  0.633
hybrid (RRF)       0.900  0.808
hybrid + rerank    1.000  0.950
```
Plus grounded answers with `[S1] [S2]` citations and a source list, and a refusal for an
out-of-scope question.

*(Exact numbers vary run to run — the event generator is random.)*

## 9. Rubric coverage

| # | Deliverable | Pts | Where |
|---|---|---|---|
| 1 | Ingestion — Kafka + schema contract + DLQ | 20 | `notebooks/01_ingestion_kafka.ipynb` |
| 2 | Delta lakehouse — Bronze/Silver/Gold + MERGE + schema enforcement | 25 | `notebooks/02_delta_lakehouse.ipynb` |
| 3 | RAG — chunking, embeddings, vector store, hybrid + RRF, reranking, citations | 25 | `notebooks/03_rag_pipeline.ipynb` |
| 4 | Orchestration — Airflow DAG | 15 | *in progress* |
| 5 | Quality gate + lineage — Great Expectations + OpenLineage | 15 | *in progress* |

## 10. Attribution

Completed as the capstone project for **Modern Data Engineering for AI Systems**, delivered by
**SDAIA Academy** via Learning Space. Trainer: Mohammed Albeladi.
Cohort / session dates: `<FILL IN>`.

SDAIA Academy on GitHub: <https://github.com/SDAIAAcademy>

## 11. License

Released under the MIT License for educational use.
