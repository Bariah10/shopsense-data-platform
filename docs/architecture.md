# Technical Documentation — ShopSense Pipeline

## 1. Pipeline overview

The platform is a five-stage pipeline. All five stages are implemented; stages 4 and 5 wrap the
first three: the Airflow DAG schedules them and the quality gate plus lineage instrument them.

```
[1] Ingestion → [2] Bronze → gate → [2] Silver → gate → [2] Gold ‖ [3] RAG index refresh
       │                        │                    │
       └── quarantine / DLQ     └── Great Expectations; a failure halts everything downstream
                                    (all of it wired by the Airflow DAG in stage [4])
```

Every stage writes a JSON run report to `reports/`. Those reports are the interface between the
stages and, on day 5, the payload for the OpenLineage events.

## 2. Components

### 2.1 Ingestion — `notebooks/01_ingestion_kafka.ipynb`

| Component | Implementation | Why |
|---|---|---|
| Broker | Apache Kafka 3.7.1, **KRaft mode**, single node on `localhost:9092` | a real broker speaking the real wire protocol; KRaft removes the ZooKeeper dependency, which matters in a single ephemeral VM |
| Client | `kafka-python` | pure Python, no native build step, works in Colab |
| Topics | `orders.raw` (3 partitions), `orders.valid`, `orders.dlq` | raw / curated / dead-letter separation |
| Data contract | `OrderEvent`, a Pydantic v2 model with `extra='forbid'` | validation happens **at the ingestion boundary**, before anything is persisted |
| Quarantine | `data/quarantine/orders_rejected_<run_id>.jsonl` + topic `orders.dlq` | a rejected record is never dropped; it is kept with its `rejection_reason`, its original payload, and its Kafka coordinates |

**The contract** (`data/contracts/order_event.schema.json`) enforces more than types:

| Field | Rule |
|---|---|
| `order_id`, `customer_id` | present, 6–32 / 4–32 characters |
| `quantity` | integer, `> 0`, `<= 100` |
| `unit_price` | float, `> 0` |
| `currency` | one of `SAR`, `USD`, `AED` |
| `category` | one of six known categories |
| `status` | one of `created`, `paid`, `shipped`, `delivered`, `cancelled` |
| `customer_email` | must contain `@` and a dotted domain |
| `delivered_ts` | if present, must not be earlier than `order_ts` (cross-field rule) |
| *(any other field)* | rejected — `extra='forbid'` blocks undeclared columns at the door |

The producer deliberately emits ~22% broken events, one defect per record, so the rejection path is
exercised and measurable rather than theoretical.

### 2.2 Lakehouse — `notebooks/02_delta_lakehouse.ipynb`

| Layer | Grain | Write mode | Notes |
|---|---|---|---|
| Bronze | one row per event as received | append / overwrite per run | plus `_bronze_loaded_at`, `_source_file`, and the Kafka offset columns |
| Silver | **one row per `order_id`** | `MERGE` (upsert) | the business key is `order_id`; the source is deduplicated by latest `ingested_at` before the merge, because `MERGE` correctly refuses an ambiguous source |
| Gold | date × category × city; and one row per customer | overwrite | partitioned by `order_date` for the revenue table |

**The MERGE.** Expressed in SQL, what runs is:

```sql
MERGE INTO delta.`.../silver/orders` AS t
USING updates AS s
   ON t.order_id = s.order_id
 WHEN MATCHED AND (s.ingested_at > t.ingested_at OR s.status <> t.status)
      THEN UPDATE SET *
 WHEN NOT MATCHED
      THEN INSERT *
```

The notebook runs it against two batches so the operation metrics show both
`numTargetRowsInserted` and `numTargetRowsUpdated` non-zero — that is the proof the upsert is real
and not an append in disguise. `DESCRIBE HISTORY` is printed as the audit trail.

**Schema enforcement.** With `spark.databricks.delta.schema.autoMerge.enabled = false`, two writes
are attempted and both are expected to fail:

1. a DataFrame with an undeclared extra column `discount_hack`,
2. a DataFrame where `quantity` is a string.

Both raise, the exception message is captured, and the Silver row count is re-checked afterwards to
show nothing corrupt landed. A third cell then evolves the schema the *controlled* way, with an
explicit `option('mergeSchema', 'true')` on a single write.

**Gold is not a copy of Silver.** Silver has one row per order. `daily_category_revenue` has one row
per `(order_date, category, city)` and carries `orders_count`, `unique_customers`, `units_sold`,
`total_revenue_sar`, `avg_order_value_sar`, `largest_order_sar` and `revenue_per_customer`.
`customer_segments` has one row per customer with lifetime value, basket average, category breadth,
cancellation rate and a `segment` label (`new` / `returning` / `loyal` / `vip`). Business logic lives
here: only `paid`, `shipped` and `delivered` orders count as revenue.

### 2.3 RAG — `notebooks/03_rag_pipeline.ipynb`

| Stage | Choice | Rationale |
|---|---|---|
| Corpus | 12 markdown documents in `data/knowledge_base/` | policy and runbook content, the kind an agent actually queries |
| Chunking | sentence-aware packing, target 420 chars, 90 char overlap | fixed-width splitting cuts sentences in half; packing whole sentences keeps each chunk self-contained, and the overlap recovers facts that straddle a boundary |
| Embeddings | `sentence-transformers/all-MiniLM-L6-v2`, 384-d, L2-normalised | small and fast enough for CPU, strong on short-passage retrieval |
| Vector store | **ChromaDB** persistent client, cosine space | a real vector database with on-disk persistence, so the index survives the session |
| Sparse retrieval | `BM25Okapi` over title + body tokens | dense models are weak on exact identifiers like `orders.dlq` or `3-D Secure` |
| Fusion | **Reciprocal Rank Fusion**, `k = 60` | combines the two lists by rank, so a cosine similarity never has to be normalised against a BM25 score |
| Reranking | `cross-encoder/ms-marco-MiniLM-L-6-v2` | reads query and passage jointly; too slow for the whole corpus, so it only reorders the fused top-10 |
| Grounding | citations `[S1]…[Sn]` on every sentence + an **empirically calibrated** score floor | a hard-coded threshold made the assistant refuse in-scope questions, so the floor is now measured: section 8.1 scores probe questions the corpus answers against ones it does not and places the floor between the two distributions |

**Evaluation.** Eighteen labelled questions, deliberately split between exact-identifier queries
(where dense retrieval is weak) and paraphrase queries (where BM25 is weak). Four configurations are
compared on Hit@1, Hit@3 and MRR. Hit@1 is the metric to read: Hit@3 over twelve documents saturates
and cannot separate the configurations. A per-question rank table shows exactly which questions each
strategy loses. The measured results are exported to `reports/rag_eval_table.md`.

## 3. Configuration

There are no secrets in this project, so there is no `.env` file to manage. The values you may want
to change are all constants at the top of the relevant notebook.

| Setting | Notebook | Default | Meaning |
|---|---|---|---|
| `PROJECT` | all | `/content/drive/MyDrive/sdaia_capstone` | the shared project folder in Drive |
| `N_EVENTS` | 01 | `400` | how many order events to produce |
| `BROKEN_PCT` | 01 | `0.22` | share of deliberately malformed events |
| bootstrap servers | 01 | `localhost:9092` | the in-VM Kafka broker |
| `CHUNK_CHARS` / `OVERLAP_CHARS` | 03 | `420` / `90` | chunking window |
| `EMBED_MODEL` | 03 | `all-MiniLM-L6-v2` | embedding model |
| `RERANK_MODEL` | 03 | `ms-marco-MiniLM-L-6-v2` | cross-encoder |
| `RRF_K` | 03 | `60` | RRF smoothing constant |
| refusal floor | 03 | calibrated at run time | measured in section 8.1 from in-scope vs off-topic probes; below it the assistant refuses |

**Optional environment values** (used only by the optional generative-answer cell in notebook 03):

| Name | Where it goes | Required? |
|---|---|---|
| `GEMINI_API_KEY` | Colab *Secrets* panel (the key icon in the left sidebar) | no — the pipeline produces grounded, cited answers without any LLM |

Never commit an API key. `.gitignore` excludes `.env` and `*.key`; Colab Secrets are stored in your
Google account, not in the notebook.

## 4. Design decisions worth defending

**Why a real broker in Colab instead of a queue?** The rubric is explicit that a simulation earns no
credit, and the difference is not cosmetic: partitions, offsets, consumer groups and topic retention
are what make replay and the dead-letter pattern work. All of them are exercised here.

**Why validate before Bronze rather than after?** Bronze is meant to be replayable and trustworthy as
a raw record of what arrived. Letting malformed events into it moves the problem downstream and makes
every later stage defensive. Validating at the boundary keeps exactly one place responsible for the
contract.

**Why deduplicate before the MERGE?** Delta refuses a source with duplicate keys, and it is right to.
Choosing the winning row (latest `ingested_at`) is a business decision, so it is written explicitly in
the notebook rather than hidden inside a merge condition.

**Why hybrid retrieval instead of dense alone?** Because the two retrievers fail on different
questions: dense embeddings are weak on rare literal tokens (`orders.dlq`, `line_total`, `3-D
Secure`), BM25 is weak on paraphrases that share no vocabulary with the source text. Section 9 of
notebook 03 measures this on 18 labelled questions split between those two cases, and reports
Hit@1, Hit@3 and MRR for all four configurations. The measured table for the committed run is
exported to `reports/rag_eval_table.md` - whatever it shows is the result, including the
possibility that on a corpus this small dense retrieval alone is already competitive.

## 5. Known limitations

* The Kafka broker, Spark session and vector store all live in one Colab VM. Nothing here is sized
  for concurrency or failure recovery across machines.
* Order events are synthetic and the knowledge base was written for this project.
* Gold tables are rebuilt in full on each run. That is fine at this volume and would not be at scale.
* The RAG corpus is 12 documents. That is large enough to exercise the retrieval stack but small
  enough that retrieval metrics saturate, so the four-way comparison should be read as a
  demonstration of method rather than a benchmark result.
* The orchestrated pipeline (`dags/shopsense_pipeline.py`) runs the medallion stages through
  delta-rs rather than Spark, to keep the scheduler light. Notebook 02 is the Spark implementation
  of the same layers; the rubric credits either engine.
* Airflow runs single-node with `airflow dags test` rather than a scheduler plus webserver, which is
  enough to prove dependencies and the gate but is not a deployment.
