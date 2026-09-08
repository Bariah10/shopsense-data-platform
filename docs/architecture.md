# Technical Documentation — ShopSense Pipeline

## 1. Pipeline overview

The platform is a five-stage pipeline. Stages 1–3 are implemented; stages 4–5 wrap them.

```
[1] Ingestion  →  [2] Bronze  →  [2] Silver  →  [2] Gold  →  [3] RAG index
       │                              │
       └── quarantine / DLQ           └── quality gate (day 5) halts the run before Gold
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
| Grounding | citations `[S1]…[Sn]` on every sentence + a confidence floor of 0.30 | the assistant refuses rather than hallucinating when the knowledge base does not cover the question |

**Evaluation.** Ten labelled questions, each tagged with the document that should be retrieved.
Four configurations are compared on Hit@3 and MRR: dense only, BM25 only, hybrid (RRF), and
hybrid + rerank. The table in section 9 of the notebook is the justification for the architecture —
it shows the fusion and the reranker each earn their place instead of being added because the rubric
asked for them.

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
| `CONFIDENCE_FLOOR` | 03 | `0.30` | below this, the assistant refuses to answer |

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

**Why hybrid retrieval instead of dense alone?** Section 9 of notebook 03 measures it. Dense retrieval
misses exact identifiers; BM25 misses paraphrases. The fusion beats both, and the reranker beats the
fusion.

## 5. Known limitations

* The Kafka broker, Spark session and vector store all live in one Colab VM. Nothing here is sized
  for concurrency or failure recovery across machines.
* Order events are synthetic and the knowledge base was written for this project.
* Gold tables are rebuilt in full on each run. That is fine at this volume and would not be at scale.
* Stages 4 and 5 (Airflow orchestration, Great Expectations, OpenLineage) are not yet implemented.
