# Medallion transformations for the orchestrated pipeline (delta-rs implementation).
import json
import os
from pathlib import Path
import pandas as pd
from deltalake import DeltaTable, write_deltalake

PROJECT   = Path('/content/drive/MyDrive/sdaia_capstone')
# Delta tables are written to LOCAL disk. delta-rs commits by atomic rename, and the
# Google Drive FUSE mount does not support rename -> "Operation not permitted (os error 1)".
# Nothing is lost: every DAG run rebuilds Bronze from the landing file in Drive.
LAKE      = Path(os.environ.get('SHOPSENSE_LAKE', '/content/lakehouse_dag'))
BRONZE    = str(LAKE / 'bronze' / 'orders')
SILVER    = str(LAKE / 'silver' / 'orders')
GOLD      = str(LAKE / 'gold' / 'daily_category_revenue')
REVENUE_STATUSES = ['paid', 'shipped', 'delivered']


def latest_landing_file(override: str | None = None) -> Path:
    # An override lets the notebook point the DAG at a deliberately corrupted file
    # so the failure path can be demonstrated.
    if override:
        return Path(override)
    files = sorted((PROJECT / 'data' / 'bronze_landing').glob('orders_valid_*.jsonl'))
    if not files:
        raise FileNotFoundError('no bronze landing file - run notebook 01 first')
    return files[-1]


def read_bronze() -> pd.DataFrame:
    return DeltaTable(BRONZE).to_pandas()


def read_silver() -> pd.DataFrame:
    return DeltaTable(SILVER).to_pandas()


def ingest_to_bronze(landing_override: str | None = None) -> dict:
    src = latest_landing_file(landing_override)
    df = pd.read_json(src, lines=True)
    LAKE.mkdir(parents=True, exist_ok=True)
    write_deltalake(BRONZE, df, mode='overwrite', schema_mode='overwrite')
    return {'source_file': src.name, 'rows': int(len(df)),
            'distinct_order_id': int(df['order_id'].nunique())}


def to_silver_shape(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out['order_ts']    = pd.to_datetime(out['order_ts'], format='mixed', utc=True)
    out['ingested_at'] = pd.to_datetime(out['ingested_at'], format='mixed', utc=True)
    out['order_date']  = out['order_ts'].dt.date.astype(str)
    out['line_total']  = (out['quantity'] * out['unit_price']).round(2)
    out['is_revenue']  = out['status'].isin(REVENUE_STATUSES)
    out['city']        = out['city'].str.strip().str.title()
    out['category']    = out['category'].str.strip().str.lower()
    cols = ['order_id','customer_id','customer_email','product_id','product_name','category',
            'quantity','unit_price','line_total','currency','city','status','is_revenue',
            'order_date','ingested_at']
    return out[cols]


def build_silver(landing_override: str | None = None) -> dict:
    shaped = to_silver_shape(read_bronze())
    # MERGE refuses an ambiguous source, so pick a winner per business key first:
    # the most recently ingested version of each order.
    deduped = (shaped.sort_values('ingested_at')
                     .drop_duplicates('order_id', keep='last')
                     .reset_index(drop=True))
    deduped['ingested_at'] = deduped['ingested_at'].dt.tz_localize(None)

    if not Path(SILVER, '_delta_log').exists():
        write_deltalake(SILVER, deduped, mode='overwrite', schema_mode='overwrite')
        return {'created': True, 'rows': int(len(deduped)),
                'inserted': int(len(deduped)), 'updated': 0}

    dt = DeltaTable(SILVER)
    metrics = (dt.merge(source=deduped,
                        predicate='target.order_id = source.order_id',
                        source_alias='source', target_alias='target')
                 .when_matched_update_all()
                 .when_not_matched_insert_all()
                 .execute())
    return {'created': False, 'rows': int(len(DeltaTable(SILVER).to_pandas())),
            'inserted': int(metrics.get('num_target_rows_inserted', 0)),
            'updated':  int(metrics.get('num_target_rows_updated', 0))}


def build_gold() -> dict:
    silver = read_silver()
    revenue = silver[silver['is_revenue']]
    gold = (revenue.groupby(['order_date', 'category', 'city'], as_index=False)
                   .agg(orders_count=('order_id', 'nunique'),
                        unique_customers=('customer_id', 'nunique'),
                        units_sold=('quantity', 'sum'),
                        total_revenue_sar=('line_total', 'sum'),
                        avg_order_value_sar=('line_total', 'mean')))
    gold['total_revenue_sar']   = gold['total_revenue_sar'].round(2)
    gold['avg_order_value_sar'] = gold['avg_order_value_sar'].round(2)
    write_deltalake(GOLD, gold, mode='overwrite', schema_mode='overwrite')
    return {'silver_rows': int(len(silver)), 'gold_rows': int(len(gold)),
            'total_revenue_sar': float(gold['total_revenue_sar'].sum())}


def refresh_rag_index() -> dict:
    # Re-chunks the knowledge base and writes a manifest. The embedding refresh itself
    # lives in notebook 03; this task proves the stage is wired into the DAG.
    import re
    docs = sorted((PROJECT / 'data' / 'knowledge_base').glob('*.md'))
    if not docs:
        raise FileNotFoundError('knowledge base is empty - run notebook 03 first')
    sent = re.compile(r'(?<=[.!?])\s+')
    manifest, total = [], 0
    for d in docs:
        text = re.sub(r'\s+', ' ', d.read_text()).strip()
        chunks, cur = [], ''
        for s in [x.strip() for x in sent.split(text) if x.strip()]:
            if cur and len(cur) + len(s) + 1 > 420:
                chunks.append(cur); cur = s
            else:
                cur = (cur + ' ' + s).strip()
        if cur:
            chunks.append(cur)
        manifest.append({'doc': d.name, 'chunks': len(chunks)})
        total += len(chunks)
    out = PROJECT / 'reports' / 'rag_index_manifest.json'
    out.write_text(json.dumps({'documents': len(docs), 'chunks': total,
                               'per_document': manifest}, indent=2))
    return {'documents': len(docs), 'chunks': total}
