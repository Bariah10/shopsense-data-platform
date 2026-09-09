# ShopSense end-to-end pipeline.
# Ingestion -> Bronze -> quality gate -> Silver -> quality gate -> Gold + RAG refresh.
# A failing gate raises, so Airflow marks every downstream task upstream_failed.
import os
import sys

import pendulum
from airflow.sdk import DAG
from airflow.providers.standard.operators.python import PythonOperator

PROJECT = os.environ.get('SHOPSENSE_PROJECT', '/content/drive/MyDrive/sdaia_capstone')
if PROJECT not in sys.path:
    sys.path.insert(0, PROJECT)

from src import transforms, quality, lineage      # noqa: E402

DS_KAFKA  = 'kafka.orders.valid'
DS_BRONZE = 'lakehouse_dag.bronze.orders'
DS_SILVER = 'lakehouse_dag.silver.orders'
DS_GOLD   = 'lakehouse_dag.gold.daily_category_revenue'
DS_INDEX  = 'rag.knowledge_base_index'


def ingest_orders(**_):
    override = os.environ.get('SHOPSENSE_LANDING_OVERRIDE') or None
    with lineage.stage('ingest_orders', inputs=[DS_KAFKA], outputs=[DS_BRONZE]):
        info = transforms.ingest_to_bronze(override)
    print('ingested:', info)
    return info


def quality_gate_bronze(**_):
    with lineage.stage('quality_gate_bronze', inputs=[DS_BRONZE]):
        summary = quality.gate(transforms.read_bronze(), 'bronze')
    print(f"bronze gate PASSED: {summary['passed']}/{summary['evaluated']} expectations "
          f"over {summary['rows']} rows")
    return {k: summary[k] for k in ('rows', 'evaluated', 'passed', 'failed')}


def build_silver(**_):
    with lineage.stage('build_silver', inputs=[DS_BRONZE], outputs=[DS_SILVER]):
        info = transforms.build_silver()
    print('silver:', info)
    return info


def quality_gate_silver(**_):
    with lineage.stage('quality_gate_silver', inputs=[DS_SILVER]):
        summary = quality.gate(transforms.read_silver(), 'silver')
    print(f"silver gate PASSED: {summary['passed']}/{summary['evaluated']} expectations "
          f"over {summary['rows']} rows")
    return {k: summary[k] for k in ('rows', 'evaluated', 'passed', 'failed')}


def build_gold(**_):
    with lineage.stage('build_gold', inputs=[DS_SILVER], outputs=[DS_GOLD]):
        info = transforms.build_gold()
    print('gold:', info)
    return info


def refresh_rag_index(**_):
    with lineage.stage('refresh_rag_index', outputs=[DS_INDEX]):
        info = transforms.refresh_rag_index()
    print('rag index:', info)
    return info


def pipeline_complete(**_):
    with lineage.stage('pipeline_complete', inputs=[DS_GOLD, DS_INDEX]):
        print('all stages completed and both quality gates passed')
    return 'ok'


with DAG(
    dag_id='shopsense_pipeline',
    description='Kafka -> Delta medallion -> RAG refresh, gated by Great Expectations',
    start_date=pendulum.datetime(2026, 9, 1, tz='UTC'),
    schedule='@daily',
    catchup=False,
    max_active_runs=1,
    default_args={'owner': 'shopsense', 'retries': 0},
    tags=['sdaia', 'capstone', 'medallion', 'rag'],
) as dag:

    t_ingest = PythonOperator(task_id='ingest_orders',       python_callable=ingest_orders)
    t_gate_b = PythonOperator(task_id='quality_gate_bronze', python_callable=quality_gate_bronze)
    t_silver = PythonOperator(task_id='build_silver',        python_callable=build_silver)
    t_gate_s = PythonOperator(task_id='quality_gate_silver', python_callable=quality_gate_silver)
    t_gold   = PythonOperator(task_id='build_gold',          python_callable=build_gold)
    t_rag    = PythonOperator(task_id='refresh_rag_index',   python_callable=refresh_rag_index)
    t_done   = PythonOperator(task_id='pipeline_complete',   python_callable=pipeline_complete)

    t_ingest >> t_gate_b >> t_silver >> t_gate_s >> [t_gold, t_rag] >> t_done
