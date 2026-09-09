# OpenLineage emission for every ShopSense pipeline stage.
import os
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path

from openlineage.client import OpenLineageClient
from openlineage.client.transport.file import FileConfig, FileTransport
from openlineage.client.event_v2 import (RunEvent, RunState, Run, Job,
                                         InputDataset, OutputDataset)
from openlineage.client.uuid import generate_new_uuid
from openlineage.client.facet_v2 import job_type_job, error_message_run

NAMESPACE  = 'shopsense'
PRODUCER   = 'https://github.com/Bariah10/shopsense-data-platform'
EVENTS_LOG = os.environ.get(
    'SHOPSENSE_LINEAGE_LOG',
    '/content/drive/MyDrive/sdaia_capstone/reports/lineage_events.jsonl')


def _client():
    Path(EVENTS_LOG).parent.mkdir(parents=True, exist_ok=True)
    return OpenLineageClient(
        transport=FileTransport(FileConfig(log_file_path=EVENTS_LOG, append=True)))


def emit(state, job_name, run_id, inputs=(), outputs=(), error=None):
    facets = {}
    if error is not None:
        facets['errorMessage'] = error_message_run.ErrorMessageRunFacet(
            message=str(error)[:2000], programmingLanguage='PYTHON')
    event = RunEvent(
        eventType=state,
        eventTime=datetime.now(timezone.utc).isoformat(),
        run=Run(runId=str(run_id), facets=facets),
        job=Job(namespace=NAMESPACE, name=job_name, facets={
            'jobType': job_type_job.JobTypeJobFacet(
                processingType='BATCH', integration='AIRFLOW', jobType='TASK')}),
        inputs=[InputDataset(namespace=NAMESPACE, name=n) for n in inputs],
        outputs=[OutputDataset(namespace=NAMESPACE, name=n) for n in outputs],
        producer=PRODUCER,
    )
    _client().emit(event)
    return event


@contextmanager
def stage(job_name, inputs=(), outputs=(), run_id=None):
    # START on entry, COMPLETE on success, FAIL on exception - then re-raise so
    # Airflow still marks the task failed and halts everything downstream.
    rid = run_id or generate_new_uuid()
    emit(RunState.START, job_name, rid, inputs, outputs)
    try:
        yield rid
    except Exception as exc:
        emit(RunState.FAIL, job_name, rid, inputs, outputs, error=exc)
        raise
    else:
        emit(RunState.COMPLETE, job_name, rid, inputs, outputs)


def read_events(path=None):
    import json
    p = Path(path or EVENTS_LOG)
    if not p.exists():
        return []
    return [json.loads(line) for line in p.read_text().splitlines() if line.strip()]
