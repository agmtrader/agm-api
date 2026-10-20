"""Dispatch/poll the fixed market job using the API's Google identity."""
import re

import google.auth
from google.auth.transport.requests import AuthorizedSession
import requests

from src.utils.exception import ServiceError

PARENT = 'projects/agm-datalake/locations/us-central1'
JOB = PARENT + '/jobs/agm-market-etl'


def _session():
    credentials, _ = google.auth.default(scopes=['https://www.googleapis.com/auth/cloud-platform'])
    return AuthorizedSession(credentials)


def _request(method, resource):
    try:
        # Never retry a dispatch: a timed-out request may already have started a job.
        response = getattr(_session(), method)(f'https://run.googleapis.com/v2/{resource}',
                                               timeout=20, **({'json': {}} if method == 'post' else {}))
        if not response.ok:
            raise ServiceError('Market job control unavailable; inspect executions before another dispatch.',
                               status_code=503, code='market_job_control_unavailable')
        value = response.json()
        if not isinstance(value, dict):
            raise ValueError('Invalid job response')
        return value
    except (requests.RequestException, ValueError) as exc:
        raise ServiceError('Market job outcome unavailable; inspect executions before another dispatch.',
                           status_code=503, code='market_job_outcome_unknown') from exc


def dispatch():
    operation = _request('post', JOB + ':run')
    prefix = PARENT + '/operations/'
    name = operation.get('name', '')
    if not name.startswith(prefix) or not re.fullmatch(r'[A-Za-z0-9_-]{1,128}', name[len(prefix):]):
        raise ServiceError('Dispatch accepted without a usable operation reference; inspect executions.',
                           status_code=503, code='market_job_outcome_unknown')
    return {'status': 'accepted', 'operation': name[len(prefix):],
            'status_url': '/etl/market_data?operation=' + name[len(prefix):]}, 202


def poll(operation_id):
    if not re.fullmatch(r'[A-Za-z0-9_-]{1,128}', operation_id):
        raise ServiceError('Invalid market operation reference', status_code=400, code='invalid_operation')
    operation = _request('get', PARENT + '/operations/' + operation_id)
    metadata = (operation.get('response') if operation.get('done') else None) or operation.get('metadata') or {}
    execution = metadata.get('name', '')
    # An operation from another job must not expose its result through this route.
    if not execution.startswith(JOB + '/executions/'):
        raise ServiceError('Operation does not identify a market execution', status_code=404, code='market_operation_not_found')
    result = {'status': 'running', 'operation': operation_id, 'execution': execution.rsplit('/', 1)[-1]}
    if operation.get('done'):
        completed = any(c.get('type') == 'Completed' and c.get('state') == 'CONDITION_SUCCEEDED'
                        for c in metadata.get('conditions', []))
        result['status'] = 'completed' if completed and not operation.get('error') else 'failed'
        # Cloud Run completion is distinct from the runner's durable pipeline overview.
        result['overview_uri'] = f'gs://agm-datalake-etl-runs/runs/{result["execution"]}/overview.json'
    return result
