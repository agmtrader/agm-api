import json
import os
import subprocess
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock

import pytest
import yaml
from flask import Flask, g
from flask_jwt_extended import JWTManager, create_access_token
from src.app.tools.private.actions import bp
from src.components.tools.private import daily_agreements
from src.utils import authz

ROOT=Path(__file__).resolve().parents[1]
WORKFLOW=yaml.safe_load((ROOT/'.github/workflows/daily_agreements.yaml').read_text())
STEPS=WORKFLOW['jobs']['prepare']['steps']


def preparation():
    numbers=daily_agreements.DAILY_AGREEMENT_FORM_NUMBERS
    return {'ok':False,'ready':True,'submitted':False,'masterAccount':'I6413690','mode':'preparation',
            'formsRequested':len(numbers),'documentsPrepared':len(numbers),'accepted':0,'rejected':0,'skipped':0,
            'expectedForms':numbers,'results':[{'formNumber':str(n),'status':'Prepared'} for n in numbers]}


def submission():
    return {**preparation(), 'ok': True, 'submitted': True, 'mode': 'submission', 'accepted': len(daily_agreements.DAILY_AGREEMENT_FORM_NUMBERS),
            'results': [{'formNumber': str(n), 'status': 'Accepted'} for n in daily_agreements.DAILY_AGREEMENT_FORM_NUMBERS]}


def execute(tmp_path,payload,*,http='200',token_http='200',transport=False,credentials=True,token_body=None):
    binaries=tmp_path/'bin';binaries.mkdir()
    curl=binaries/'curl'
    curl.write_text('#!/usr/bin/env python3\nimport os,sys\n'
                    'with open(os.environ["CALLS"],"a") as f:f.write(sys.argv[-1]+"\\n")\n'
                    'if sys.argv[-1].endswith("/token"):print(os.environ["TOKEN_BODY"]+"\\n"+os.environ["TOKEN_HTTP"])\n'
                    'elif os.environ["TRANSPORT"]=="1":sys.exit(28)\n'
                    'else:print(os.environ["BODY"]+"\\n"+os.environ["HTTP"])\n')
    curl.chmod(0o700)
    env={**os.environ,'PATH':str(binaries)+os.pathsep+os.environ['PATH'],
         'AGM_API_EMAIL':'synthetic@example.test' if credentials else '', 'AGM_API_PASSWORD':'synthetic',
         'AGREEMENT_MASTER':'I6413690','RUNNER_TEMP':str(tmp_path),
         'GITHUB_OUTPUT':str(tmp_path/'output'),'GITHUB_STEP_SUMMARY':str(tmp_path/'summary'),
         'CALLS':str(tmp_path/'calls'),'TOKEN_BODY':json.dumps(token_body if token_body is not None else {'access_token':'synthetic-token'}),
         'TOKEN_HTTP':token_http,'BODY':json.dumps(payload),'HTTP':http,'TRANSPORT':'1' if transport else '0'}
    result=subprocess.run(['bash','-e','-o','pipefail','-c',STEPS[1]['run']],cwd=ROOT,env=env,capture_output=True,text=True)
    publish=subprocess.run(['bash','-e','-o','pipefail','-c',STEPS[2]['run']],cwd=ROOT,env=env,capture_output=True,text=True)
    assert publish.returncode==0,publish.stderr
    calls=(tmp_path/'calls').read_text().splitlines() if (tmp_path/'calls').exists() else []
    return result,(tmp_path/'summary').read_text(),calls


def test_actual_shell_submits_once_and_reports_explicit_outcomes(tmp_path):
    result,summary,calls=execute(tmp_path,submission())
    assert result.returncode==0,result.stderr
    assert 'PASSED' in summary and 'explicit successful IBKR outcomes' in summary
    assert len(calls)==2 and calls[-1].endswith('/actions/submit_daily_agreements')
    assert 'synthetic-token' not in result.stdout+result.stderr+summary


@pytest.mark.parametrize('change',[{'documentsPrepared':23},{'ok':False},{'accepted':0,'submitted':False},
                                  {'masterAccount':'F10740574'},{'skipped':1},{'results':[]},{'expectedForms':[9902]}])
def test_partial_invalid_or_unsubmitted_result_fails_workflow(tmp_path,change):
    result,summary,calls=execute(tmp_path,{**submission(),**change})
    assert result.returncode==1 and 'FAILED' in summary and len(calls)==2


@pytest.mark.parametrize('options,expected_calls',[({'credentials':False},0),({'token_http':'401'},1),
                                                    ({'token_body':{}},1),({'http':'500'},2),({'transport':True},2)])
def test_auth_and_transport_failures_publish_summary_without_retry(tmp_path,options,expected_calls):
    result,summary,calls=execute(tmp_path,submission(),**options)
    assert result.returncode!=0 and len(calls)==expected_calls
    assert 'acceptance' in summary or 'No agreement submission was attempted' in summary


def test_workflow_schedule_concurrency_and_standard_notifications():
    triggers=WORKFLOW.get('on',WORKFLOW.get(True))
    assert triggers['schedule']==[{'cron':'0 11 * * *'}]
    assert WORKFLOW['concurrency']['cancel-in-progress'] is False
    assert set(WORKFLOW['jobs']) == {'prepare', 'notify-success', 'notify-failure'}
    assert '--retry' not in STEPS[1]['run'] and 'wretry' not in json.dumps(WORKFLOW)


@pytest.fixture
def route(monkeypatch):
    client=Mock();client.submit_all_agreements.return_value={**preparation(),'ibkrResponse':{'private':'must not leave action'}}
    monkeypatch.setattr(daily_agreements.IBKRWebAPI, '__new__', lambda cls: client)
    monkeypatch.setattr(authz,'_load_user',lambda user:{'id':user,'scopes':'actions' if user=='allowed' else 'accounts/read'})
    app=Flask(__name__);app.config['JWT_SECRET_KEY']='synthetic-test-signing-key-at-least-32-bytes';JWTManager(app)
    app.before_request(lambda:setattr(g,'request_id','synthetic-preparation'))
    app.before_request(authz.authenticate_request);app.before_request(authz.enforce_route_scope)
    app.register_blueprint(bp,url_prefix='/actions')
    with app.app_context():
        headers={'Authorization':'Bearer '+create_access_token(identity='allowed')}
        denied={'Authorization':'Bearer '+create_access_token(identity='denied')}
    return app.test_client(),headers,denied,client


def test_actual_route_forces_no_submission_and_omits_raw_response(route):
    http,headers,_,client=route
    result=http.post('/actions/prepare_daily_agreements',headers=headers,json={'master_account':'I6413690'})
    assert result.status_code==200
    assert result.json['mode']=='preparation' and result.json['submitted'] is False
    assert 'ibkrResponse' not in result.json and result.json['expectedForms']==daily_agreements.DAILY_AGREEMENT_FORM_NUMBERS
    client.submit_all_agreements.assert_called_once_with(master_account='I6413690',dry_run=True)


@pytest.mark.parametrize('payload',[{},[],{'master_account':'UNKNOWN'},{'master_account':'I6413690','dry_run':False}])
def test_invalid_payload_or_attempted_submission_fails_before_connector(route,payload):
    http,headers,_,client=route
    assert http.post('/actions/prepare_daily_agreements',headers=headers,json=payload).status_code==422
    client.submit_all_agreements.assert_not_called()


def test_route_requires_verified_identity_and_scope(route):
    http,headers,denied,client=route
    assert http.post('/actions/prepare_daily_agreements',json={'master_account':'I6413690'}).status_code==401
    assert http.post('/actions/prepare_daily_agreements',headers=denied,json={'master_account':'I6413690'}).status_code==403
    client.submit_all_agreements.assert_not_called()


def test_live_route_submits_once_and_strips_raw_response(route):
    http,headers,_,client=route
    client.submit_all_agreements.return_value = {**submission(), 'requestId':12345, 'dateSubmitted':'2026-10-20T11:00:00Z', 'ibkrResponse': {'private': 'omitted'}}
    response=http.post('/actions/submit_daily_agreements',headers=headers,json={'master_account':'I6413690'})
    assert response.status_code==200 and response.json['mode']=='submission'
    assert response.json['accepted']==len(daily_agreements.DAILY_AGREEMENT_FORM_NUMBERS) and 'ibkrResponse' not in response.json
    assert response.json['requestId']==12345 and response.json['dateSubmitted']=='2026-10-20T11:00:00Z'
    client.submit_all_agreements.assert_called_once_with(master_account='I6413690',dry_run=False)


def test_live_route_auth_scope_and_payload_validation(route):
    http,headers,denied,client=route
    assert http.post('/actions/submit_daily_agreements',json={'master_account':'I6413690'}).status_code==401
    assert http.post('/actions/submit_daily_agreements',headers=denied,json={'master_account':'I6413690'}).status_code==403
    assert http.post('/actions/submit_daily_agreements',headers=headers,json={'master_account':'UNKNOWN'}).status_code==422
    client.submit_all_agreements.assert_not_called()


def test_workflow_requires_expanded_coverage_not_an_old_successful_batch(tmp_path):
    payload=submission()
    old_numbers=[n for n in payload['expectedForms'] if n not in {3230,3044,4615,4155,4402}]
    payload.update(expectedForms=old_numbers,formsRequested=24,documentsPrepared=24,accepted=24,
                   results=[{'formNumber':str(n),'status':'ACCEPTED'} for n in old_numbers])
    result,summary,_=execute(tmp_path,payload)
    assert result.returncode==1 and 'FAILED' in summary


def test_workflow_retains_a_verified_preload_receipt(tmp_path):
    payload={**submission(),'requestId':12345,'dateSubmitted':'2026-10-20T11:00:00Z','ibkrResponse':{'private':'must-not-retain'},'payload':{'data':'must-not-retain'}}
    for row in payload['results']:row['status']=' ACCEPTED '
    result,summary,_=execute(tmp_path,payload)
    assert result.returncode==0,result.stderr
    receipt=json.loads((tmp_path/'daily-agreements-receipt.json').read_text())
    assert receipt['verified'] is True and receipt['masterAccount']=='I6413690'
    assert receipt['requestId']==12345 and len(receipt['results'])==29
    assert 'must-not-retain' not in json.dumps(receipt)
    assert 'Request ID: 12345' in summary and 'All 29 forms' in summary
    artifact=next(step for step in STEPS if step.get('uses','').startswith('actions/upload-artifact@'))
    assert 'daily-agreements-receipt.json' in artifact['with']['path']


def test_upstream_failure_survives_shell_summary_receipt_and_console(tmp_path):
    payload={'error':'The IBKR request could not be completed.', 'error_id':'observed-api-error',
             'code':'ibkr_upstream_error', 'details':{'ibkr_status':503,
             'ibkr_message':'Document service unavailable', 'ibkr_error_code':'SERVICE_UNAVAILABLE',
             'ibkr_request_id':'provider-correlation', 'payload':'private-pdf-marker',
             'access_token':'private-token-marker'}}
    result,summary,calls=execute(tmp_path,payload,http='502')
    receipt=json.loads((tmp_path/'daily-agreements-receipt.json').read_text())
    assert result.returncode==1 and len(calls)==2
    for evidence in (summary,json.dumps(receipt),result.stdout):
        for expected in ('observed-api-error','ibkr_upstream_error','503','Document service unavailable','provider-correlation'):
            assert expected in evidence
        assert 'private-pdf-marker' not in evidence and 'private-token-marker' not in evidence


def test_authentication_failure_preserves_diagnostic_without_submission(tmp_path):
    result,summary,calls=execute(tmp_path,submission(),token_http='503',token_body={
        'error':'Identity service unavailable','error_id':'auth-correlation','code':'identity_unavailable',
        'access_token':'must-not-log-token'})
    assert result.returncode!=0 and len(calls)==1
    receipt=json.loads((tmp_path/'daily-agreements-receipt.json').read_text())
    assert receipt['phase']=='authentication'
    assert 'auth-correlation' in summary and 'Identity service unavailable' in summary
    assert 'No agreement submission was attempted' in summary
    assert 'must-not-log-token' not in summary+result.stdout+result.stderr+json.dumps(receipt)
