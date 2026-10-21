import base64
import hashlib
from types import SimpleNamespace
from unittest.mock import Mock

import pytest
from src.utils.connectors import ibkr_web_api as module


@pytest.fixture
def agreement_client(monkeypatch):
    monkeypatch.setattr(module,'get_secret',Mock(return_value='synthetic-key'))
    api=module.IBKRWebAPI()
    raw=b'%PDF-1.4\nSynthetic agreement\n%%EOF'
    def forms(forms,master_account):
        n=forms[0]
        return {'formDetails':[{'formNumber':n,'formName':'Synthetic agreement','fileName':f'{n}.pdf',
                               'fileLength':len(raw),'sha1Checksum':hashlib.sha1(raw).hexdigest()}],
                'fileData':{'name':f'{n}.pdf','data':base64.b64encode(raw).decode()}}
    api.get_forms=Mock(side_effect=forms)
    api.get_bearer_token=Mock(return_value='synthetic-token')
    api.sign_request=Mock(return_value='synthetic-signed-request')
    response=Mock(status_code=200)
    response.json.return_value={'processDocuments':{'documents':[{'formNumber':n,'status':'Accepted'} for n in module.DAILY_AGREEMENT_FORM_NUMBERS]}}
    post=Mock(return_value=response);monkeypatch.setattr(module.requests,'post',post)
    return api,post,response,forms,raw


def test_complete_batch_preserves_payload_and_explicit_acceptance(agreement_client):
    api,post,response,forms,raw=agreement_client
    response.json.return_value.update(requestId=12345,dateSubmitted='2026-10-20T11:00:00Z')
    result=api.submit_all_agreements(master_account='I6413690')
    assert result['ok'] and result['accepted']==len(module.DAILY_AGREEMENT_FORM_NUMBERS)
    assert result['requestId']==12345 and result['dateSubmitted']=='2026-10-20T11:00:00Z'
    body=api.sign_request.call_args.args[0]
    assert len(body['processDocuments']['documents'])==len(module.DAILY_AGREEMENT_FORM_NUMBERS)
    for doc in body['processDocuments']['documents']:
        assert base64.b64decode(doc['payload']['data'])==raw
        assert int(doc['attachedFile']['fileLength'])==len(raw)
        assert doc['attachedFile']['sha1Checksum']==hashlib.sha1(raw).hexdigest()
    assert post.call_count==1 and post.call_args.args[0].endswith('/accounts/documents')
    assert post.call_args.kwargs['timeout']==(10,120)
    assert post.call_args.kwargs['headers']['Content-Type']=='application/jwt'


@pytest.mark.parametrize('body',[{},[],{'status':'success'},{'documents':[{'formNumber':3024}]},
                                {'documents':[{'formNumber':3024,'status':'Accepted'}]}])
def test_incomplete_or_empty_acknowledgement_never_means_all_accepted(agreement_client,body):
    api,post,response,_,_=agreement_client;response.json.return_value=body
    result=api.submit_all_agreements(master_account='I6413690')
    assert result['ok'] is False and result['accepted']<len(module.DAILY_AGREEMENT_FORM_NUMBERS)
    assert post.call_count==1


@pytest.mark.parametrize('mutation',['hash','size','name','form','data','not_pdf'])
def test_invalid_pdf_identity_aborts_whole_batch_before_post(agreement_client,mutation):
    api,post,response,forms,raw=agreement_client
    def corrupt(forms,master_account):
        result=agreement_client[3](forms,master_account)
        if forms[0]=='3024':
            detail=result['formDetails'][0]
            if mutation=='hash':detail['sha1Checksum']='0'*40
            elif mutation=='size':detail['fileLength']+=1
            elif mutation=='name':result['fileData']['name']='different.pdf'
            elif mutation=='form':detail['formNumber']=9999
            elif mutation=='data':result['fileData']['data']='invalid-base64'
            else:result['fileData']['data']=base64.b64encode(b'not PDF').decode()
        return result
    api.get_forms.side_effect=corrupt
    result=api.submit_all_agreements(master_account='I6413690')
    assert result['ok'] is False and result['skipped']==1 and result['documentsPrepared']==len(module.DAILY_AGREEMENT_FORM_NUMBERS)-1
    post.assert_not_called();api.sign_request.assert_not_called()


def test_dry_run_is_no_submit_and_not_acceptance(agreement_client):
    api,post,_,_,_=agreement_client
    result=api.submit_all_agreements(master_account='I6413690',dry_run=True)
    assert result['ready'] and result['submitted'] is False and result['ok'] is False
    assert result['documentsPrepared']==len(module.DAILY_AGREEMENT_FORM_NUMBERS) and result['accepted']==0
    post.assert_not_called();api.get_bearer_token.assert_not_called();api.sign_request.assert_not_called()


@pytest.mark.parametrize('selection',[[],[5001]])
def test_empty_or_tax_form_selection_fails_before_reads_and_post(agreement_client,selection):
    api,post,_,_,_=agreement_client
    with pytest.raises(module.ServiceError):api.submit_all_agreements(master_account='I6413690',forms=selection)
    api.get_forms.assert_not_called();post.assert_not_called()


def test_master_is_required_and_unknown_master_is_denied(agreement_client):
    api,post,_,_,_=agreement_client
    for master in [None,'UNKNOWN']:
        with pytest.raises(module.ServiceError):api.submit_all_agreements(master_account=master)
    api.get_forms.assert_not_called();post.assert_not_called()


def test_ambiguous_post_timeout_is_not_retried(agreement_client):
    api,post,_,_,_=agreement_client;post.side_effect=module.requests.exceptions.Timeout('synthetic timeout')
    with pytest.raises(module.ServiceError):api.submit_all_agreements(master_account='I6413690')
    assert post.call_count==1


def test_conflicting_duplicate_provider_outcomes_are_unconfirmed(agreement_client):
    api,post,response,_,_=agreement_client
    response.json.return_value={'documents':[{'formNumber':3024,'status':'Accepted'},{'formNumber':3024,'status':'Rejected'}]}
    result=api.submit_all_agreements(master_account='I6413690')
    assert not result['ok'] and result['accepted']==0


def test_provider_batch_error_cannot_be_overridden_by_document_statuses(agreement_client):
    api,post,response,_,_=agreement_client
    response.json.return_value['hasError']=True
    result=api.submit_all_agreements(master_account='I6413690')
    assert not result['ok'] and result['accepted']==0


def test_daily_preload_includes_all_five_new_agreements(agreement_client):
    api,post,_,_,_=agreement_client
    required={3230,3044,4615,4155,4402}
    assert len(module.DAILY_AGREEMENT_FORM_NUMBERS)==29
    assert required.issubset(module.DAILY_AGREEMENT_FORM_NUMBERS)
    assert not {5001,9902,8001,8002}.intersection(module.DAILY_AGREEMENT_FORM_NUMBERS)
    api.submit_all_agreements(master_account='I6413690')
    submitted={int(document['formNumber']) for document in api.sign_request.call_args.args[0]['processDocuments']['documents']}
    assert submitted==set(module.DAILY_AGREEMENT_FORM_NUMBERS)
    assert required.issubset({int(call.kwargs['forms'][0]) for call in api.get_forms.call_args_list})


def test_non_200_submission_retains_diagnostic_and_never_retries(agreement_client, caplog):
    api,post,response,_,_=agreement_client
    response.status_code=503
    response.headers={'X-Request-ID':'provider-correlation'}
    response.json.return_value={'code':'SERVICE_UNAVAILABLE','message':'Document service unavailable',
                                'access_token':'private-token-marker','payload':'private-pdf-marker'}
    with pytest.raises(module.ServiceError) as failure:
        api.submit_all_agreements(master_account='I6413690')
    assert post.call_count==1
    assert failure.value.details['ibkr_status']==503
    assert failure.value.details['ibkr_message']=='Document service unavailable'
    assert failure.value.details['ibkr_request_id']=='provider-correlation'
    assert 'private-token-marker' not in str(failure.value.details)
    for expected in ('503','Document service unavailable','provider-correlation','ibkr_upstream_error'):
        assert expected in caplog.text
    assert 'private-token-marker' not in caplog.text and 'private-pdf-marker' not in caplog.text


def test_provider_message_redacts_credentials_and_handles_gateway_html():
    from src.utils.ibkr_errors import raise_ibkr_response_error
    response=Mock(status_code=502, headers={'X-Request-ID':'provider-id'})
    response.json.side_effect=ValueError('non-json gateway')
    response.text='<html><body>Bad gateway. password="private password with spaces" bearer private-bearer user@example.test</body></html>'
    with pytest.raises(module.ServiceError) as failure:
        raise_ibkr_response_error(response)
    message=failure.value.details['ibkr_message']
    assert 'Bad gateway' in message and 'redacted' in message
    for marker in ('private password','with spaces','private-bearer','user@example.test','<html>'):
        assert marker not in message


def test_byte_bounded_batches_cover_every_form_once(agreement_client, monkeypatch):
    api,post,response,_,_=agreement_client
    monkeypatch.setattr(module,'DAILY_AGREEMENT_MAX_SIGNED_BYTES',7000)
    plan=api.submit_all_agreements(master_account='I6413690',dry_run=True)
    assert len(plan['batches'])>1 and not plan['submitted']
    post.assert_not_called();api.sign_request.assert_not_called()
    signed={}
    def sign(body):
        key='batch-'+str(len(signed));signed[key]=body;return key
    api.sign_request.side_effect=sign
    def accept(url,**options):
        documents=signed[options['data']]['processDocuments']['documents']
        return SimpleNamespace(status_code=200,json=lambda:{'requestId':'request-'+options['data'],
            'documents':[{'formNumber':doc['formNumber'],'status':'Accepted'} for doc in documents]})
    post.side_effect=accept
    result=api.submit_all_agreements(master_account='I6413690')
    assert result['ok'] and result['accepted']==29 and post.call_count==len(plan['batches'])
    numbers=[doc['formNumber'] for body in signed.values() for doc in body['processDocuments']['documents']]
    assert numbers==[str(n) for n in module.DAILY_AGREEMENT_FORM_NUMBERS] and len(set(numbers))==29
    assert all(row['signedBytesBound']<=7000 for row in result['batches'])
    assert len({row['requestId'] for row in result['batches']})==len(plan['batches'])


def test_later_batch_413_retains_prior_acceptance_and_stops(agreement_client, monkeypatch):
    api,post,response,_,_=agreement_client
    monkeypatch.setattr(module,'DAILY_AGREEMENT_MAX_SIGNED_BYTES',7000)
    response.json.return_value.update(requestId='first-batch-receipt')
    failure=SimpleNamespace(status_code=413,headers={},json=lambda:{'title':'Content Too Large','traceId':'observed-trace'})
    post.side_effect=[response,failure]
    with pytest.raises(module.ServiceError) as caught:
        api.submit_all_agreements(master_account='I6413690')
    details=caught.value.details
    assert post.call_count==2 and details['ibkr_status']==413
    assert details['ibkr_message']=='Content Too Large' and details['ibkr_request_id']=='observed-trace'
    assert details['agreement_batch']==2 and details['agreement_accepted_forms']
    assert details['agreement_unsubmitted_forms']
    assert details['agreement_completed_batches'][0]['requestId']=='first-batch-receipt'


def test_single_oversized_form_aborts_before_signing_or_submission(agreement_client, monkeypatch):
    api,post,_,_,_=agreement_client
    monkeypatch.setattr(module,'DAILY_AGREEMENT_MAX_SIGNED_BYTES',4100)
    with pytest.raises(module.ServiceError) as caught:
        api.submit_all_agreements(master_account='I6413690')
    assert caught.value.code=='ibkr_agreement_too_large'
    post.assert_not_called();api.sign_request.assert_not_called()


def test_rejected_document_is_visible_in_log_and_stops_later_batches(agreement_client,monkeypatch,caplog):
    api,post,response,_,_=agreement_client
    monkeypatch.setattr(module,'DAILY_AGREEMENT_MAX_SIGNED_BYTES',7000)
    response.json.return_value={'documents':[{'formNumber':3024,'status':'Rejected','message':'Unsupported PDF version'}]}
    result=api.submit_all_agreements(master_account='I6413690')
    assert not result['ok'] and post.call_count==1
    assert any(row['status']=='NotSubmitted' for row in result['results'])
    assert 'Unsupported PDF version' in caplog.text and '3024' in caplog.text
