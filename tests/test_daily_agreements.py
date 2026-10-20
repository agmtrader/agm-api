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
