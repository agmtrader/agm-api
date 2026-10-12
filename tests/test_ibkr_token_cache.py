from concurrent.futures import ThreadPoolExecutor
from unittest.mock import Mock
from types import SimpleNamespace
import pytest
from src.utils.connectors import ibkr_web_api as module


def make_client(monkeypatch, now=None):
    monkeypatch.setattr(module, 'get_secret', Mock(return_value='synthetic-private-key'))
    if now is not None:
        monkeypatch.setattr(module.time, 'time', lambda: now[0])
    api = module.IBKRWebAPI()
    api.generate_client_assertion = lambda: api.CLIENT_ID
    issued = []
    def post(url, data, headers, **kwargs):
        issued.append(data['client_assertion'])
        return SimpleNamespace(status_code=200, json=lambda: {'access_token': data['client_assertion']+'-token', 'expires_in': 59})
    monkeypatch.setattr(module.requests, 'post', post)
    return api, issued


def test_short_lived_token_reuses_until_expiry_relative_margin(monkeypatch):
    now=[100.0];api, issued=make_client(monkeypatch,now)
    first=api.get_bearer_token();now[0]=150
    assert api.get_bearer_token()==first and len(issued)==1
    now[0]=154
    api.get_bearer_token()
    assert len(issued)==2


def test_master_switch_cache_and_concurrent_calls_never_mix_credentials(monkeypatch):
    api,issued=make_client(monkeypatch)
    def get(url,headers):
        assert headers['Authorization']=='Bearer '+api.CLIENT_ID+'-token'
        return SimpleNamespace(status_code=200,json=lambda:{'credential':api.CLIENT_ID})
    monkeypatch.setattr(module.requests,'get',get)
    masters=['I6413690','F10740574']*10
    with ThreadPoolExecutor(8) as pool:
        results=list(pool.map(lambda master: api.get_status_of_instruction('synthetic-id',master),masters))
    assert [r['credential'] for r in results]==[api.MASTER_ACCOUNT_CREDENTIALS[m]['client_id'] for m in masters]
    assert len(issued)==2
    assert api.CLIENT_ID=='AGMTechnology-FD2'


def test_key_rotation_and_connection_invalidation_force_new_token(monkeypatch):
    api,issued=make_client(monkeypatch)
    api.get_bearer_token();api._invalidate_token();api.get_bearer_token()
    assert len(issued)==2
    api.CLIENT_PRIVATE_KEY='rotated-synthetic-key';api.get_bearer_token()
    assert len(issued)==3


def test_unknown_master_fails_before_secret_or_network_calls(monkeypatch):
    api,issued=make_client(monkeypatch)
    module.get_secret.reset_mock()
    with pytest.raises(module.ServiceError) as caught:
        api.get_status_of_instruction('synthetic-id','UNKNOWN')
    assert caught.value.status_code==400 and caught.value.code=='ibkr_master_unsupported'
    assert issued==[] and module.get_secret.call_count==0
