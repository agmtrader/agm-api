"""Real exporter + Flask/JWT routes. External identity, Flex, DB and Gmail mocked."""
import sys,types
from pathlib import Path
from unittest.mock import Mock
sys.path.insert(0,str(Path(__file__).resolve().parents[2]))
for name,attrs in {
 'src.utils.connectors.supabase':{'db':Mock()},
 'src.utils.connectors.flex_query_api':{'getFlexQuery':Mock(side_effect=AssertionError('live Flex prohibited'))},
 'src.utils.connectors.gmail':{'GmailConnector':Mock(side_effect=AssertionError('live email prohibited'))},
}.items():
 m=types.ModuleType(name);m.__dict__.update(attrs);sys.modules[name]=m
import pytest,xlrd
from flask import Flask
from flask_jwt_extended import JWTManager,create_access_token
from src.utils import authz
from src.app.tools.public import trade_tickets as routes
from src.components.tools.public.trade_tickets import generate_improsa_xls

BASE={'ExecID':'execution-A','ClientAccountID':'TEST-CONTRACT','Description':'Synthetic bond','TradeDate':'2026-10-12','SettleDate':'2026-10-14','ISIN':'TEST-ISIN','Quantity':'1000','Price':'98.25','Yield':'5.125','AccruedInterest':'12.50','NetCash':'-995.00','CurrencyPrimary':'USD','Maturity':'2027-10-12','Exchange':'TEST-EXCHANGE','Market':'TEST-MARKET','TransactionType':'BUY','Commission':'-2.50','OtherCommission':'-0.50'}
def book(rows,indices):
 return xlrd.open_workbook(file_contents=generate_improsa_xls(rows,indices).getvalue(),formatting_info=True)

def test_independent_synthetic_field_mapping_and_formats():
 w=book([BASE],[0]);s=w.sheet_by_index(0)
 assert w.sheet_names()==['ExportacionDatos_0'] and (s.nrows,s.ncols)==(2,28)
 assert s.merged_cells==[(1,2,0,2)]
 expected={0:'TEST-CONTRACT',6:'TEST-ISIN',7:'TEST-ISIN',8:1000,10:98.25,11:5.125,12:12.5,13:-995,18:'USD',19:365,24:'TEST-EXCHANGE',25:'TEST-MARKET',26:'BUY',27:3}
 for col,value in expected.items():assert s.cell_value(1,col)==value
 for col,date in {3:(2026,10,12),5:(2026,10,14),20:(2027,10,12)}.items():
  assert s.cell_type(1,col)==xlrd.XL_CELL_DATE
  assert xlrd.xldate_as_tuple(s.cell_value(1,col),w.datemode)[:3]==date
  assert w.format_map[w.xf_list[s.cell_xf_index(1,col)].format_key].format_str=='dd/mm/yyyy;@'
 for col in [2,4,9,14,15,16,17,21,22,23]:assert s.cell_value(1,col)==''
 for col in [2,14,15,16,17,21,22,23]:assert s.cell_value(0,col)=='N/A'
 assert w.format_map[w.xf_list[s.cell_xf_index(1,10)].format_key].format_str=='_-* #,##0.00_-;\\-* #,##0.00_-;_-* "-"??_-;_-@_-'

@pytest.mark.parametrize('field',['ClientAccountID','TradeDate','SettleDate','ISIN','Quantity','Price','NetCash','CurrencyPrimary'])
def test_missing_required_fields_fail_explicitly(field):
 row={k:v for k,v in BASE.items() if k!=field}
 with pytest.raises(ValueError,match='Missing required Improsa fields'):book([row],[0])

def test_empty_selection_rejected():
 with pytest.raises(ValueError,match='At least one'):book([BASE],[])

def test_optional_missing_values_are_blank_not_zero():
 row={k:v for k,v in BASE.items() if k not in ['Yield','AccruedInterest','Maturity','Exchange','Market','TransactionType','Commission','OtherCommission']}
 s=book([row],[0]).sheet_by_index(0)
 for col in [11,12,19,20,24,25,26,27]:assert s.cell_value(1,col)==''

@pytest.fixture
def boundary(monkeypatch):
 app=Flask(__name__);app.config['JWT_SECRET_KEY']='synthetic-signing-key-only-at-least-32-bytes';JWTManager(app)
 users={'owner':{'id':'owner','scopes':'trade_tickets'},'other':{'id':'other','scopes':'trade_tickets'},'limited':{'id':'limited','scopes':'accounts/read'},'admin':{'id':'admin','scopes':'all'}}
 monkeypatch.setattr(authz,'_load_user',lambda identity:users.get(identity))
 app.before_request(authz.authenticate_request);app.before_request(authz.enforce_route_scope)
 app.register_blueprint(routes.bp,url_prefix='/trade_tickets')
 lookup=Mock(side_effect=lambda query:[{'query_id':'owned-query','user_id':'owner'}] if query.get('query_id','owned-query')=='owned-query' and query.get('user_id','owner')=='owner' else [])
 reads=Mock(return_value=[BASE,{**BASE,'ExecID':'execution-B','NetCash':'-996'}])
 monkeypatch.setattr(routes,'list_trade_tickets',lookup);monkeypatch.setattr(routes,'read',reads)
 email=Mock(side_effect=AssertionError('export must not email'));monkeypatch.setattr(routes,'send_trade_ticket_email',email)
 def headers(identity='owner',claims=None):
  with app.app_context():return {'Authorization':'Bearer '+create_access_token(identity=identity,additional_claims=claims or {})}
 return app.test_client(),headers,lookup,reads,email

def test_anonymous_bad_token_and_scope_denied_before_flex(boundary):
 http,headers,lookup,reads,email=boundary
 for h,status in [({},401),({'Authorization':'Bearer broken'},401),(headers('limited',{'scopes':'all'}),403)]:
  assert http.post('/trade_tickets/export_improsa',headers=h,json={'query_id':'owned-query','execution_ids':['execution-A']}).status_code==status
 reads.assert_not_called();lookup.assert_not_called();email.assert_not_called()

@pytest.mark.parametrize('endpoint',['read','export_improsa'])
def test_other_owner_rejected_before_flex(boundary,endpoint):
 http,h,lookup,reads,email=boundary
 r=http.get('/trade_tickets/read?query_id=owned-query&user_id=owner',headers=h('other',{'scopes':'all'})) if endpoint=='read' else http.post('/trade_tickets/export_improsa',headers=h('other',{'scopes':'all'}),json={'query_id':'owned-query','execution_ids':['execution-A'],'user_id':'owner'})
 assert r.status_code==404
 lookup.assert_called_once_with(query={'query_id':'owned-query','user_id':'other'});reads.assert_not_called();email.assert_not_called()

def test_list_caller_cannot_override_owner(boundary):
 http,h,lookup,reads,email=boundary
 assert http.get('/trade_tickets/list?user_id=other',headers=h()).status_code==200
 lookup.assert_called_once_with(query={'user_id':'owner'})

def test_http_export_execid_preserves_duplicate_looking_rows(boundary):
 http,h,lookup,reads,email=boundary
 r=http.post('/trade_tickets/export_improsa',headers=h(),json={'query_id':'owned-query','execution_ids':['execution-B','execution-A']})
 assert r.status_code==200 and r.mimetype=='application/vnd.ms-excel'
 assert 'improsa-trade-ticket.xls' in r.headers['Content-Disposition']
 s=xlrd.open_workbook(file_contents=r.data).sheet_by_index(0)
 assert s.nrows==3 and [s.cell_value(i,13) for i in [1,2]]==[-996,-995]
 reads.assert_called_once_with('owned-query');email.assert_not_called()

@pytest.mark.parametrize('ids',[[],['unknown'],[''],[1],'execution-A'])
def test_bad_selection_returns_400(boundary,ids):
 http,h,*_=boundary
 assert http.post('/trade_tickets/export_improsa',headers=h(),json={'query_id':'owned-query','execution_ids':ids}).status_code==400

def test_upstream_invalid_response_returns_502(boundary):
 http,h,lookup,reads,email=boundary;reads.return_value={'error':'fixture upstream error'}
 assert http.post('/trade_tickets/export_improsa',headers=h(),json={'query_id':'owned-query','execution_ids':['execution-A']}).status_code==502

def test_missing_field_http_error_not_false_success(boundary):
 http,h,lookup,reads,email=boundary;reads.return_value=[{k:v for k,v in BASE.items() if k!='ISIN'}]
 r=http.post('/trade_tickets/export_improsa',headers=h(),json={'query_id':'owned-query','execution_ids':['execution-A']})
 assert r.status_code==400 and 'issue' in r.json['error']

def test_missing_required_value_in_one_of_multiple_rows_rejected():
 second={k:v for k,v in BASE.items() if k!='Price'}
 with pytest.raises(ValueError,match='gross price'):book([BASE,second],[0,1])

@pytest.mark.parametrize('value',['NaN','Infinity','-Infinity'])
def test_nonfinite_required_numeric_rejected(value):
 with pytest.raises(ValueError,match='gross price'):book([{**BASE,'Price':value}],[0])

@pytest.mark.parametrize('price',[None,'NaN','Infinity','-Infinity'])
def test_bad_required_price_in_mixed_rows_http_400(boundary,price):
 http,h,lookup,reads,email=boundary
 reads.return_value=[BASE,{**BASE,'ExecID':'execution-B','Price':price}]
 r=http.post('/trade_tickets/export_improsa',headers=h(),json={'query_id':'owned-query','execution_ids':['execution-A','execution-B']})
 assert r.status_code==400 and 'gross price' in r.json['error']
 email.assert_not_called()

@pytest.mark.parametrize('field',['ClientAccountID','ISIN','CurrencyPrimary'])
def test_missing_required_text_in_mixed_rows_rejected(field):
 second={k:v for k,v in BASE.items() if k!=field}
 with pytest.raises(ValueError,match='Missing required Improsa fields'):book([BASE,second],[0,1])

def test_nan_primary_source_uses_valid_fallback():
 s=book([{**BASE,'ISIN':float('nan'),'SecurityID':'FALLBACK-ISIN'}],[0]).sheet_by_index(0)
 assert s.cell_value(1,6)=='FALLBACK-ISIN'



import hashlib,json

def style_signature(w,s,r,c):
 x=w.xf_list[s.cell_xf_index(r,c)];f=w.font_list[x.font_index]
 fields={'font':{k:v for k,v in vars(f).items() if k!='font_index'},'alignment':vars(x.alignment),'border':vars(x.border),'background':vars(x.background),'protection':vars(x.protection),'format':w.format_map[x.format_key].format_str}
 return hashlib.sha256(json.dumps(fields,sort_keys=True).encode()).hexdigest()


def test_reference_template_layout_and_styles_preserved_for_each_execution():
 import json
 reference=json.loads((Path(__file__).parent/'improsa_reference_fingerprints.json').read_text())
 w=book([BASE,{**BASE,'ExecID':'execution-B'}],[0,1]);s=w.sheet_by_index(0)
 assert w.sheet_names()==[reference['sheet']]
 assert s.row_values(0)==reference['headers']
 assert [s.colinfo_map[c].width for c in range(28)]==reference['widths']
 assert s.rowinfo_map[0].height==reference['heights'][0]
 assert [style_signature(w,s,0,c) for c in range(28)]==reference['header_styles']
 for r in (1,2):
  assert s.rowinfo_map[r].height==reference['heights'][1]
  assert [style_signature(w,s,r,c) for c in range(28)]==reference['data_styles']
 assert sorted(s.merged_cells)==[(1,2,0,2),(2,3,0,2)]

if __name__=='__main__':
 raise SystemExit(pytest.main([__file__,'-q']))
