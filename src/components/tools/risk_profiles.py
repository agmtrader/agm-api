from src.utils.logger import logger
from src.utils.connectors.supabase import db
from src.utils.exception import handle_exception

logger.announcement('Initializing Risk Profile Service', type='info')
logger.announcement('Initialized Risk Profile Service', type='success')

risk_archetypes = [
  {
    'id': 1,
    'name': 'Conservative A',
    'bonds_aaa_a': 0.3,
    'bonds_bbb': 0.7,
    'bonds_bb': 0,
    'etfs': 0,
    'min_score': 0,
    'max_score': 0.9
  },
  {
    'id': 2,
    'name': 'Conservative B',
    'bonds_aaa_a': 0.18,
    'bonds_bbb': 0.54,
    'bonds_bb': .18,
    'etfs': .1,
    'min_score': 0.9,
    'max_score': 1.25
  },
  {
    'id': 3,
    'name': 'Moderate A',
    'bonds_aaa_a': 0.16,
    'bonds_bbb': 0.48,
    'bonds_bb': 0.16,
    'etfs': 0.2,
    'min_score': 1.25,
    'max_score': 1.5
  },
  {
    'id': 4,
    'name': 'Moderate B',
    'bonds_aaa_a': 0.15,
    'bonds_bbb': 0.375,
    'bonds_bb': 0.15,
    'etfs': 0.25,
    'min_score': 1.5,
    'max_score': 2
  },
  {
    'id': 5,
    'name': 'Moderate C',
    'bonds_aaa_a': 0.14,
    'bonds_bbb': 0.35,
    'bonds_bb': 0.21,
    'etfs': 0.3,
    'min_score': 2,
    'max_score': 2.5
  },
  {
    'id': 6,
    'name': 'Aggressive A',
    'bonds_aaa_a': 0.13,
    'bonds_bbb': 0.325,
    'bonds_bb': .195,
    'etfs': .35,
    'min_score': 2.5,
    'max_score': 2.75
  },
  {
    'id': 7,
    'name': 'Aggressive B',
    'bonds_aaa_a': 0.12,
    'bonds_bbb': 0.30,
    'bonds_bb': 0.18,
    'etfs': 0.4,
    'min_score': 2.75,
    'max_score': 3
  },
  {
    'id': 8,
    'name': 'Aggressive C',
    'bonds_aaa_a': 0.05,
    'bonds_bbb': 0.25,
    'bonds_bb': 0.20,
    'etfs': 0.5,
    'min_score': 3,
    'max_score': 10
  }
]

@handle_exception
def list_risk_archetypes():
    return risk_archetypes

@handle_exception
def create_risk_profile(data: dict):
    risk_profile_id = db.create(table='risk_profile', data=data)
    return {'id': risk_profile_id}

@handle_exception
def read_risk_profiles(query: dict = None):
    risk_profiles = db.read(table='risk_profile', query=query)
    return risk_profiles