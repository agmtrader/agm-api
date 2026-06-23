from src.utils.logger import logger
from src.utils.connectors.supabase import db
from src.utils.exception import handle_exception

logger.announcement('Initializing Risk Profile Service', type='info')
logger.announcement('Initialized Risk Profile Service', type='success')

risk_archetypes = [
    {
      "id": 1,
      "name": "Conservative A",
      "treasuries": 0.5,
      "bonds_aaa_a": 0.15,
      "bonds_bbb": 0.35,
      "bonds_bb": 0.0,
      "etfs": 0.0,
      "min_score": 0,
      "max_score": 0.9
    },
    {
      "id": 2,
      "name": "Conservative B",
      "treasuries": 0.0,
      "bonds_aaa_a": 0.18,
      "bonds_bbb": 0.54,
      "bonds_bb": 0.08,
      "etfs": 0.2,
      "min_score": 0.9,
      "max_score": 1.25
    },
    {
      "id": 3,
      "name": "Moderate A",
      "treasuries": 0.0,
      "bonds_aaa_a": 0.104,
      "bonds_bbb": 0.312,
      "bonds_bb": 0.234,
      "etfs": 0.35,
      "min_score": 1.25,
      "max_score": 1.5
    },
    {
      "id": 4,
      "name": "Moderate B",
      "treasuries": 0.0,
      "bonds_aaa_a": 0.0,
      "bonds_bbb": 0.25,
      "bonds_bb": 0.25,
      "etfs": 0.5,
      "min_score": 1.5,
      "max_score": 2
    },
    {
      "id": 5,
      "name": "Moderate C",
      "treasuries": 0.0,
      "bonds_aaa_a": 0.0,
      "bonds_bbb": 0.16,
      "bonds_bb": 0.24,
      "etfs": 0.6,
      "min_score": 2,
      "max_score": 2.5
    },
    {
      "id": 6,
      "name": "Aggressive A",
      "treasuries": 0.0,
      "bonds_aaa_a": 0.0,
      "bonds_bbb": 0.075,
      "bonds_bb": 0.175,
      "etfs": 0.75,
      "min_score": 2.5,
      "max_score": 2.75
    },
    {
      "id": 7,
      "name": "Aggressive B",
      "treasuries": 0.0,
      "bonds_aaa_a": 0.0,
      "bonds_bbb": 0.04,
      "bonds_bb": 0.16,
      "etfs": 0.8,
      "min_score": 2.75,
      "max_score": 3
    },
    {
      "id": 8,
      "name": "Aggressive C",
      "treasuries": 0.0,
      "bonds_aaa_a": 0.0,
      "bonds_bbb": 0.0,
      "bonds_bb": 0.0,
      "etfs": 1.0,
      "min_score": 3,
      "max_score": 10
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