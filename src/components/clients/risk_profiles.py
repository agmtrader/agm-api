from src.utils.logger import logger
from src.utils.connectors.supabase import db
from src.utils.exception import ServiceError, handle_exception
from decimal import Decimal, InvalidOperation
import re
import unicodedata

logger.announcement('Initializing Risk Profile Service', type='info')
logger.announcement('Initialized Risk Profile Service', type='success')

# Analyst risk assessments are not agency credit ratings. Treasuries include
# bills, floating-rate notes and TIPS. Allocations are fractions of the portfolio.
risk_archetypes = [
    {
        "id": 1,
        "name": "Conservative A",
        "treasuries": 0.65,
        "investment_grade": 0.35,
        "high_yield": 0.0,
        "equity": 0.0,
        "positions": 25,
        "largest_position": 0.045,
        "risk_assessment": "Low–moderate",
        "horizon_fit": "5–10 years",
        "min_score": 0,
        "max_score": 0.9
    },
    {
        "id": 2,
        "name": "Conservative B",
        "treasuries": 41 / 96,
        "investment_grade": 35 / 96,
        "high_yield": 0.0,
        "equity": 20 / 96,
        "positions": 29,
        "largest_position": 0.045,
        "risk_assessment": "Moderate",
        "horizon_fit": "5–10 years",
        "min_score": 0.9,
        "max_score": 1.25
    },
    {
        "id": 3,
        "name": "Moderate A",
        "treasuries": 26 / 96,
        "investment_grade": 30 / 96,
        "high_yield": 5 / 96,
        "equity": 35 / 96,
        "positions": 32,
        "largest_position": 0.045,
        "risk_assessment": "Moderate",
        "horizon_fit": "5–10 years",
        "min_score": 1.25,
        "max_score": 1.5
    },
    {
        "id": 4,
        "name": "Moderate B",
        "treasuries": 16 / 96,
        "investment_grade": 25 / 96,
        "high_yield": 5 / 96,
        "equity": 50 / 96,
        "positions": 33,
        "largest_position": 0.045,
        "risk_assessment": "Moderate–high",
        "horizon_fit": "7–10 years",
        "min_score": 1.5,
        "max_score": 2
    },
    {
        "id": 5,
        "name": "Moderate C",
        "treasuries": 11 / 96,
        "investment_grade": 20 / 96,
        "high_yield": 5 / 96,
        "equity": 60 / 96,
        "positions": 35,
        "largest_position": 0.045,
        "risk_assessment": "High",
        "horizon_fit": "7–10 years",
        "min_score": 2,
        "max_score": 2.5
    },
    {
        "id": 6,
        "name": "Aggressive A",
        "treasuries": 6 / 96,
        "investment_grade": 10 / 96,
        "high_yield": 5 / 96,
        "equity": 75 / 96,
        "positions": 36,
        "largest_position": 0.045,
        "risk_assessment": "High",
        "horizon_fit": "8–10 years",
        "min_score": 2.5,
        "max_score": 2.75
    },
    {
        "id": 7,
        "name": "Aggressive B",
        "treasuries": 6 / 96,
        "investment_grade": 5 / 96,
        "high_yield": 5 / 96,
        "equity": 80 / 96,
        "positions": 37,
        "largest_position": 0.045,
        "risk_assessment": "High",
        "horizon_fit": "10 years",
        "min_score": 2.75,
        "max_score": 3
    },
    {
        "id": 8,
        "name": "Aggressive C",
        "treasuries": 0.0,
        "investment_grade": 0.0,
        "high_yield": 0.0,
        "equity": 1.0,
        "positions": 33,
        "largest_position": 0.045,
        "risk_assessment": "Very high",
        "horizon_fit": "10 years",
        "min_score": 3,
        "max_score": 10
    }
]

@handle_exception
def list_risk_archetypes():
    return risk_archetypes

def get_risk_archetype_for_score(score) -> dict | None:
    try:
        normalized_score = float(score)
    except (TypeError, ValueError):
        return None

    return next(
        (
            risk_archetype for risk_archetype in risk_archetypes
            if (
                float(risk_archetype['min_score']) <= normalized_score < float(risk_archetype['max_score'])
                or (normalized_score == float(risk_archetype['max_score']) and float(risk_archetype['max_score']) == 10)
            )
        ),
        None
    )

def _with_derived_risk_archetype(risk_profile: dict) -> dict:
    if not isinstance(risk_profile, dict):
        return risk_profile

    derived_risk_archetype = get_risk_archetype_for_score(risk_profile.get('score'))
    return {
        **risk_profile,
        'estimated_deposit': float(risk_profile['estimated_deposit']) if risk_profile.get('estimated_deposit') is not None else None,
        'assigned_risk_archetype': derived_risk_archetype.get('name') if derived_risk_archetype else None,
    }


def _normalize_identity(value) -> str:
    text = unicodedata.normalize('NFKD', str(value or ''))
    text = ''.join(char for char in text if not unicodedata.combining(char))
    return re.sub(r'[^a-z0-9]', '', text.casefold())


def _resolve_or_create_contact(name: str, email: str) -> str:
    normalized_name = _normalize_identity(name)
    normalized_email = str(email or '').strip().casefold()
    if not normalized_name:
        raise ValueError('A name is required to associate a risk profile with a contact.')

    contacts = db.read(table='contact', query={})
    matches = [
        contact for contact in contacts
        if _normalize_identity(contact.get('name')) == normalized_name
        and (
            not normalized_email
            or str(contact.get('email') or '').strip().casefold() == normalized_email
        )
    ]
    if len(matches) == 1:
        return str(matches[0]['id'])
    if len(matches) > 1:
        raise ValueError(f'Multiple contacts found for name and email: {name} / {email}')

    return db.create(table='contact', data={'name': str(name).strip(), 'email': str(email or '').strip() or None})

@handle_exception
def create_risk_profile(data: dict):
    data_to_save = {**(data or {})}
    deposit = data_to_save.get('estimated_deposit')
    if deposit is not None:
        try:
            amount = Decimal(str(deposit))
            valid = (
                amount.is_finite()
                and 0 < amount < Decimal('10000000000000000')
                and amount == amount.quantize(Decimal('0.01'))
            )
        except (InvalidOperation, ValueError):
            valid = False
        if not valid:
            raise ServiceError(
                'Estimated deposit must be a positive USD amount with at most two decimal places.',
                status_code=400,
                code='invalid_estimated_deposit',
            )
        data_to_save['estimated_deposit'] = amount
    data_to_save.pop('assigned_risk_archetype', None)
    contact_id = data_to_save.pop('contact_id', None)
    email = data_to_save.pop('email', None)
    if not contact_id:
        contact_id = _resolve_or_create_contact(data_to_save.get('name'), email)
    data_to_save['contact_id'] = contact_id
    risk_profile_id = db.create(table='risk_profile', data=data_to_save)
    return {'id': risk_profile_id, 'contact_id': contact_id}

@handle_exception
def read_risk_profiles(query: dict = None, view: str = 'full'):
    if view not in ('full', 'list'):
        raise ServiceError('view must be full or list', status_code=400)
    options = {'exclude_columns': ['answers', 'raw_answers']} if view == 'list' else {}
    risk_profiles = db.read(table='risk_profile', query=query, **options)
    return [_with_derived_risk_archetype(risk_profile) for risk_profile in risk_profiles]
