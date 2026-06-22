from src.utils.exception import handle_exception
from src.utils.connectors.supabase import db
from src.utils.logger import logger
from typing import Optional
from datetime import datetime
import re
import unicodedata
from src.components.tools.public.reporting import (
    get_ofac_sdn_list,
    get_uk_sanctions_list,
    get_un_sanctions_list,
    get_ibkr_details,
)
from src.components.clients.document_processing import process_document_text_extraction

logger.announcement('Initializing Contacts Service', type='info')
logger.announcement('Initialized Contacts Service', type='success')

contact_table = 'contact'
contact_document_table = 'contact_document'
contact_screening_table = 'contact_screening'
_sanctions_lists_cache = None
_ibkr_details_by_account_id_cache = None
_sanctions_match_indexes_cache = None

WEIGHTS = {
    'customerRisk': 0.30,
    'jurisdictionRisk': 0.25,
    'productRisk': 0.15,
    'deliveryRisk': 0.15,
    'introducerRisk': 0.15,
}

EU_COUNTRIES = {
    'AUT', 'BEL', 'BGR', 'HRV', 'CYP', 'CZE', 'DNK', 'EST', 'FIN', 'FRA', 'DEU', 'GRC', 'HUN',
    'IRL', 'ITA', 'LVA', 'LTU', 'LUX', 'MLT', 'NLD', 'POL', 'PRT', 'ROU', 'SVK', 'SVN', 'ESP', 'SWE'
}
LOW_RISK_JURISDICTIONS = {'USA', 'GBR', 'VGB', *EU_COUNTRIES}
OECD_FATF_WELL_REGULATED = {
    'AUS', 'CAN', 'CHL', 'COL', 'CRI', 'ISL', 'ISR', 'JPN', 'KOR', 'MEX', 'NZL', 'NOR', 'CHE', 'TUR', 'SGP'
}
HIGH_RISK_JURISDICTIONS = {'AFG', 'MMR', 'YEM', 'SSD', 'SOM', 'COD', 'IRQ', 'LBN', 'NGA', 'PAK', 'SYR', 'VEN'}
SANCTIONED_OR_FATF_HIGH_RISK = {'IRN', 'PRK', 'CUB', 'RUS', 'BLR'}
FATF_LISTED_COUNTRY_CODES = {
    'AGO', 'BOL', 'BGR', 'CMR', 'CIV', 'COD', 'DZA', 'HTI', 'IRN', 'KEN', 'KWT', 'LAO',
    'LBN', 'MCO', 'MMR', 'NAM', 'NPL', 'PNG', 'PRK', 'SSD', 'SYR', 'VEN', 'VGB', 'VNM', 'YEM',
}
FATF_LISTED_COUNTRY_NAMES = {
    'ALGERIA',
    'ANGOLA',
    'BOLIVIA',
    'BULGARIA',
    'CAMEROON',
    'COTE D IVOIRE',
    'DEMOCRATIC PEOPLE S REPUBLIC OF KOREA',
    'DEMOCRATIC REPUBLIC OF CONGO',
    'HAITI',
    'IRAN',
    'KENYA',
    'KUWAIT',
    'LAO PEOPLE S DEMOCRATIC REPUBLIC',
    'LEBANON',
    'MONACO',
    'MYANMAR',
    'NAMIBIA',
    'NEPAL',
    'PAPUA NEW GUINEA',
    'SOUTH SUDAN',
    'SYRIA',
    'VENEZUELA',
    'VIETNAM',
    'VIRGIN ISLANDS UK',
    'YEMEN',
}
SIMPLE_PRODUCTS = {'STK', 'BOND', 'FUND', 'CASH', 'ETF'}
DERIVATIVE_PRODUCTS = {'OPT', 'FUT'}
COMPLEX_PRODUCTS = {'CFD', 'WAR', 'SSF', 'FOP', 'COMB', 'MRGN'}

def _filter_supported_columns(table: str, payload: Optional[dict]) -> dict:
    payload = payload or {}
    if not payload:
        return {}
    schema = db.get_schema(table)
    allowed_columns = set(schema.keys())
    return {key: value for key, value in payload.items() if key in allowed_columns}

@handle_exception
def create_contact(contact: dict = None):
    filtered_contact = _filter_supported_columns(contact_table, contact)
    contact_id = db.create(table=contact_table, data=filtered_contact)
    return {'id': contact_id}

@handle_exception
def read_contacts(query=None):
    contacts = db.read(table=contact_table, query=query)
    return contacts

@handle_exception
def update_contact(query: dict = None, contact: dict = None):
    filtered_contact = _filter_supported_columns(contact_table, contact)
    if len(filtered_contact) == 0:
        return {'id': str((query or {}).get('id') or ''), 'status': 'skipped'}
    contact_id = db.update(table=contact_table, query=query, data=filtered_contact)
    return {'id': contact_id}


@handle_exception
def upload_contact_document(
    account_id: str = None,
    contact_id: str = None,
    file_name: str = None,
    file_length: int = None,
    sha1_checksum: str = None,
    mime_type: str = None,
    data: str = None,
    category: str = None,
    type: str = None,
    document_language: str = None,
    issued_date: str = None,
    expiry_date: str = None,
    comment: Optional[str] = None
):
    if not contact_id:
        raise Exception('contact_id is required')
    if not account_id:
        raise Exception('account_id is required')

    document_id = db.create(
        table='document',
        data={
            'file_name': file_name,
            'file_length': file_length,
            'sha1_checksum': sha1_checksum,
            'mime_type': mime_type,
            'data': data
        }
    )

    link_id = db.create(
        table=contact_document_table,
        data={
            'account_id': account_id,
            'contact_id': contact_id,
            'document_id': document_id,
            'category': category,
            'type': type,
            'document_language': document_language,
            'issued_date': issued_date,
            'expiry_date': expiry_date,
            'comment': comment
        }
    )
    processing_result = process_document_text_extraction(document_id=document_id, source_language=document_language)
    return {'id': link_id, 'document_id': document_id, 'document_processing': processing_result}


@handle_exception
def read_contact_documents(
    contact_id: str = None,
    document_ids: list[str] = None,
    include_data: bool = False,
    include_documents: bool = True
):
    query = {'contact_id': contact_id} if contact_id else {}
    links = db.read(table=contact_document_table, query=query) or []
    if document_ids:
        requested_document_ids = {str(document_id) for document_id in document_ids if document_id}
        links = [
            link for link in links
            if str(link.get('document_id') or '') in requested_document_ids
        ]
    if not include_documents:
        return {'documents': [], 'contact_documents': links}

    documents = []
    exclude = None if include_data else ['data']
    for link in links:
        document = db.read(table='document', query={'id': link.get('document_id')}, exclude_columns=exclude) or []
        documents.extend(document)
    return {'documents': documents, 'contact_documents': links}


@handle_exception
def update_contact_document(
    document_id: str = None,
    category: str = None,
    type: str = None,
    document_language: str = None,
    issued_date: str = None,
    expiry_date: str = None,
    comment: Optional[str] = None
):
    return db.update(
        table=contact_document_table,
        query={'document_id': document_id},
        data={
            'category': category,
            'type': type,
            'document_language': document_language,
            'issued_date': issued_date,
            'expiry_date': expiry_date,
            'comment': comment
        }
    )


@handle_exception
def delete_contact_document(document_id: str = None):
    db.delete(table=contact_document_table, query={'document_id': document_id})
    db.delete(table='document', query={'id': document_id})
    return {'status': 'success'}


@handle_exception
def create_contact_screening(
    contact_id: str = None,
    risk_score: Optional[float] = None,
    fatf_status: Optional[list[dict]] = None,
    un_status: Optional[list[dict]] = None,
    uk_status=None,
    ofac_results=None,
    created: Optional[str] = None
):
    if not contact_id:
        raise Exception('contact_id is required')
    return db.create(table=contact_screening_table, data={
        'contact_id': contact_id,
        'risk_score': risk_score if risk_score is not None else 0,
        'fatf_status': fatf_status,
        'un_status': un_status,
        'uk_status': uk_status,
        'ofac_results': ofac_results,
        'created': created
    })


def build_contact_screening_payload(
    contact: dict,
    account_row: dict | None,
    ibkr_detail: dict | None,
    account_contact_link: dict | None,
    created: Optional[str] = None,
    sanctions_indexes: Optional[tuple[dict[str, list[dict]], dict[str, list[dict]], dict[str, list[dict]]]] = None,
) -> dict:
    contact_id = contact.get('id') if isinstance(contact, dict) else None
    if not contact_id:
        raise Exception('contact_id is required')

    contact_name = (contact.get('name') or '').strip()
    normalized_contact_name = _normalize_name(contact_name)
    if not normalized_contact_name:
        raise Exception('contact name is required for screening')

    if not account_row:
        raise Exception('linked account not found for screening')

    ofac_index, uk_index, un_index = sanctions_indexes or _get_sanctions_match_indexes()
    ofac_results = _unique_rows(list(ofac_index.get(normalized_contact_name, [])))
    uk_status = _unique_rows(list(uk_index.get(normalized_contact_name, [])))
    un_matches = _unique_rows(list(un_index.get(normalized_contact_name, [])))
    un_status = un_matches if len(un_matches) > 0 else None
    uk_status_value = uk_status if len(uk_status) > 0 else None
    ofac_results_value = ofac_results if len(ofac_results) > 0 else None
    risk_score = _compute_weighted_holder_risk_score(
        contact=contact,
        account_row=account_row,
        ibkr_detail=ibkr_detail,
        account_contact_link=account_contact_link,
    )
    if ofac_results or uk_status or un_matches:
        risk_score = max(risk_score, 9.0)

    return {
        'contact_id': contact_id,
        'risk_score': risk_score,
        'fatf_status': _get_fatf_status(_get_contact_risk_country(contact, account_row, ibkr_detail, account_contact_link)),
        'un_status': un_status,
        'uk_status': uk_status_value,
        'ofac_results': ofac_results_value,
        'created': created or datetime.now().strftime('%Y%m%d%H%M%S'),
    }


@handle_exception
def create_contact_screenings_batch(screenings: list[dict], batch_size: int = 500):
    if not screenings:
        raise Exception('screenings are required')
    inserted = db.create_many(
        table=contact_screening_table,
        data=screenings,
        batch_size=batch_size,
    )
    return {'inserted': inserted}


def _normalize_name(value: str) -> str:
    if not value:
        return ""
    ascii_name = (
        unicodedata.normalize("NFKD", str(value))
        .encode("ascii", "ignore")
        .decode("ascii")
    )
    normalized = re.sub(r"[^a-z0-9 ]+", " ", ascii_name.lower())
    return " ".join(normalized.split())


def _to_upper(value) -> str:
    return str(value or '').strip().upper()


def _normalize_country_name(value: str | None) -> str:
    if not value:
        return ''
    ascii_name = (
        unicodedata.normalize("NFKD", str(value))
        .encode("ascii", "ignore")
        .decode("ascii")
    )
    normalized = re.sub(r"[^A-Z0-9 ]+", " ", ascii_name.upper())
    return " ".join(normalized.split())


def _get_fatf_status(country_code_or_name: str | None) -> str | None:
    country_value = str(country_code_or_name or '').strip()
    if not country_value:
        return None

    if _to_upper(country_value) in FATF_LISTED_COUNTRY_CODES:
        return 'Listed'

    if _normalize_country_name(country_value) in FATF_LISTED_COUNTRY_NAMES:
        return 'Listed'

    return None


def _clamp_risk_score(value: float) -> float:
    return max(1.0, min(10.0, float(value)))


def _extract_products(accounts: list) -> list[str]:
    products = []
    for account in accounts or []:
        for tp in account.get('tradingPermissions') or []:
            product = _to_upper(tp.get('product'))
            if product:
                products.append(product)
    return products


def _get_jurisdiction_risk_score(country_code: str | None) -> int:
    country = _to_upper(country_code)
    if not country:
        return 5
    if country in SANCTIONED_OR_FATF_HIGH_RISK:
        return 9
    if country in HIGH_RISK_JURISDICTIONS:
        return 7
    if country in LOW_RISK_JURISDICTIONS:
        return 1
    if country in OECD_FATF_WELL_REGULATED:
        return 3
    return 5


def _get_product_risk_score(accounts: list) -> int:
    if not accounts:
        return 3

    primary_account = accounts[0] if accounts else {}
    margin_type = _to_upper(primary_account.get('margin'))
    products = _extract_products(accounts)
    has_high_ml_keywords = any(re.search(r'CRYPTO|PRIVATE|UNREGULATED|OMNIBUS', p) for p in products)
    if has_high_ml_keywords:
        return 9

    has_complex_products = any(p in COMPLEX_PRODUCTS for p in products)
    if has_complex_products:
        return 7

    has_derivatives = any(p in DERIVATIVE_PRODUCTS for p in products)
    high_permission_volume = len(products) >= 5
    if high_permission_volume or (margin_type == 'MARGIN' and has_derivatives):
        return 5

    if margin_type == 'MARGIN' or has_derivatives:
        return 3

    has_only_simple_products = len(products) > 0 and all(p in SIMPLE_PRODUCTS for p in products)
    if has_only_simple_products:
        return 1

    return 3


def _has_adverse_signals(holder_details: dict, regulatory_info: dict) -> bool:
    if holder_details.get('isPEP') is True:
        return True
    regulatory_details = (regulatory_info or {}).get('regulatoryDetails') or []
    for rd in regulatory_details:
        if not rd or not rd.get('status'):
            continue
        text = f"{rd.get('code', '')} {rd.get('detail', '')} {rd.get('details', '')}".upper()
        if re.search(r'SANCTION|ADVERSE|CRIME|CRIMINAL|FRAUD|AML|TERROR|OFAC', text):
            return True
    return False


def _get_customer_risk_score(
    customer_type: str,
    holder_details: dict,
    financial_info: dict,
    regulatory_info: dict,
    organization: dict,
    account_countries: list[str],
    residence_country: str,
) -> int:
    if _has_adverse_signals(holder_details, regulatory_info):
        return 9

    investment_experience = (financial_info or {}).get('investmentExperience') or []
    sources_of_wealth = (financial_info or {}).get('sourcesOfWealth') or []
    has_complex_trading_experience = any(
        _to_upper(exp.get('assetClass')) in COMPLEX_PRODUCTS or _to_upper(exp.get('assetClass')) in DERIVATIVE_PRODUCTS
        for exp in investment_experience
    )
    has_many_wealth_sources = len(sources_of_wealth) >= 3
    has_other_source = any(re.search(r'OTHER', str(sow.get('sourceType', '')), re.IGNORECASE) for sow in sources_of_wealth)

    cross_border = any(country and country != _to_upper(residence_country) for country in account_countries)
    associated_entities = ((organization or {}).get('associatedEntities') or {}).get('associatedEntities') or []
    associated_individuals = ((organization or {}).get('associatedEntities') or {}).get('associatedIndividuals') or []
    has_complex_org_structure = bool(associated_entities or len(associated_individuals) > 2)

    if customer_type == 'ORG':
        if has_complex_org_structure or cross_border or has_complex_trading_experience or has_many_wealth_sources:
            return 7
        return 5

    employment_type = _to_upper(holder_details.get('employmentType'))
    is_professional_or_owner = employment_type == 'SELFEMPLOYED' or bool(((holder_details.get('employmentDetails') or {}).get('businessDescription')))

    if customer_type == 'JOINT':
        return 3
    if is_professional_or_owner or has_complex_trading_experience or has_many_wealth_sources or has_other_source:
        return 3
    return 1


def _get_delivery_channel_risk_score() -> int:
    return 5


def _get_introducer_risk_score(advisor_code: str | None) -> int:
    return 3 if advisor_code else 1


def _pick_latest_contact_link(contact_id: str) -> dict | None:
    links = db.read(table='account_contact', query={'contact_id': contact_id}) or []
    if not links:
        return None
    links_sorted = sorted(
        links,
        key=lambda row: str(row.get('updated') or row.get('created') or ''),
        reverse=True,
    )
    return links_sorted[0]


def _extract_candidate_names_from_application(customer: dict) -> set[str]:
    names = set()
    customer_type = _resolve_customer_type(customer.get('type'))
    if customer_type == 'INDIVIDUAL':
        holder = ((customer.get('accountHolder') or {}).get('accountHolderDetails') or [{}])[0] or {}
        first = str((holder.get('name') or {}).get('first') or '').strip()
        last = str((holder.get('name') or {}).get('last') or '').strip()
        if first or last:
            names.add(_normalize_name(f'{first} {last}'.strip()))
    elif customer_type == 'JOINT':
        joint = customer.get('jointHolders') or {}
        for key in ('firstHolderDetails', 'secondHolderDetails'):
            holder = (joint.get(key) or [{}])[0] or {}
            first = str((holder.get('name') or {}).get('first') or '').strip()
            last = str((holder.get('name') or {}).get('last') or '').strip()
            if first or last:
                names.add(_normalize_name(f'{first} {last}'.strip()))
    elif customer_type == 'ORG':
        associated_individuals = ((customer.get('organization') or {}).get('associatedEntities') or {}).get('associatedIndividuals') or []
        for holder in associated_individuals:
            first = str((holder.get('name') or {}).get('first') or '').strip()
            last = str((holder.get('name') or {}).get('last') or '').strip()
            if first or last:
                names.add(_normalize_name(f'{first} {last}'.strip()))
    return {name for name in names if name}


def _find_latest_account_from_application_json(contact: dict) -> dict | None:
    all_accounts = db.read(table='account', query={}) or []
    if not all_accounts:
        return None

    contact_email = str(contact.get('email') or '').strip().lower()
    contact_name = _normalize_name(contact.get('name') or '')
    candidates: list[dict] = []

    for account_row in all_accounts:
        application_json = account_row.get('application_json') or {}
        if not isinstance(application_json, dict):
            continue
        customer = application_json.get('customer') or {}
        if not isinstance(customer, dict):
            continue

        customer_email = str(customer.get('email') or '').strip().lower()
        if contact_email and customer_email and contact_email == customer_email:
            candidates.append(account_row)
            continue

        candidate_names = _extract_candidate_names_from_application(customer)
        if contact_name and contact_name in candidate_names:
            candidates.append(account_row)

    if not candidates:
        return None

    candidates_sorted = sorted(
        candidates,
        key=lambda row: str(row.get('updated') or row.get('created') or ''),
        reverse=True,
    )
    return candidates_sorted[0]


def _resolve_customer_type(value: str | None) -> str:
    customer_type = str(value or '').upper().strip()
    if customer_type == 'ORGANIZATION':
        return 'ORG'
    return customer_type


def _get_contact_risk_country(contact: dict, account_row: dict | None, ibkr_detail: dict | None, account_contact_link: dict | None) -> str:
    if ibkr_detail:
        associated_people = ibkr_detail.get('associatedPersons') or []
        holder_details = _match_associated_person(contact=contact, associated_people=associated_people)
        residence = holder_details.get('residence') or {}
        return (
            residence.get('country')
            or ((holder_details.get('legalResidence') or {}).get('country') if isinstance(holder_details, dict) else '')
            or holder_details.get('countryOfLegalResidence')
            or ''
        )

    application_json = (account_row or {}).get('application_json') or {}
    customer = (application_json.get('customer') or {})
    customer_type = _resolve_customer_type(customer.get('type'))
    holder_details = {}

    if customer_type == 'INDIVIDUAL':
        holder_details = ((customer.get('accountHolder') or {}).get('accountHolderDetails') or [{}])[0] or {}
    elif customer_type == 'JOINT':
        joint = customer.get('jointHolders') or {}
        first_holder = (joint.get('firstHolderDetails') or [{}])[0] or {}
        second_holder = (joint.get('secondHolderDetails') or [{}])[0] or {}
        entity_id = account_contact_link.get('entity_id') if isinstance(account_contact_link, dict) else None
        if entity_id is not None and str(first_holder.get('entityId')) == str(entity_id):
            holder_details = first_holder
        elif entity_id is not None and str(second_holder.get('entityId')) == str(entity_id):
            holder_details = second_holder
        else:
            holder_details = first_holder if first_holder else second_holder
    elif customer_type == 'ORG':
        organization = customer.get('organization') or {}
        associated_individuals = ((organization.get('associatedEntities') or {}).get('associatedIndividuals')) or []
        entity_id = account_contact_link.get('entity_id') if isinstance(account_contact_link, dict) else None
        if entity_id is not None:
            holder_details = next((p for p in associated_individuals if str(p.get('entityId')) == str(entity_id)), {})
        if not holder_details and associated_individuals:
            holder_details = associated_individuals[0]

    return (
        (holder_details.get('residence') or {}).get('country')
        or (holder_details.get('countryOfLegalResidence') if isinstance(holder_details, dict) else '')
        or ((holder_details.get('legalResidence') or {}).get('country') if isinstance(holder_details, dict) else '')
        or ''
    )


def _compute_weighted_holder_risk_score(contact: dict, account_row: dict | None, ibkr_detail: dict | None, account_contact_link: dict | None) -> float:
    if not account_row:
        raise Exception('linked account not found for risk score calculation')
    advisor_code = account_row.get('advisor_code')
    if ibkr_detail:
        account_payload = ibkr_detail.get('account') or {}
        raw_customer_type = account_payload.get('applicantType')
        customer_type = _resolve_customer_type(raw_customer_type)
        if customer_type not in {'INDIVIDUAL', 'JOINT', 'ORG'}:
            raise Exception(f'invalid customer type for risk score calculation: {customer_type or "missing"}')

        associated_people = ibkr_detail.get('associatedPersons') or []
        holder_details = _match_associated_person(contact=contact, associated_people=associated_people)
        financial_information = ibkr_detail.get('financialInformation') or {}
        sources_of_wealth_raw = ibkr_detail.get('sourcesOfWealth') or []
        sources_of_wealth = []
        for row in sources_of_wealth_raw:
            if isinstance(row, dict):
                sources_of_wealth.append({'sourceType': row.get('label')})

        accounts = [{'margin': account_payload.get('margin'), 'tradingPermissions': []}]
        financial_info = {'investmentExperience': [], 'sourcesOfWealth': sources_of_wealth}
        if isinstance(financial_information, dict):
            investment_experience = financial_information.get('investmentExperience') or {}
            if isinstance(investment_experience, dict):
                financial_info['investmentExperience'] = [{'assetClass': k} for k in investment_experience.keys()]
        regulatory_info = {}
        organization = {}
        residence = holder_details.get('residence') or {}
        risk_country = (
            residence.get('country')
            or ((holder_details.get('legalResidence') or {}).get('country') if isinstance(holder_details, dict) else '')
            or holder_details.get('countryOfLegalResidence')
            or ''
        )
        account_countries = []
    else:
        application_json = (account_row or {}).get('application_json') or {}
        customer = (application_json.get('customer') or {})
        raw_customer_type = customer.get('type')
        customer_type = _resolve_customer_type(raw_customer_type)
        if customer_type not in {'INDIVIDUAL', 'JOINT', 'ORG'}:
            raise Exception(f'invalid customer type for risk score calculation: {customer_type or "missing"}')

        accounts = application_json.get('accounts') or []
        financial_info = {}
        regulatory_info = {}
        organization = customer.get('organization') or {}
        holder_details = {}
        if customer_type == 'INDIVIDUAL':
            holder_details = ((customer.get('accountHolder') or {}).get('accountHolderDetails') or [{}])[0] or {}
            financial_info = ((customer.get('accountHolder') or {}).get('financialInformation') or [{}])[0] or {}
            regulatory_info = ((customer.get('accountHolder') or {}).get('regulatoryInformation') or [{}])[0] or {}
        elif customer_type == 'JOINT':
            joint = customer.get('jointHolders') or {}
            first_holder = (joint.get('firstHolderDetails') or [{}])[0] or {}
            second_holder = (joint.get('secondHolderDetails') or [{}])[0] or {}
            entity_id = account_contact_link.get('entity_id') if isinstance(account_contact_link, dict) else None
            if entity_id is not None and str(first_holder.get('entityId')) == str(entity_id):
                holder_details = first_holder
            elif entity_id is not None and str(second_holder.get('entityId')) == str(entity_id):
                holder_details = second_holder
            else:
                holder_details = first_holder if first_holder else second_holder
            financial_info = (joint.get('financialInformation') or [{}])[0] or {}
            regulatory_info = (joint.get('regulatoryInformation') or [{}])[0] or {}
        else:
            associated_individuals = ((organization.get('associatedEntities') or {}).get('associatedIndividuals')) or []
            entity_id = account_contact_link.get('entity_id') if isinstance(account_contact_link, dict) else None
            if entity_id is not None:
                holder_details = next((p for p in associated_individuals if str(p.get('entityId')) == str(entity_id)), {})
            if not holder_details and associated_individuals:
                holder_details = associated_individuals[0]
            financial_info = (organization.get('financialInformation') or [{}])[0] or {}
            regulatory_info = (organization.get('regulatoryInformation') or [{}])[0] or {}

        account_countries = []
        for acc in accounts:
            for tp in (acc.get('tradingPermissions') or []):
                country = _to_upper(tp.get('country'))
                if country:
                    account_countries.append(country)
        risk_country = (
            (holder_details.get('residence') or {}).get('country')
            or (holder_details.get('countryOfLegalResidence') if isinstance(holder_details, dict) else '')
            or ((holder_details.get('legalResidence') or {}).get('country') if isinstance(holder_details, dict) else '')
            or ''
        )

    cr = _get_customer_risk_score(
        customer_type=customer_type,
        holder_details=holder_details if isinstance(holder_details, dict) else {},
        financial_info=financial_info if isinstance(financial_info, dict) else {},
        regulatory_info=regulatory_info if isinstance(regulatory_info, dict) else {},
        organization=organization if isinstance(organization, dict) else {},
        account_countries=account_countries,
        residence_country=risk_country,
    )
    jr = _get_jurisdiction_risk_score(risk_country)
    pr = _get_product_risk_score(accounts if isinstance(accounts, list) else [])
    dr = _get_delivery_channel_risk_score()
    ir = _get_introducer_risk_score(advisor_code)

    weighted = (
        cr * WEIGHTS['customerRisk']
        + jr * WEIGHTS['jurisdictionRisk']
        + pr * WEIGHTS['productRisk']
        + dr * WEIGHTS['deliveryRisk']
        + ir * WEIGHTS['introducerRisk']
    )
    return _clamp_risk_score(weighted)


def _get_sanctions_lists():
    global _sanctions_lists_cache
    if _sanctions_lists_cache is None:
        _sanctions_lists_cache = (
            get_ofac_sdn_list() or [],
            get_uk_sanctions_list() or [],
            get_un_sanctions_list() or [],
        )
    return _sanctions_lists_cache


def _append_index(index: dict[str, list[dict]], key: str, row: dict):
    if not key:
        return
    bucket = index.setdefault(key, [])
    bucket.append(row)


def _unique_rows(rows: list[dict]) -> list[dict]:
    seen = set()
    unique = []
    for row in rows or []:
        key = id(row)
        if key in seen:
            continue
        seen.add(key)
        unique.append(row)
    return unique


def _build_sanctions_match_indexes() -> tuple[dict[str, list[dict]], dict[str, list[dict]], dict[str, list[dict]]]:
    ofac_sdn_list, uk_sanctions_list, un_sanctions_list = _get_sanctions_lists()

    ofac_index: dict[str, list[dict]] = {}
    for row in ofac_sdn_list:
        if not isinstance(row, dict):
            continue
        _append_index(ofac_index, _normalize_name(row.get('name', '')), row)

    uk_index: dict[str, list[dict]] = {}
    for row in uk_sanctions_list:
        if not isinstance(row, dict):
            continue
        for i in range(1, 7):
            _append_index(uk_index, _normalize_name(row.get(f'Name {i}', '')), row)

    un_index: dict[str, list[dict]] = {}
    for row in un_sanctions_list:
        if not isinstance(row, dict):
            continue
        _append_index(un_index, _normalize_name(row.get('name', '')), row)
        aliases = row.get('aliases', '')
        if isinstance(aliases, str) and aliases.strip():
            for alias in aliases.split('|'):
                _append_index(un_index, _normalize_name(alias), row)

    return ofac_index, uk_index, un_index


def _get_sanctions_match_indexes() -> tuple[dict[str, list[dict]], dict[str, list[dict]], dict[str, list[dict]]]:
    global _sanctions_match_indexes_cache
    if _sanctions_match_indexes_cache is None:
        _sanctions_match_indexes_cache = _build_sanctions_match_indexes()
    return _sanctions_match_indexes_cache


def _get_ibkr_details_by_account_id() -> dict[str, dict]:
    global _ibkr_details_by_account_id_cache
    if _ibkr_details_by_account_id_cache is None:
        details = get_ibkr_details() or []
        _ibkr_details_by_account_id_cache = {}
        for detail in details:
            if not isinstance(detail, dict):
                continue
            account = detail.get('account') or {}
            account_id = str(account.get('accountId') or '').strip()
            if account_id:
                _ibkr_details_by_account_id_cache[account_id] = detail
    return _ibkr_details_by_account_id_cache


def _match_associated_person(contact: dict, associated_people: list[dict]) -> dict:
    contact_name = _normalize_name(contact.get('name') or '')
    contact_email = str(contact.get('email') or '').strip().lower()
    for person in associated_people or []:
        if not isinstance(person, dict):
            continue
        first = str(person.get('firstName') or '').strip()
        middle = str(person.get('middleName') or '').strip()
        last = str(person.get('lastName') or '').strip()
        person_names = {
            _normalize_name(f'{first} {last}'.strip()),
            _normalize_name(f'{first} {middle} {last}'.strip()),
        }
        person_email = str(person.get('email') or '').strip().lower()
        if contact_name and contact_name in person_names:
            return person
        if contact_email and person_email and contact_email == person_email:
            return person
    return (associated_people or [{}])[0] or {}


@handle_exception
def create_contact_screening_from_contact_id(contact_id: str = None, created: Optional[str] = None):
    if not contact_id:
        raise Exception('contact_id is required')

    contact_rows = db.read(table=contact_table, query={'id': contact_id}) or []
    if len(contact_rows) == 0:
        raise Exception('contact not found')

    contact = contact_rows[0]
    contact_name = (contact.get('name') or '').strip()
    normalized_contact_name = _normalize_name(contact_name)
    if not normalized_contact_name:
        raise Exception('contact name is required for screening')

    latest_contact_link = _pick_latest_contact_link(contact_id)
    account_row = None
    if latest_contact_link and latest_contact_link.get('account_id'):
        account_rows = db.read(table='account', query={'id': latest_contact_link.get('account_id')}) or []
        account_row = account_rows[0] if account_rows else None
        if not account_row:
            raise Exception(f"linked account not found: {latest_contact_link.get('account_id')}")
    else:
        account_row = _find_latest_account_from_application_json(contact)
        if account_row:
            logger.info(
                f"Resolved fallback account from application_json for unlinked contact_id={contact_id} "
                f"account_id={account_row.get('id')}"
            )
        else:
            raise Exception('contact is not linked to any account and no application_json fallback account was found')

    ibkr_account_number = str(account_row.get('ibkr_account_number') or '').strip()
    ibkr_detail = _get_ibkr_details_by_account_id().get(ibkr_account_number) if ibkr_account_number else None

    screening_payload = build_contact_screening_payload(
        contact=contact,
        account_row=account_row,
        ibkr_detail=ibkr_detail,
        account_contact_link=latest_contact_link,
        created=created,
    )
    return create_contact_screening(**screening_payload)


@handle_exception
def read_contact_screenings(contact_id: str = None):
    if not contact_id:
        raise Exception('contact_id is required')
    return db.read(table=contact_screening_table, query={'contact_id': contact_id}) or []
