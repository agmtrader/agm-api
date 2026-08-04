from src.utils.exception import ServiceError, handle_exception
from src.utils.connectors.gmail import GmailConnector
from src.utils.connectors.supabase import db
from src.utils.logger import logger
from src.utils.connectors.ibkr_web_api import IBKRWebAPI
from sqlalchemy import text
import re
import uuid

logger.announcement('Initializing Accounts Service', type='info')
ibkr_web_api = IBKRWebAPI()
logger.announcement('Initialized Accounts Service', type='success')

table = 'account'
account_contact_table = 'account_contact'
SENSITIVE_ACCOUNT_FIELDS = {
    'ibkr_password_secret_id',
    'temporal_password_secret_id',
}

@handle_exception
def link_account_contact(account_contact: dict = None) -> dict:
    if not account_contact:
        raise Exception('account_contact payload is required')
    return {'id': db.create(table=account_contact_table, data=account_contact)}

@handle_exception
def read_account_contacts(query: dict = None) -> list:
    return db.read(table=account_contact_table, query=query or {})

@handle_exception
def update_account_contact(query: dict = None, account_contact: dict = None) -> dict:
    if not query:
        raise Exception('query is required')
    if not account_contact:
        raise Exception('account_contact payload is required')
    db.update(table=account_contact_table, query=query, data=account_contact)
    return {'status': 'success'}


def _sanitize_account(account: dict = None):
    if account is None:
        return None
    return {key: value for key, value in account.items() if key not in SENSITIVE_ACCOUNT_FIELDS}

def _sanitize_accounts(accounts: list = None):
    return [_sanitize_account(account) for account in accounts or []]

def _vault_secret_name(account_id: str, field: str) -> str:
    return f'account:{account_id}:{field}'

def _create_vault_secret(secret: str, name: str, description: str) -> str | None:
    if secret is None or secret == '':
        return None

    @db.with_session
    def _create_secret(session, secret: str, name: str, description: str):
        result = session.execute(
            text('select vault.create_secret(:secret, :name, :description) as id'),
            {'secret': secret, 'name': name, 'description': description},
        ).mappings().first()
        if not result or not result.get('id'):
            raise Exception('Vault did not return a secret id')
        return str(result['id'])

    return _create_secret(secret, name, description)

def _read_vault_secret(secret_id: str | None) -> str | None:
    if not secret_id:
        return None

    @db.with_session
    def _read_secret(session, secret_id: str):
        result = session.execute(
            text('select decrypted_secret from vault.decrypted_secrets where id = :secret_id'),
            {'secret_id': secret_id},
        ).mappings().first()
        return result.get('decrypted_secret') if result else None

    return _read_secret(str(secret_id))

def _prepare_account_secrets(account: dict, account_id: str | None = None) -> dict:
    if account is None:
        return account

    prepared = dict(account)
    prepared.pop('ibkr_password_secret_id', None)
    prepared.pop('temporal_password_secret_id', None)
    secret_fields = {
        'ibkr_password': 'ibkr_password_secret_id',
        'temporal_password': 'temporal_password_secret_id',
    }

    for password_field, secret_id_field in secret_fields.items():
        if password_field not in prepared:
            continue

        password_value = prepared.pop(password_field)
        prepared[secret_id_field] = None

        if password_value is None or password_value == '':
            continue

        if not account_id:
            raise Exception(f'account_id is required to store {password_field} in Vault')

        prepared[secret_id_field] = _create_vault_secret(
            password_value,
            _vault_secret_name(account_id, password_field),
            f'{password_field} for account {account_id}',
        )

    return prepared

def _resolve_account_secret(account: dict, secret_id_field: str) -> str | None:
    secret = _read_vault_secret(account.get(secret_id_field))
    if secret:
        return secret

    return None

@handle_exception
def create_account(account: dict = None) -> dict:
    logger.info(f"Attempting to create account with data: {account}")
    pending_secrets = {}
    account_data = dict(account or {})
    account_data.pop('ibkr_password_secret_id', None)
    account_data.pop('temporal_password_secret_id', None)
    for password_field in ('ibkr_password', 'temporal_password'):
        if password_field in account_data:
            pending_secrets[password_field] = account_data.pop(password_field)

    account_id = db.create(table=table, data=account_data)

    if pending_secrets:
        secret_update = _prepare_account_secrets(pending_secrets, account_id=account_id)
        if secret_update:
            db.update(table=table, query={'id': account_id}, data=secret_update)

    return {'id': account_id}

@handle_exception
def read_accounts(query: dict = None) -> list:
    accounts = db.read(table=table, query=query)
    return _sanitize_accounts(accounts)

@handle_exception
def read_instructions(query: dict = None) -> list:
    instructions = db.read(table='account_instruction', query=query)
    return instructions

@handle_exception
def update_account(query: dict = None, account: dict = None) -> dict:
    existing_accounts = db.read(table=table, query=query) or []
    if len(existing_accounts) == 0:
        raise Exception('Account not found')
    if len(existing_accounts) > 1:
        raise Exception('Multiple accounts found')

    prepared_account = _prepare_account_secrets(account, account_id=existing_accounts[0].get('id'))
    db.update(table=table, query=query, data=prepared_account)
    return {'status': 'success'}

@handle_exception
def send_account_credentials_email(
    account_id: str = None,
    client_email: str = None,
    lang: str = 'es',
    cc: str = '',
    send_welcome: bool = False,
    client_name: str = '',
) -> dict:
    if not account_id:
        raise Exception('Missing account_id')
    if not client_email:
        raise Exception('Missing client_email')

    accounts = db.read(table=table, query={'id': account_id}) or []
    if len(accounts) == 0:
        raise Exception('Account not found')
    if len(accounts) > 1:
        raise Exception('Multiple accounts found')

    account = accounts[0]
    username = account.get('ibkr_username')
    password = _resolve_account_secret(account, 'ibkr_password_secret_id')
    if not username or not password:
        raise Exception('Account credentials not found')

    send_credentials_email(
        content={'username': username, 'password': password},
        client_email=client_email,
        lang=lang,
        cc=cc,
    )

    if send_welcome:
        send_welcome_email(
            content={'client_name': client_name or 'Client'},
            client_email=client_email,
            lang=lang,
        )
        db.update(table=table, query={'id': account_id}, data={'emailed_credentials': True})

    return {'status': 'success'}

@handle_exception
def send_credentials_email(content, client_email, lang='es', cc=''):
    gmail = GmailConnector()
    subject = 'Credenciales de acceso para cuenta AGM' if lang == 'es' else 'Access Credentials for AGM Account'
    return gmail.send_email(content, client_email, subject, f'credentials_{lang}', bcc='', cc='jc@agmtechnology.com,hc@agmtechnology.com,mjc@agmtechnology.com,' + cc)

@handle_exception
def send_transfer_instructions_email(content, client_email, lang='es', cc='', initial=True):
    gmail = GmailConnector()
    subject = 'Instrucciones de transferencia' if lang == 'es' else 'Transfer Instructions'
    template = 'transfer_instructions' if initial else 'transfer_instructions_existing'
    return gmail.send_email(content, client_email, subject, f'{template}_{lang}', bcc='', cc='jc@agmtechnology.com,hc@agmtechnology.com,mjc@agmtechnology.com,' + cc)

@handle_exception
def send_welcome_email(content, client_email, lang='es', cc=''):
    gmail = GmailConnector()
    subject = 'Bienvenido a AGM Technology' if lang == 'es' else 'Welcome to AGM Technology'
    return gmail.send_email(content, client_email, subject, f'welcome_{lang}', bcc='', cc='jc@agmtechnology.com,hc@agmtechnology.com,mjc@agmtechnology.com,' + cc)

@handle_exception
def send_funding_notification_email(content, client_email, lang='es', cc='', days_since_opened=None, notice_number=None):
    gmail = GmailConnector()
    subject = 'Recordatorio de Fondeo' if lang == 'es' else 'Funding Reminder'
    return gmail.send_email(content, client_email, subject, f'funding_notification_{lang}', bcc='', cc='jc@agmtechnology.com,hc@agmtechnology.com,mjc@agmtechnology.com,' + cc)

@handle_exception
def send_missing_documents_email(content, client_email, missing_type='multiple', lang='en', cc=''):
    normalized_lang = 'es' if lang == 'es' else 'en'
    normalized_type = missing_type if missing_type in {'poi', 'poa', 'sow', 'multiple'} else 'multiple'
    payload = content or {}
    is_company = bool(payload.get('is_company_contact'))
    company_name = str(payload.get('company_name') or '').strip()
    if is_company and not company_name:
        raise ServiceError('company_name is required for company missing-documents emails', status_code=400)
    subject = (
        ('Documentos pendientes para su cuenta corporativa' if is_company else 'Documentos pendientes para su cuenta personal')
        if normalized_lang == 'es' else
        ('Pending Documents for Your Corporate Account' if is_company else 'Pending Documents for Your Personal Account')
    )
    if payload.get('important'):
        subject = f"{'IMPORTANTE' if normalized_lang == 'es' else 'IMPORTANT'}: {subject}"
    gmail = GmailConnector()
    return gmail.send_email({**payload, 'company_name': company_name, 'missing_type': normalized_type}, client_email, subject, f'missing_documents_{normalized_lang}', bcc='', cc='jc@agmtechnology.com,hc@agmtechnology.com,mjc@agmtechnology.com,' + cc)

@handle_exception
def send_to_ibkr(account_id: str = None, master_account: str = None, application: dict = None) -> dict:
    if not account_id:
        raise Exception('Missing account_id')

    accounts = db.read(table=table, query={'id': account_id}) or []
    if len(accounts) == 0:
        raise Exception(f'Account not found for id={account_id}')
    if len(accounts) > 1:
        raise Exception(f'Multiple accounts found for id={account_id}')

    account = accounts[0]
    application_json = application if isinstance(application, dict) else account.get('application_json')
    if not application_json:
        raise Exception('Account has no application_json')

    resolved_master_account = master_account or account.get('master_account')
    if not resolved_master_account:
        raise Exception('Missing master_account (payload and account row are empty)')

    return ibkr_web_api.send_to_ibkr(
        application={'application': application_json},
        master_account=resolved_master_account
    )

"""
Account Management API
"""
@handle_exception
def read_account_details(account_id: str = None, master_account: str = None) -> dict:
    return ibkr_web_api.get_account_details(account_id=account_id, master_account=master_account)

@handle_exception
def get_pending_tasks(account_id: str = None, master_account: str = None) -> list:
    return ibkr_web_api.get_pending_tasks(account_id=account_id, master_account=master_account)

@handle_exception
def get_account_statements(
    account_id: str = None,
    start_date: str = None,
    end_date: str = None,
    master_account: str = None,
    language: str = 'en'
) -> dict:
    """Get account statements via IBKR API."""
    return ibkr_web_api.get_account_statements(
        account_id=account_id,
        start_date=start_date,
        end_date=end_date,
        master_account=master_account,
        language=language,
    )

@handle_exception
def submit_documents(document_submission: dict = None, master_account: str = None) -> dict:
    return ibkr_web_api.submit_documents(document_submission=document_submission, master_account=master_account)

@handle_exception
def apply_fee_template(account_id: str = None, template_name: str = None, master_account: str = None) -> dict:
    """Apply a fee template to an account via IBKR API."""
    return ibkr_web_api.apply_fee_template(account_id=account_id, template_name=template_name, master_account=master_account)

@handle_exception
def add_trading_permissions(account_id: str = None, trading_permissions: list = None, master_account: str = None) -> dict:
    """Add trading permissions to an account via IBKR API."""
    return ibkr_web_api.add_trading_permissions(account_id=account_id, trading_permissions=trading_permissions, master_account=master_account)

@handle_exception
def add_clp_capability(account_id: str = None, document_submission: dict = None, master_account: str = None) -> dict:
    """Add CLP capability to an account via IBKR API."""
    return ibkr_web_api.add_clp_capability(account_id=account_id, document_submission=document_submission, master_account=master_account)

@handle_exception
def update_account_alias(account_id: str = None, new_alias: str = None, master_account: str = None) -> dict:
    """Update account alias via IBKR API."""
    return ibkr_web_api.update_account_alias(account_id=account_id, new_alias=new_alias, master_account=master_account)

@handle_exception
def change_financial_information(account_id: str = None, new_financial_information: dict = None, master_account: str = None) -> dict:
    """Change account financial information via IBKR API."""
    return ibkr_web_api.change_financial_information(
        account_id=account_id,
        new_financial_information=new_financial_information,
        master_account=master_account
    )

@handle_exception
def change_account_holder_external_id(account_id: str = None, id: str = None, master_account: str = None) -> dict:
    """Change account holder external id via IBKR API using IBKR id (entityId)."""
    external_id = str(uuid.uuid4())
    return ibkr_web_api.change_account_holder_external_id(
        accountId=account_id,
        entityId=id,
        external_id=external_id,
        master_account=master_account
    )

@handle_exception
def deposit_funds(master_account: str = None, instruction: dict = None, account_id: str = None) -> dict:
    """Deposit funds via IBKR API."""
    client_instruction_id = db.create(table='account_instruction', data={'account_id': account_id})
    instruction['clientInstructionId'] = client_instruction_id
    return ibkr_web_api.deposit_funds(master_account=master_account, instruction=instruction)

@handle_exception
def withdraw_funds(master_account: str = None, instruction: dict = None, account_id: str = None) -> dict:
    """Withdraw funds via IBKR API."""
    client_instruction_id = db.create(table='account_instruction', data={'account_id': account_id})
    instruction['clientInstructionId'] = client_instruction_id
    return ibkr_web_api.withdraw_funds(master_account=master_account, instruction=instruction)

@handle_exception
def get_status_of_instruction(client_instruction_id: str = None) -> dict:
    """Get the status of a banking instruction via IBKR API."""
    return ibkr_web_api.get_status_of_instruction(client_instruction_id=client_instruction_id)
    
@handle_exception
def view_withdrawable_cash(master_account: str = None, account_id: str = None, client_instruction_id: str = None) -> dict:
    """View the withdrawable cash for the given account via IBKR API."""
    return ibkr_web_api.view_withdrawable_cash(master_account=master_account, account_id=account_id, client_instruction_id=client_instruction_id)

@handle_exception
def view_active_bank_instructions(master_account: str = None, account_id: str = None, client_instruction_id: str = None, bank_instruction_method: str = None) -> dict:
    """View the active bank instructions for the given account via IBKR API."""
    return ibkr_web_api.view_active_bank_instructions(master_account=master_account, account_id=account_id, client_instruction_id=client_instruction_id, bank_instruction_method=bank_instruction_method)

@handle_exception
def get_wire_instructions(master_account: str = None, account_id: str = None, currency: str = "USD") -> dict:
    """Get wire instructions via IBKR API."""
    return ibkr_web_api.get_wire_instructions(master_account=master_account, account_id=account_id, currency=currency)

# Enums
@handle_exception
def get_product_country_bundles() -> dict:
    """Get product country bundles enumeration via IBKR API."""
    return ibkr_web_api.get_product_country_bundles()

@handle_exception
def get_forms(forms: list = None, master_account: str = None) -> dict:
    return ibkr_web_api.get_forms(forms=forms, master_account=master_account)

@handle_exception
def get_financial_ranges() -> dict:
    """Get financial ranges via IBKR API."""
    return ibkr_web_api.get_financial_ranges()

@handle_exception
def get_business_and_occupation() -> dict:
    """Get business and occupation via IBKR API."""
    return ibkr_web_api.get_business_and_occupation()
