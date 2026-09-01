from src.utils.connectors.supabase import db
from src.utils.exception import ServiceError, handle_exception

TABLE = 'application_provider'


@handle_exception
def create_application_provider(data: dict | None = None) -> dict:
    data = data or {}
    color_scheme = data.get('color_scheme')
    if not isinstance(color_scheme, dict):
        raise ServiceError('color_scheme must be an object', status_code=400)
    provider_id = db.create(table=TABLE, data={'color_scheme': color_scheme})
    return (db.read(table=TABLE, query={'id': provider_id}) or [{'id': provider_id}])[0]


@handle_exception
def read_application_providers(query: dict | None = None) -> list[dict]:
    return db.read(table=TABLE, query=query or {})
