from flask import Blueprint, request
from uuid import UUID

from src.components.clients.application_providers import (
    create_application_provider,
    read_application_providers,
)
from src.utils.response import format_response

bp = Blueprint('application_providers', __name__)


@bp.route('/read', methods=['GET'])
@format_response
def read_route():
    provider_id = request.args.get('id')
    if provider_id:
        try:
            UUID(provider_id)
        except ValueError:
            return {"error": "id must be a valid UUID"}, 400
    return read_application_providers({'id': provider_id} if provider_id else {})


@bp.route('/create', methods=['POST'])
@format_response
def create_route():
    payload = request.get_json(force=True) or {}
    return create_application_provider(payload.get('application_provider'))
