from flask import Blueprint, request
from src.components.leads import read_leads, update_lead, create_lead, delete_lead, read_follow_ups, update_follow_up, delete_follow_up, create_follow_up
from src.utils.managers.scope_manager import verify_scope

bp = Blueprint('leads', __name__)

@bp.route('/create', methods=['POST'])
@verify_scope('leads/create')
def create_lead_route():
    payload = request.get_json(force=True)
    lead = payload.get('lead', None)
    follow_ups = payload.get('follow_ups', None)
    return create_lead(lead=lead, follow_ups=follow_ups)

@bp.route('/create_follow_up', methods=['POST'])
@verify_scope('leads/create_follow_up')
def create_follow_up_route():
    payload = request.get_json(force=True)
    lead_id = payload.get('lead_id', None)
    follow_up = payload.get('follow_up', None)
    return create_follow_up(lead_id=lead_id, follow_up=follow_up)

@bp.route('/read', methods=['POST'])
@verify_scope('leads/read')
def read_leads_route():
    payload = request.get_json(force=True)
    query = payload.get('query', None)
    return read_leads(query=query)

@bp.route('/read_follow_ups', methods=['POST'])
@verify_scope('leads/read_follow_ups')
def read_follow_ups_route():
    payload = request.get_json(force=True)
    query = payload.get('query', None)
    return read_follow_ups(query=query)

@bp.route('/update', methods=['POST'])
@verify_scope('leads/update')
def update_lead_route():
    payload = request.get_json(force=True)
    lead = payload.get('lead', None)
    query = payload.get('query', None)
    return update_lead(query=query, lead=lead)

@bp.route('/update_follow_up', methods=['POST'])
@verify_scope('leads/update_follow_up')
def update_follow_up_route():
    payload = request.get_json(force=True)
    follow_up = payload.get('follow_up', None)
    lead_id = payload.get('lead_id', None)
    follow_up_id = payload.get('follow_up_id', None)
    return update_follow_up(lead_id=lead_id, follow_up_id=follow_up_id, follow_up=follow_up)

@bp.route('/delete', methods=['POST'])
@verify_scope('leads/delete')
def delete_lead_route():
    payload = request.get_json(force=True)
    query = payload.get('query', None)
    return delete_lead(query=query)

@bp.route('/delete_follow_up', methods=['POST'])
@verify_scope('leads/delete_follow_up')
def delete_follow_up_route():
    payload = request.get_json(force=True)
    lead_id = payload.get('lead_id', None)
    follow_up_id = payload.get('follow_up_id', None)
    return delete_follow_up(lead_id=lead_id, follow_up_id=follow_up_id)