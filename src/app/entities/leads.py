from flask import Blueprint, request
from src.components.entities.leads import read_leads, update_lead, create_lead, read_follow_ups, update_follow_up, delete_follow_up, create_follow_up
from src.utils.response import format_response

bp = Blueprint('leads', __name__)

# Leads
@bp.route('/create', methods=['POST'])
@format_response
def create_lead_route():
    payload = request.get_json(force=True)
    lead = payload.get('lead', None)
    follow_ups = payload.get('follow_ups', None)
    return create_lead(lead=lead, follow_ups=follow_ups)

@bp.route('/read', methods=['GET'])
@format_response
def read_leads_route(): 
    query = {}
    id = request.args.get('id', None)
    lead_id = request.args.get('lead_id', None)
    if id:
        query['id'] = id
    if lead_id:
        query['lead_id'] = lead_id
    return read_leads(query=query)

@bp.route('/update', methods=['POST'])
@format_response
def update_lead_route():
    payload = request.get_json(force=True)
    lead = payload.get('lead', None)
    query = payload.get('query', None)
    return update_lead(query=query, lead=lead)

# Follow Ups
@bp.route('/follow_up/create', methods=['POST'])
@format_response
def create_follow_up_route():
    payload = request.get_json(force=True)
    lead_id = payload.get('lead_id', None)
    follow_up = payload.get('follow_up', None)
    return create_follow_up(lead_id=lead_id, follow_up=follow_up)

@bp.route('/follow_up/read', methods=['GET'])
@format_response
def read_follow_ups_route():
    query = request.args.get('query', {})
    return read_follow_ups(query=query)

@bp.route('/follow_up/update', methods=['POST'])
@format_response
def update_follow_up_route():
    payload = request.get_json(force=True)
    follow_up = payload.get('follow_up', None)
    lead_id = payload.get('lead_id', None)
    follow_up_id = payload.get('follow_up_id', None)
    return update_follow_up(lead_id=lead_id, follow_up_id=follow_up_id, follow_up=follow_up)

@bp.route('/follow_up/delete', methods=['POST'])
@format_response
def delete_follow_up_route():
    payload = request.get_json(force=True)
    lead_id = payload.get('lead_id', None)
    follow_up_id = payload.get('follow_up_id', None)
    return delete_follow_up(lead_id=lead_id, follow_up_id=follow_up_id)