from flask import Blueprint, request
from src.components.pending_tasks import create_pending_task, read_pending_tasks, update_pending_task, create_pending_task_follow_up, update_pending_task_follow_up, delete_pending_task_follow_up
from src.utils.managers.scope_manager import verify_scope
from src.utils.response import format_response

bp = Blueprint('pending_tasks', __name__)

# Pending Tasks
@bp.route('/create', methods=['POST'])
@format_response
def create_pending_task_route():
    payload = request.get_json(force=True)
    task = payload.get('task', None)
    follow_ups = payload.get('follow_ups', None)
    return create_pending_task(task=task, follow_ups=follow_ups)

@bp.route('/read', methods=['GET'])
@format_response
def read_pending_tasks_route():
    query = {}
    id = request.args.get('id', None)
    account_id = request.args.get('account_id', None)
    if id:
        query['id'] = id
    if account_id:
        query['account_id'] = account_id
    return read_pending_tasks(query=query)

@bp.route('/update', methods=['POST'])
@format_response
def update_pending_task_route():
    payload = request.get_json(force=True)
    query = payload.get('query', None)
    task = payload.get('task', None)
    return update_pending_task(query=query, task=task)

# Pending Task Follow Ups
@bp.route('/follow_ups/create', methods=['POST'])
@format_response
def create_pending_task_follow_up_route():
    payload = request.get_json(force=True)
    follow_up = payload.get('follow_up', None)
    pending_task_id = payload.get('pending_task_id', None)
    return create_pending_task_follow_up(follow_up=follow_up, pending_task_id=pending_task_id)

@bp.route('/follow_ups/update', methods=['POST'])
@format_response
def update_pending_task_follow_up_route():
    payload = request.get_json(force=True)
    query = payload.get('query', None)
    follow_up = payload.get('follow_up', None)
    return update_pending_task_follow_up(query=query, follow_up=follow_up)

@bp.route('/follow_ups/delete', methods=['POST'])
@format_response
def delete_pending_task_follow_up_route():
    payload = request.get_json(force=True)
    query = payload.get('query', None)
    return delete_pending_task_follow_up(query=query)