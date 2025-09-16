from flask import Blueprint, request
from src.components.pending_tasks import create_pending_task, read_pending_tasks, update_pending_task
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