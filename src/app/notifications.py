from flask import Blueprint, request
from src.components.notifications import create_notification, read_notifications_by_type, read_all_notifications
from src.utils.scope_manager import verify_scope

bp = Blueprint('notifications', __name__)
    
@bp.route('/create', methods=['POST'])
@verify_scope('notifications/create')
def create():
    data = request.get_json(force=True)
    notification = data['notification']
    t = data['type']
    return create_notification(notification, t)

@bp.route('/read_by_type', methods=['POST'])
@verify_scope('notifications/read_by_type')
def read_by_type():
    data = request.get_json(force=True)
    t = data['type']
    return read_notifications_by_type(t) 

@bp.route('/read', methods=['GET'])
@verify_scope('notifications/read')
def read():
    return read_all_notifications()