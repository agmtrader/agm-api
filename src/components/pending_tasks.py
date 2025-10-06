from src.utils.exception import handle_exception
from src.utils.connectors.supabase import db
from src.utils.logger import logger

logger.announcement('Initializing Pending Tasks Service', type='info')
logger.announcement('Initialized Pending Tasks Service', type='success')
    
@handle_exception
def create_pending_task(task: dict = None, follow_ups: list = None):
    
    pending_task_id = db.create(table='pending_task', data=task)
    
    for follow_up in follow_ups:
        follow_up['pending_task_id'] = pending_task_id
        db.create(table='pending_task_follow_up', data=follow_up)
    
    return {'id': pending_task_id}

@handle_exception
def create_follow_up(task_id: str = None, follow_up: dict = None):
    follow_up['pending_task_id'] = task_id
    follow_up_id = db.create(table='pending_task_follow_up', data=follow_up)
    return {'id': follow_up_id}

@handle_exception
def read_pending_tasks(query: dict = None):
    pending_tasks = db.read(table='pending_task', query=query)
    if 'id' in query:
        pending_task_follow_ups_query = {'pending_task_id': query['id']}
    else:
        pending_task_follow_ups_query = {}
    pending_task_follow_ups = db.read(table='pending_task_follow_up', query=pending_task_follow_ups_query)

    return {'pending_tasks': pending_tasks, 'follow_ups': pending_task_follow_ups}

@handle_exception
def update_pending_task(query: dict = None, task: dict = None):
    db.update(table='pending_task', query=query, data=task)
    return {'id': query['id']}

@handle_exception
def update_follow_up(task_id: str = None, follow_up_id: str = None, follow_up: dict = None):
    query = {'pending_task_id': task_id, 'id': follow_up_id}
    db.update(table='pending_task_follow_up', query=query, data=follow_up)
    return {'id': query['id']}

@handle_exception
def delete_follow_up(task_id: str = None, follow_up_id: str = None):
    query = {'pending_task_id': task_id, 'id': follow_up_id}
    db.delete(table='pending_task_follow_up', query=query)
    return {'id': query['id']}