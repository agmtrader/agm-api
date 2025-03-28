from src.utils.exception import handle_exception
import json
from src.helpers.database import Firebase
from src.utils.logger import logger

logger.announcement('Initializing Notifications Service', type='info')
logger.announcement('Initialized Notifications Service', type='success')

Database = Firebase()

notification_types = [
    'account_applications',
    'risk_profiles',
    'tickets'
]

@handle_exception
def create_notification(notification, type_name):
    logger.announcement(f'Creating notification for {type_name}')
    path = f'db/notifications/{type_name}'
    notification = Database.create(
        path=path,
        data=notification,
        id=notification.get('NotificationID')
    )
    logger.announcement(f'Notification created for {type_name}')
    return notification

@handle_exception
def read_notifications_by_type(type_name):
    path = f'db/notifications/{type_name}'
    notifications = Database.read(path=path)
    return notifications

@handle_exception
def read_all_notifications():
    logger.announcement('Reading all notifications')
    notifications = []
    for type_name in notification_types:
        type_notifications = read_notifications_by_type(type_name)
        notifications.extend(type_notifications)

    logger.announcement(f'Found {len(notifications)} notifications')
    return notifications