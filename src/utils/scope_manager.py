from functools import wraps
from flask import request, jsonify
from flask_jwt_extended import get_jwt, get_jwt_identity
from src.utils.logger import logger

public_routes = ['docs', 'index', 'token', 'oauth.login', 'oauth.create']

def verify_scope(required_scope):
    def decorator(fn):
        @wraps(fn)
        def wrapper(*args, **kwargs):
            logger.info(f'Verifying if user has {required_scope} scope')
            if request.endpoint in public_routes:
                return fn(*args, **kwargs)
            
            claims = get_jwt()
            user_scopes = claims.get('scopes', '').split()
            
            # "all" scope has access to everything
            if 'all' in user_scopes:
                logger.success(f'User has all scope, granting access')
                return fn(*args, **kwargs)
            
            # Check if user has the exact scope or a parent scope
            scope_parts = required_scope.split('/')
            for i in range(len(scope_parts)):
                partial_scope = '/'.join(scope_parts[:i+1])
                if partial_scope in user_scopes:
                    logger.success(f'User has {partial_scope} scope, granting access')
                    return fn(*args, **kwargs)
            
            logger.warning(f'User attempted to access {required_scope} without proper authorization. User scopes: {user_scopes}')
            return jsonify({"error": "Insufficient scope"}), 403
        return wrapper
    return decorator

def enforce_user_filter(field_name='UserID'):
    """
    Decorator that enforces user-specific filtering for routes.
    This decorator should be applied after @verify_scope.
    
    :param field_name: The field name to filter by (defaults to 'UserID')
    """
    def decorator(fn):
        @wraps(fn)
        def wrapper(*args, **kwargs):
            claims = get_jwt()
            current_user = get_jwt_identity()
            user_scopes = claims.get('scopes', '').split()
            logger.info(f'Enforcing user filter for current user: {current_user}')
            
            # If user has 'all' scope, no filtering needed
            if 'all' in user_scopes:
                logger.success(f'User has all scope, no filtering needed')
                return fn(*args, **kwargs)
            
            # Get the request payload
            payload = request.get_json(force=True)
            
            # Handle different HTTP methods and payload structures
            if request.method == 'POST':
                # For create operations, add user ID to data
                if 'data' in payload:
                    payload['data'][field_name] = current_user
                
                # For read/update operations with query
                if 'query' in payload:
                    if not isinstance(payload['query'], dict):
                        payload['query'] = {}
                    payload['query'][field_name] = current_user
                else:
                    # If no query exists, create one with the user filter
                    payload['query'] = {field_name: current_user}
            
            logger.success(f'Enforced user filter for {field_name}')
            return fn(*args, **kwargs)
            
        return wrapper
    return decorator
       