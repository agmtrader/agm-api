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

def enforce_user_filter():
    """
    Decorator that prepares user-specific filtering for routes by injecting `invoking_user_id`.
    This decorator should be applied after @verify_scope.
    The wrapped function (and subsequently the component layer) is responsible for using 
    the `invoking_user_id` to filter data according to the relational schema.
    """
    def decorator(fn):
        @wraps(fn)
        def wrapper(*args, **kwargs):
            # Access public_routes from the module's scope
            # If the route is public, it might not have JWT context or require user filtering.
            if request.endpoint in public_routes:
                logger.info(f'Endpoint {request.endpoint} is public. Injecting invoking_user_id=None.')
                kwargs['invoking_user_id'] = None
                return fn(*args, **kwargs)

            # For non-public routes, JWT is expected (as verify_scope would have run and checked)
            claims = get_jwt()
            current_user_id = get_jwt_identity()
            user_scopes = claims.get('scopes', '').split()
            
            logger.info(f'Preparing user filter for current user: {current_user_id} on endpoint {request.endpoint} for function {fn.__name__}')
            
            # If user has 'all' scope, pass None as invoking_user_id.
            # Components can use this to bypass user-specific filtering if applicable.
            if 'all' in user_scopes:
                logger.success(f'User has "all" scope. Injecting invoking_user_id=None.')
                kwargs['invoking_user_id'] = None
            else:
                if current_user_id is None:
                    # This scenario (no user_id for a non-'all' scope user on a non-public route)
                    # might indicate an issue or require specific error handling.
                    logger.warning(f'User ID is None for a non-"all" scope user on non-public route {request.endpoint}. Injecting invoking_user_id=None.')
                    kwargs['invoking_user_id'] = None # Or raise an error, e.g., return jsonify({"error": "User identity not found"}), 401
                else:
                    logger.success(f'Injecting invoking_user_id={current_user_id} for {fn.__name__}')
                    kwargs['invoking_user_id'] = current_user_id
            
            return fn(*args, **kwargs)
            
        return wrapper
    return decorator
       