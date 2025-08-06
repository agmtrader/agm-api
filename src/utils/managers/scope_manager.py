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
            
            logger.error(f'User attempted to access {required_scope} without proper authorization. User scopes: {user_scopes}')
            return jsonify({"error": "Insufficient scope"}), 403
        return wrapper
    return decorator