from google.cloud import secretmanager
import json
import time
from src.utils.exception import handle_exception
from src.utils.logger import logger
from threading import Lock
from typing import Dict, Tuple, Union
import time

# Thread-safe cache implementation
_secret_cache: Dict[str, Tuple[str, float]] = {}
_cache_lock = Lock()
_CACHE_EXPIRATION_SECONDS = 3600  # 1 hour cache expiration

def _get_cached_secret(secret_id: str) -> Union[str, None]:
    """
    Retrieve a secret from cache if it exists and hasn't expired.
    Returns None if secret is not in cache or has expired.
    """
    with _cache_lock:
        if secret_id not in _secret_cache:
            return None
        
        secret_value, expiration_time = _secret_cache[secret_id]
        if time.time() > expiration_time:
            del _secret_cache[secret_id]
            return None
            
        return secret_value

def _cache_secret(secret_id: str, secret_value: str) -> None:
    """
    Store a secret in cache with expiration time.
    """
    with _cache_lock:
        expiration_time = time.time() + _CACHE_EXPIRATION_SECONDS
        _secret_cache[secret_id] = (secret_value, expiration_time)

def get_secret(secret_id: str):
    try:
        # Check cache first
        cached_secret = _get_cached_secret(secret_id)
        if cached_secret is not None:
            logger.info(f"Retrieved secret from cache: {secret_id}")
            return cached_secret

        logger.info(f"Fetching secret: {secret_id}")

        # Initialize the Secret Manager client
        client = secretmanager.SecretManagerServiceClient()

        # Define your project ID and secret name
        project_id = "agm-datalake"
        version_id = "1"

        # Build the secret version path
        secret_path = f"projects/{project_id}/secrets/{secret_id}/versions/{version_id}"

        # Fetch the secret (ADC credentials are used here)
        response = client.access_secret_version(request={"name": secret_path})
        
        try:
            logger.info(f"Attempting UTF-8 decode")
            json_string = response.payload.data.decode("UTF-8")
            secrets = json.loads(json_string)
        except Exception as e:
            logger.warning(f"UTF-8 decode failed, trying ASCII")
            try:
                logger.info(f"Attempting ASCII decode")
                json_string = response.payload.data.decode("ascii")
                secrets = json_string
            except Exception as e:
                logger.error(f"All decoding attempts failed")
                raise Exception(f"Error fetching secret: {e}")

        # Cache the successfully retrieved secret
        _cache_secret(secret_id, secrets)
        
        logger.success(f"Successfully fetched and decoded secret")
        return secrets
        
    except Exception as e:
        logger.error(f"Unexpected error while fetching secret")
        raise