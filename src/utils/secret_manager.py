from google.cloud import secretmanager
import json
import time
from src.utils.exception import handle_exception
from src.utils.logger import logger

@handle_exception
def get_secret(secret_id):

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
        json_string = response.payload.data.decode("UTF-8")
        secrets = json.loads(json_string)
    except Exception as e:
        try:
            logger.info(f"Secret must have been encoded in ascii, decoding...")
            json_string = response.payload.data.decode("ascii")
            secrets = json_string
        except Exception as e:
            raise Exception(f"Error fetching secret: {e}")

    # Access your secrets
    logger.success(f"Secret fetched.")
    time.sleep(1)
    return secrets