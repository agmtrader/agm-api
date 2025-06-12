import jwt
import time
import requests
from src.utils.managers.secret_manager import get_secret
from src.utils.exception import handle_exception
from src.utils.logger import logger

logger.announcement('Initializing Interactive Brokers Trading API Service', type='info')
logger.announcement('Initialized Interactive Brokers Trading API Service', type='success')

class Trading:

    def __init__(self):
        self.BASE_URL = "https://qa.interactivebrokers.com"
        self.CLIENT_ID = "AGMTechnology-FD-QA"
        self.KEY_ID = "main"
        self.CLIENT_PRIVATE_KEY = get_secret("IBKR_ACCOUNT_MANAGEMENT_PRIVATE_KEY")

        # Initialize token cache
        self._token = None
        self._token_expiry = 0
        self.TOKEN_REFRESH_BUFFER = 300
    
    @handle_exception
    def get_accounts(self):
        logger.info(f"Getting accounts")
        logger.info(f"Base URL: {self.BASE_URL}")
        url = f"{self.BASE_URL}/portfolio/accounts"
        
        # Get OAuth2 token for authentication
        token = self.get_bearer_token()
        if not token:
            raise Exception("No token found")
        
        headers = {
            "Authorization": f"Bearer {token}"
        }
        
        response = requests.get(url, headers=headers)
        if response.status_code != 200:
            raise Exception(f"Error {response.status_code}: {response.text}")
        
        logger.success(f"Accounts fetched successfully")
        return response.json()

    def get_bearer_token(self):
        current_time = int(time.time())
        
        # Check if we have a valid cached token
        if self._token and current_time < (self._token_expiry - self.TOKEN_REFRESH_BUFFER):
            logger.info("Using cached token")
            return self._token

        logger.info("Fetching new bearer token")
        url = f"{self.BASE_URL}/oauth2/api/v1/token"
        client_assertion = self.generate_client_assertion()

        # Prepare the request payload according to the API documentation
        payload = {
            "grant_type": "client_credentials",
            "client_assertion_type": "urn:ietf:params:oauth:client-assertion-type:jwt-bearer",
            "client_assertion": client_assertion,
            "scope": "accounts.read accounts.write bank-instructions.read bank-instructions.write clients.read clients.write echo.read echo.write fee-templates.read fee-templates.write instructions.read instructions.write statements.read transfers.read transfers.write sso-sessions.write sso-browser-sessions.write"
        }

        headers = {
            "Content-Type": "application/x-www-form-urlencoded"
        }

        # Send the request
        response = requests.post(url, data=payload, headers=headers)
        if response.status_code == 200:
            response_data = response.json()
            self._token = response_data["access_token"]
            # Calculate absolute expiry time
            self._token_expiry = current_time + response_data["expires_in"]
            logger.success(f"New token fetched, expires in {response_data['expires_in']} seconds")
            return self._token
        else:
            logger.error(f"Error {response.status_code}: {response.text}")
            return None
        
    def generate_client_assertion(self):
        # Define the JWT claims matching the Postman example
        current_time = int(time.time())
        payload = {
            "iss": self.CLIENT_ID,
            "sub": self.CLIENT_ID,
            "aud": f"{self.BASE_URL}/oauth2/api/v1/token",
            "exp": current_time + 20,  # 20 seconds from now, as in Postman
            "iat": current_time - 10   # 10 seconds ago, as in Postman
        }

        # Define the JWT headers
        headers = {
            "typ": "JWT",
            "alg": "RS256",
            "kid": self.KEY_ID
        }

        # Generate the client assertion
        client_assertion = jwt.encode(
            payload,
            self.CLIENT_PRIVATE_KEY,
            algorithm="RS256",
            headers=headers
        )

        return client_assertion

    def sign_request(self, request_body):
        """
        Sign the request body as a JWT for endpoints requiring a signed JWT.
        The JWT payload is the request body. The JWT headers use RS256 and kid.
        """
        headers = {
            "typ": "JWT",
            "alg": "RS256",
            "kid": self.KEY_ID  # From environment or config
        }

        # Create the payload by starting with a copy of the request_body 
        # and adding/overwriting JWT claims, as done in the Postman script.
        payload_claims = request_body.copy()

        current_time = int(time.time())
        
        payload_claims["iss"] = self.CLIENT_ID  # From environment or config
        payload_claims["exp"] = current_time + 1000  # Expiry in 1000 seconds (as per Postman)
        payload_claims["iat"] = current_time         # Issued at current time

        # The pyjwt library's encode function takes a dictionary as payload.
        # It handles JSON serialization and base64url encoding internally.
        signed_jwt = jwt.encode(
            payload_claims,
            self.CLIENT_PRIVATE_KEY, # Loaded PEM private key
            algorithm="RS256",
            headers=headers
        )
        
        return signed_jwt