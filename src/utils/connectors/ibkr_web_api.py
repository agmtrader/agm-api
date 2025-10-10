import jwt
import time
import requests
import json
from src.utils.managers.secret_manager import get_secret
from src.utils.exception import handle_exception, ServiceError
from src.utils.logger import logger
from datetime import datetime

logger.announcement('Initializing Interactive Brokers Web API Service', type='info')
logger.announcement('Initialized Interactive Brokers Web API Service', type='success')

class IBKRWebAPI:

    def __init__(self):
        self.BASE_URL = "https://api.ibkr.com"
        self.CLIENT_ID = "AGMTechnology-FD2"
        self.KEY_ID = "prodfd"
        self.CLIENT_PRIVATE_KEY = get_secret("IBKR_ACCOUNT_MANAGEMENT_PRIVATE_KEY")

        self.sso_token = None

        # Initialize token cache
        self._token = None
        self._token_expiry = 0
        self.TOKEN_REFRESH_BUFFER = 300

    def _apply_credentials(self, master_account: str):
        """
        Temporarily switch credentials according to the requested master account type.
        Args:
            master_account (str): 'ad' for Advisor, 'br' for Fully-Disclosed Broker.
        Returns:
            tuple: (original_client_id, original_key_id, original_private_key)
        """
        original = (self.CLIENT_ID, self.KEY_ID, self.CLIENT_PRIVATE_KEY)
        self.CLIENT_PRIVATE_KEY = get_secret("IBKR_ACCOUNT_MANAGEMENT_PRIVATE_KEY")
        if not master_account:
            raise Exception("Master account is required")
        if (master_account).lower() == 'ad':
            self.CLIENT_ID = "AGMTechnology-FA2"
            self.KEY_ID = "prodfa"
        elif (master_account).lower() == 'br':
            self.CLIENT_ID = "AGMTechnology-FD2"
            self.KEY_ID = "prodfd"
        else:
            raise Exception(f"Invalid master_account: {master_account}")
        self._token = None
        self._token_expiry = 0
        return original

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

    # Account Management API
    @handle_exception
    def list_accounts(self, master_account: str = None):
        try:
            original_creds = self._apply_credentials(master_account)
            logger.info("Getting accounts")
            logger.info(f"Base URL: {self.BASE_URL}")
            url = f"{self.BASE_URL}/gw/api/v1/accounts/status?startDate=2022-03-01&endDate=2025-06-19"
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
        finally:
            self.CLIENT_ID, self.KEY_ID, self.CLIENT_PRIVATE_KEY = original_creds
    
    @handle_exception
    def get_account_details(self, account_id, master_account: str = None):
        try:
            original_creds = self._apply_credentials(master_account)
            logger.info(f"Getting account details for {account_id}")
            url = f"{self.BASE_URL}/gw/api/v1/accounts/{account_id}/details"
            token = self.get_bearer_token()
            if not token:
                raise Exception("No token found")
            
            headers = {
                "Authorization": f"Bearer {token}"
            }

            response = requests.get(url, headers=headers)
            if response.status_code != 200:
                raise Exception(f"Error {response.status_code}: {response.text}")
            
            logger.success(f"Account details fetched successfully")
            return response.json()
        finally:
            self.CLIENT_ID, self.KEY_ID, self.CLIENT_PRIVATE_KEY = original_creds

    @handle_exception
    def get_registration_tasks(self, account_id, master_account: str = None):
        try:
            original_creds = self._apply_credentials(master_account)
            logger.info(f"Getting registration tasks for account {account_id}")
            url = f"{self.BASE_URL}/gw/api/v1/accounts/{account_id}/tasks?type=registration"
            token = self.get_bearer_token()
            if not token:
                raise Exception("No token found")
            headers = {
                "Authorization": f"Bearer {token}"
            }
            response = requests.get(url, headers=headers)
            if response.status_code != 200:
                raise Exception(f"Error {response.status_code}: {response.text}")
            logger.success(f"Registration tasks fetched successfully")
            return response.json()
        finally:
            self.CLIENT_ID, self.KEY_ID, self.CLIENT_PRIVATE_KEY = original_creds

    @handle_exception
    def get_pending_tasks(self, account_id, master_account: str = None):
        try:
            original_creds = self._apply_credentials(master_account)
            logger.info(f"Getting pending tasks for account {account_id}")
            url = f"{self.BASE_URL}/gw/api/v1/accounts/{account_id}/tasks?type=pending"
            token = self.get_bearer_token()
            if not token:
                raise Exception("No token found")

            headers = {
                "Authorization": f"Bearer {token}"
            }
            response = requests.get(url, headers=headers)
            if response.status_code != 200:
                raise Exception(f"Error {response.status_code}: {response.text}")
            logger.success(f"Pending tasks fetched successfully")
            return response.json()
        finally:
            self.CLIENT_ID, self.KEY_ID, self.CLIENT_PRIVATE_KEY = original_creds

    @handle_exception
    def submit_account_management_requests(self, account_management_requests, master_account: str = None):
        try:
            original_creds = self._apply_credentials(master_account)
            logger.info(f"Updating account.")
            url = f"{self.BASE_URL}/gw/api/v1/accounts"
            token = self.get_bearer_token()
            if not token:
                raise Exception("No token found")

            account_management_requests = self.sign_request(account_management_requests)
            
            headers = {
                "Authorization": f"Bearer {token}",
                "Content-Type": "application/jwt"
            }
            response = requests.patch(url, headers=headers, data=account_management_requests)
            if response.status_code != 200:
                raise Exception(f"Error {response.status_code}: {response.text}")
            logger.success(f"Account updated successfully")
            return response.json()
        finally:
            self.CLIENT_ID, self.KEY_ID, self.CLIENT_PRIVATE_KEY = original_creds

    @handle_exception
    def apply_fee_template(self, account_id: str, template_name: str, master_account: str = None):
        """Apply a fee template to the specified account.

        Args:
            account_id (str): The IBKR account ID.
            template_name (str): Name of the fee template to apply.

        Returns:
            dict: API response after applying fee template.
        """
        try:
            original_creds = self._apply_credentials(master_account)
            logger.info(f"Applying fee template {template_name} to account {account_id}")

            url = f"{self.BASE_URL}/gw/api/v1/accounts"

            body = {
                "accountManagementRequests": {
                    "applyFeeTemplate": {
                        "accountId": account_id,
                        "templateName": template_name
                    }
                }
            }

            token = self.get_bearer_token()
            if not token:
                raise Exception("No token found")

            signed_jwt = self.sign_request(body)

            headers = {
                "Authorization": f"Bearer {token}",
                "Content-Type": "application/jwt"
            }

            response = requests.patch(url, headers=headers, data=signed_jwt)
            if response.status_code != 200:
                logger.error(f"Error {response.status_code}: {response.text}")
                raise Exception(f"Error {response.status_code}: {response.text}")

            logger.success("Fee template applied successfully")
            return response.json()
        finally:
            self.CLIENT_ID, self.KEY_ID, self.CLIENT_PRIVATE_KEY = original_creds

    @handle_exception
    def update_account_alias(self, account_id: str, new_alias: str, master_account: str = None):
        """Update the alias for a given account.

        Args:
            account_id (str): The account number whose alias will be updated.
            new_alias (str): Desired alias string.

        Returns:
            dict: API response after updating the alias.
        """
        try:
            original_creds = self._apply_credentials(master_account)
            logger.info(f"Updating alias for account {account_id} to {new_alias}")

            url = f"{self.BASE_URL}/gw/api/v1/accounts"

            body = {
                "accountManagementRequests": {
                    "updateAccountAlias": {
                        "referenceAccountId": account_id,
                        "accountAlias": new_alias
                    }
                }
            }

            token = self.get_bearer_token()
            if not token:
                raise Exception("No token found")

            signed_jwt = self.sign_request(body)

            headers = {
                "Authorization": f"Bearer {token}",
                "Content-Type": "application/jwt"
            }

            response = requests.patch(url, headers=headers, data=signed_jwt)
            if response.status_code != 200:
                logger.error(f"Error {response.status_code}: {response.text}")
                raise Exception(f"Error {response.status_code}: {response.text}")

            logger.success("Account alias updated successfully")
            return response.json()
        finally:
            self.CLIENT_ID, self.KEY_ID, self.CLIENT_PRIVATE_KEY = original_creds

    @handle_exception
    def process_documents(self, documents: list = None, master_account: str = None) -> dict:
        """Auto-sign and upload IBKR forms given their form numbers.

        The caller should supply a plain list/array of form numbers, e.g. `["2001", "8001"]`.
        This method will:
            1. Fetch each form via ``get_forms``.
            2. Build the required *documents* payload (metadata + PDF bytes).
            3. Sign the resulting *DocumentSubmissionRequest* as a JWT and POST it to
               ``/gw/api/v1/accounts/documents``.
        """
        try:
            original_creds = self._apply_credentials(master_account)
            if documents is None or not isinstance(documents, list):
                raise Exception("process_documents expects a list of form numbers (strings or ints).")

            form_numbers = [str(f) for f in documents]

            logger.info(f"Building DocumentSubmissionRequest for forms: {form_numbers}")

            timestamp = int(datetime.utcnow().strftime("%Y%m%d%H%M%S"))

            built_documents = []
            for form_no in form_numbers:
                try:
                    form_result = self.get_forms(forms=[form_no])
                    form_details = form_result.get("formDetails", [])
                    if not form_details:
                        logger.warning(f"No formDetails returned for form {form_no}")
                        continue

                    form = form_details[0]
                    file_data = form_result.get("fileData", {})

                    built_documents.append({
                        "signedBy": ["Account Holder"],
                        "attachedFile": {
                            "fileName": form.get("fileName"),
                            "fileLength": form.get("fileLength"),
                            "sha1Checksum": form.get("sha1Checksum"),
                        },
                        "formNumber": int(form.get("formNumber")),
                        "validAddress": False,
                        "execLoginTimestamp": timestamp,
                        "execTimestamp": timestamp,
                        "payload": {
                            "mimeType": "application/pdf",
                            "data": file_data.get("data"),
                        },
                    })
                except Exception as e:
                    logger.error(f"Failed to build document for form {form_no}: {e}")

            if not built_documents:
                raise Exception("Failed to build any document payloads – all form fetches failed.")

            submission_request = {
                "processDocuments": {
                    "documents": built_documents,
                    "inputLanguage": "en",
                    "translation": False,
                }
            }

            logger.info("Uploading documents via /accounts/documents endpoint")
            url = f"{self.BASE_URL}/gw/api/v1/accounts/documents"

            token = self.get_bearer_token()
            if not token:
                raise Exception("No token found")

            signed_jwt = self.sign_request(submission_request)

            headers = {
                "Authorization": f"Bearer {token}",
                "Content-Type": "application/jwt"
            }

            response = requests.post(url, headers=headers, data=signed_jwt)
            logger.info(f"Response: {response.text}")
            
            if response.status_code != 200:
                raise Exception(f"Error {response.status_code}: {response.text}")

            logger.success("Documents uploaded successfully")
            return response.json()
        finally:
            self.CLIENT_ID, self.KEY_ID, self.CLIENT_PRIVATE_KEY = original_creds

    @handle_exception
    def send_to_ibkr(self, application, master_account: str = None):
        try:
            original_creds = self._apply_credentials(master_account)
            logger.info(f"Sending application to Interactive Brokers using master account type: {master_account}")
            url = f"{self.BASE_URL}/gw/api/v1/accounts"

            # Get OAuth2 token (refresh if needed) with the overridden credentials
            token = self.get_bearer_token()
            if not token:
                raise Exception("No token found")

            # Sign the application payload with the overridden credentials
            signed_jwt = self.sign_request(application)

            headers = {
                "Authorization": f"Bearer {token}",
                "Content-Type": "application/jwt"
            }

            response = requests.post(url, headers=headers, data=signed_jwt)
            data = response.json()
            if response.status_code != 200:
                # Extract meaningful error message if available
                error_message = None
                if isinstance(data, dict):
                    # IBKR API typically returns { "detail": "..." } on error
                    error_message = data.get("detail") or data.get("message")

                if not error_message:
                    # Fallback to raw response text
                    error_message = response.text
                
                logger.error(f"Error 505: {error_message}")
                raise ServiceError(error_message[0:50] + '...', status_code=505)

            logger.success("Application sent to Interactive Brokers successfully")
            return data
        finally:
            # Restore original credentials to avoid side-effects
            self.CLIENT_ID, self.KEY_ID, self.CLIENT_PRIVATE_KEY = original_creds

    @handle_exception
    def get_forms(self, forms: list = None, master_account: str = None):
        try:
            original_creds = self._apply_credentials(master_account)
            logger.info("Getting forms")
            url = f"{self.BASE_URL}/gw/api/v1/forms?fromDate=2016-10-20&toDate=&getDocs=T"
            if forms:
                forms = ",".join(str(f) for f in forms)
                url += f"&formNo={forms}"
            
            token = self.get_bearer_token()
            if not token:
                raise Exception("No token found")
            headers = {
                "Authorization": f"Bearer {token}"
            }
            import base64
            import io
            import zipfile

            response = requests.get(url, headers=headers)
            if response.status_code != 200:
                raise Exception(f"Error {response.status_code}: {response.text}")
            logger.success(f"Form fetched successfully")
            result = response.json()

            # Filter formDetails to only English forms
            form_details = result.get('formDetails')
            if form_details and isinstance(form_details, list):
                en_forms = [f for f in form_details if f.get('language') == 'en']
                result['formDetails'] = en_forms
            else:
                en_forms = []

            file_data = result.get('fileData')
            if file_data and 'data' in file_data:
                file_name = file_data.get('name')
                data_b64 = file_data['data']
                if file_name and file_name.endswith('.zip') and en_forms:
                    # Extract the PDF matching the English form fileName
                    pdf_file_name = en_forms[0].get('fileName')
                    zip_bytes = base64.b64decode(data_b64)
                    with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zf:
                        if pdf_file_name and pdf_file_name in zf.namelist():
                            with zf.open(pdf_file_name) as pdf_file:
                                pdf_bytes = pdf_file.read()
                                pdf_b64 = base64.b64encode(pdf_bytes).decode('utf-8')
                                file_data['data'] = pdf_b64
                                file_data['name'] = pdf_file_name
                        else:
                            logger.error(f'English PDF file {pdf_file_name} not found in ZIP archive')
                            # fallback: first PDF in zip
                            for name in zf.namelist():
                                if name.lower().endswith('.pdf'):
                                    with zf.open(name) as pdf_file:
                                        pdf_bytes = pdf_file.read()
                                        pdf_b64 = base64.b64encode(pdf_bytes).decode('utf-8')
                                        file_data['data'] = pdf_b64
                                        file_data['name'] = name
                                        break
                elif file_name and file_name.endswith('.pdf') and en_forms:
                    # Only keep fileData if it matches an English form
                    if file_name != en_forms[0].get('fileName'):
                        logger.info(f'PDF file {file_name} does not match English form {en_forms[0].get("fileName")}')
                        file_data['data'] = ''
            return result
        finally:
            self.CLIENT_ID, self.KEY_ID, self.CLIENT_PRIVATE_KEY = original_creds

    @handle_exception
    def get_security_questions(self):
        """Retrieve list of security questions from IBKR."""
        try:
            original_creds = self._apply_credentials('br')
            logger.info("Fetching security questions")

            url = f"{self.BASE_URL}/gw/api/v1/enumerations/security-questions"

            token = self.get_bearer_token()
            if not token:
                raise Exception("No token found")

            headers = {"Authorization": f"Bearer {token}"}

            response = requests.get(url, headers=headers)
            if response.status_code != 200:
                logger.error(f"Error {response.status_code}: {response.text}")
                raise Exception(f"Error {response.status_code}: {response.text}")

            logger.success("Security questions fetched successfully")
            return response.json()
        finally:
            self.CLIENT_ID, self.KEY_ID, self.CLIENT_PRIVATE_KEY = original_creds

    @handle_exception
    def update_account_email(self, reference_user_name: str, new_email: str, access: bool = True, master_account: str = None):
        """Update the email associated with a given IBKR user name.

        Args:
            reference_user_name (str): The IBKR username whose email will be updated.
            new_email (str): The new email address.
            access (bool, optional): Email access flag required by IBKR. Defaults to True.
            master_account (str, optional): Master account type ('ad' or 'br'). Defaults to None.

        Returns:
            dict: API response after updating the email.
        """
        try:
            original_creds = self._apply_credentials(master_account)
            logger.info(f"Updating email for user {reference_user_name} to {new_email}")

            url = f"{self.BASE_URL}/gw/api/v1/accounts"

            body = {
                "accountManagementRequests": {
                    "updateCredentials": [
                        {
                            "referenceUserName": reference_user_name,
                            "updateEmail": {
                                "email": new_email,
                                "token": "12345",                
                                "access": True
                            }
                        }
                    ]
                }
            }

            token = self.get_bearer_token()
            if not token:
                raise Exception("No token found")

            signed_jwt = self.sign_request(body)

            headers = {
                "Authorization": f"Bearer {token}",
                "Content-Type": "application/jwt"
            }

            response = requests.patch(url, headers=headers, data=signed_jwt)
            if response.status_code != 200:
                logger.error(f"Error {response.status_code}: {response.text}")
                raise Exception(f"Error {response.status_code}: {response.text}")

            logger.success("Account email updated successfully")
            return response.json()
        finally:
            self.CLIENT_ID, self.KEY_ID, self.CLIENT_PRIVATE_KEY = original_creds

    @handle_exception
    def add_trading_permissions(self, reference_account_id: str, trading_permissions: list, documents: list = None, master_account: str = None) -> dict:
        """Add trading permissions to the given account.

        Args:
            reference_account_id (str): IBKR account id that will receive the permissions.
            trading_permissions (list): List of trading permission dictionaries as required by IBKR.
            documents (list | None): Optional list of DocumentSubmission items (already built). Defaults to empty list.
            master_account (str | None): Credential set to use (``ad`` or ``br``). Defaults to ``None`` to use current creds.

        Returns:
            dict: API response from IBKR.
        """
        try:
            original_creds = self._apply_credentials(master_account)
            logger.info(f"Adding trading permissions for account {reference_account_id}")

            url = f"{self.BASE_URL}/gw/api/v1/accounts"

            body = {
                "accountManagementRequests": {
                    "addTradingPermissions": [
                        {
                            "tradingPermissions": trading_permissions,
                            "documentSubmission": {
                                "documents": documents or [],
                                "referenceAccountId": reference_account_id,
                                "inputLanguage": "en",
                                "translation": False,
                            },
                            "referenceAccountId": reference_account_id,
                        }
                    ]
                }
            }

            token = self.get_bearer_token()
            if not token:
                raise Exception("No token found")

            signed_jwt = self.sign_request(body)

            headers = {
                "Authorization": f"Bearer {token}",
                "Content-Type": "application/jwt",
            }

            response = requests.patch(url, headers=headers, data=signed_jwt)
            if response.status_code != 200:
                logger.error(f"Error {response.status_code}: {response.text}")
                raise Exception(f"Error {response.status_code}: {response.text}")

            logger.success("Trading permissions added successfully")
            return response.json()
        finally:
            self.CLIENT_ID, self.KEY_ID, self.CLIENT_PRIVATE_KEY = original_creds

    @handle_exception
    def get_exchange_bundles(self, master_account: str = None):
        """Retrieve enumeration list for exchange bundles from IBKR."""
        try:
            original_creds = self._apply_credentials(master_account)
            logger.info("Fetching exchange bundles enumerations")

            url = f"{self.BASE_URL}/gw/api/v1/enumerations/exchange-bundles"

            token = self.get_bearer_token()
            if not token:
                raise Exception("No token found")

            headers = {"Authorization": f"Bearer {token}"}

            response = requests.get(url, headers=headers)
            if response.status_code != 200:
                logger.error(f"Error {response.status_code}: {response.text}")
                raise Exception(f"Error {response.status_code}: {response.text}")

            logger.success("Exchange bundles fetched successfully")
            return response.json()
        finally:
            self.CLIENT_ID, self.KEY_ID, self.CLIENT_PRIVATE_KEY = original_creds

    # Trading API
    def create_sso_session(self, credential: str, ip: str) -> str:
        """
        Create an SSO browser session for IBKR Client Portal.
        Args:
            credential (str): IBKR username associated with the user.
            ip (str): The user's actual IP address (REMOTE_ADDR).
        Returns:
            str: The SSO URL to be opened in the browser.
        """
        try:
            original_creds = self._apply_credentials('br')
            logger.info(f"Creating SSO browser session for credential: {credential}, ip: {ip}")
            url = f"{self.BASE_URL}/gw/api/v1/sso-sessions"
            token = self.get_bearer_token()
            if not token:
                logger.error("No token found for SSO session creation")
                return None
            headers = {
                "Authorization": f"Bearer {token}",
                "Content-Type": "application/jwt"
            }
            payload = {
                "credential": credential,
                "ip": ip
            }

            signed_jwt = self.sign_request(payload)
            response = requests.post(url, data=signed_jwt, headers=headers)
            if response.status_code != 200:
                logger.error(f"Error {response.status_code}: {response.text}")
                raise Exception(f"Error {response.status_code}: {response.text}")
            data = response.json()
            if 'access_token' in data:
                self.sso_token = data['access_token']
            else:
                raise Exception(f"No access token found in response: {data}")
            return data
        finally:
            self.CLIENT_ID, self.KEY_ID, self.CLIENT_PRIVATE_KEY = original_creds

    def initialize_brokerage_session(self) -> str:
        """
        Initialize a brokerage session for IBKR Client Portal.
        Args:
            credential (str): IBKR username associated with the user.
            ip (str): The user's actual IP address (REMOTE_ADDR).
        Returns:
            str: The brokerage session URL to be opened in the browser.
        """
        original_creds = self._apply_credentials('br')
        logger.info(f"Initializing Brokerage session")
        url = f"{self.BASE_URL}/v1/api/iserver/auth/ssodh/init"
        #token = self.get_bearer_token()
        if not self.sso_token:
            logger.error("No SSO token found for Brokerage session initialization")
            raise Exception("No SSO token found for Brokerage session initialization")
            
        headers = {
            "Authorization": f"Bearer {self.sso_token}",
            "Content-Type": "application/json"
        }

        payload = {
            "publish": True,
            "compete": True
        }

        #signed_jwt = self.sign_request(payload)
        try:
            response = requests.post(url, data=json.dumps(payload), headers=headers)
            if response.status_code != 200:
                logger.error(f"Error {response.status_code}: {response.text}")
                raise Exception(f"Error {response.status_code}: {response.text}")
            data = response.json()
            return data
        finally:
            self.CLIENT_ID, self.KEY_ID, self.CLIENT_PRIVATE_KEY = original_creds

    def logout_of_brokerage_session(self):
        """
        Logout of the brokerage session.
        """
        try:
            original_creds = self._apply_credentials('br')
            url = f"{self.BASE_URL}/v1/api/logout"
            if not self.sso_token:
                logger.error("No SSO token found for brokerage session logout")
                raise Exception("No SSO token found for brokerage session logout")
            headers = {
                "Authorization": f"Bearer {self.sso_token}",
                "Content-Type": "application/json"
            }
            response = requests.post(url, headers=headers)
            if response.status_code != 200:
                logger.error(f"Error {response.status_code}: {response.text}")
                raise Exception(f"Error {response.status_code}: {response.text}")
            logger.success("Logged out of brokerage session successfully")
            return response.json()
        finally:
            self.CLIENT_ID, self.KEY_ID, self.CLIENT_PRIVATE_KEY = original_creds

    def get_brokerage_accounts(self):
        """
        Get the brokerage accounts.
        """
        try:
            original_creds = self._apply_credentials('br')
            url = f"{self.BASE_URL}/v1/api/iserver/accounts"
            if not self.sso_token:
                raise Exception("No token found")
            headers = {
                "Authorization": f"Bearer {self.sso_token}",
                "Content-Type": "application/json"
            }
            response = requests.get(url, headers=headers)
            if response.status_code != 200:
                raise Exception(f"Error {response.status_code}: {response.text}")
            return response.json()
        finally:
            self.CLIENT_ID, self.KEY_ID, self.CLIENT_PRIVATE_KEY = original_creds   