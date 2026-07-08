import json
import time

import requests

from src.lib.ibkr_trading_api import MarketDataField
from src.utils.connectors.ibkr_web_api import IBKRWebAPI, retry_on_connection_error
from src.utils.exception import handle_exception
from src.utils.logger import logger


class IBKRTradingAPI(IBKRWebAPI):
    def __init__(self):
        super().__init__()
        self.sso_token = None

    def _require_sso_headers(self, content_type: str | None = "application/json") -> dict:
        if not self.sso_token:
            raise Exception("No SSO token found")
        headers = {"Authorization": f"Bearer {self.sso_token}"}
        if content_type:
            headers["Content-Type"] = content_type
        return headers

    @handle_exception
    def create_sso_session(self, credential: str, ip: str) -> dict:
        try:
            original_creds = self._apply_credentials("I6413690")
            logger.info(f"Creating SSO browser session for credential: {credential}, ip: {ip}")
            url = f"{self.BASE_URL}/gw/api/v1/sso-sessions"
            token = self.get_bearer_token()
            if not token:
                logger.error("No token found for SSO session creation")
                return None

            headers = {
                "Authorization": f"Bearer {token}",
                "Content-Type": "application/jwt",
            }
            payload = {
                "credential": credential,
                "ip": ip,
            }

            signed_jwt = self.sign_request(payload)
            response = requests.post(url, data=signed_jwt, headers=headers)
            if response.status_code != 200:
                logger.error(f"Error {response.status_code}: {response.text}")
                raise Exception(f"Error {response.status_code}: {response.text}")
            data = response.json()
            if "access_token" not in data:
                raise Exception(f"No access token found in response: {data}")
            self.sso_token = data["access_token"]
            return data
        finally:
            self.CLIENT_ID, self.KEY_ID, self.CLIENT_PRIVATE_KEY = original_creds

    @handle_exception
    def initialize_brokerage_session(self) -> dict:
        try:
            original_creds = self._apply_credentials("I6413690")
            logger.info("Initializing Brokerage session")
            url = f"{self.BASE_URL}/v1/api/iserver/auth/ssodh/init"
            headers = self._require_sso_headers()
            payload = {
                "publish": True,
                "compete": True,
            }
            response = requests.post(url, data=json.dumps(payload), headers=headers)
            if response.status_code != 200:
                logger.error(f"Error {response.status_code}: {response.text}")
                raise Exception(f"Error {response.status_code}: {response.text}")
            return response.json()
        finally:
            self.CLIENT_ID, self.KEY_ID, self.CLIENT_PRIVATE_KEY = original_creds

    @handle_exception
    def logout_of_brokerage_session(self) -> dict:
        try:
            original_creds = self._apply_credentials("I6413690")
            url = f"{self.BASE_URL}/v1/api/logout"
            response = requests.post(url, headers=self._require_sso_headers())
            if response.status_code != 200:
                logger.error(f"Error {response.status_code}: {response.text}")
                raise Exception(f"Error {response.status_code}: {response.text}")
            logger.success("Logged out of brokerage session successfully")
            return response.json()
        finally:
            self.CLIENT_ID, self.KEY_ID, self.CLIENT_PRIVATE_KEY = original_creds

    @handle_exception
    def get_brokerage_accounts(self) -> dict:
        try:
            original_creds = self._apply_credentials("I6413690")
            url = f"{self.BASE_URL}/v1/api/iserver/accounts"
            headers = self._require_sso_headers()
            payload = None

            for attempt in range(1, 4):
                response = requests.get(url, headers=headers)
                if response.status_code != 200:
                    raise Exception(f"Error {response.status_code}: {response.text}")

                payload = response.json()
                has_accounts_shape = (
                    isinstance(payload, dict)
                    and isinstance(payload.get("accounts"), list)
                    and payload.get("selectedAccount")
                    and isinstance(payload.get("aliases"), dict)
                )
                if has_accounts_shape:
                    break

                if attempt < 3:
                    time.sleep(0.5)

            if not isinstance(payload, dict):
                raise Exception(f"Invalid accounts payload type: {type(payload).__name__}")

            if not isinstance(payload.get("accounts"), list):
                payload["accounts"] = []
            if not payload.get("selectedAccount") and payload["accounts"]:
                payload["selectedAccount"] = payload["accounts"][0]
            if not isinstance(payload.get("aliases"), dict):
                payload["aliases"] = {}
            for account_id in payload["accounts"]:
                payload["aliases"].setdefault(account_id, account_id)

            if not payload["accounts"] or not payload.get("selectedAccount"):
                raise Exception(f"Incomplete accounts payload after retry: {payload}")

            return payload
        finally:
            self.CLIENT_ID, self.KEY_ID, self.CLIENT_PRIVATE_KEY = original_creds

    def _prime_iserver_session(self, reason: str):
        return self.get_brokerage_accounts()

    def _request_iserver_json(self, method: str, url: str, headers: dict, reason: str, **kwargs):
        self._prime_iserver_session(reason)
        response = requests.request(method, url, headers=headers, **kwargs)

        should_retry = (
            response.status_code in (410, 500)
            and (
                response.status_code == 410
                or "Please query /accounts first" in response.text
            )
        )
        if should_retry:
            self._prime_iserver_session(f"{reason} retry")
            response = requests.request(method, url, headers=headers, **kwargs)

        if response.status_code != 200:
            raise Exception(f"Error {response.status_code}: {response.text}")
        return response.json()

    @handle_exception
    def get_portfolio_analyst_performance(self, acct_ids: list = None, freq: str = None) -> dict:
        try:
            original_creds = self._apply_credentials("I6413690")
            url = f"{self.BASE_URL}/v1/api/pa/performance"
            payload = {
                "acctIds": acct_ids or [],
                "freq": freq,
            }
            response = requests.post(url, headers=self._require_sso_headers(), data=json.dumps(payload))
            if response.status_code != 200:
                raise Exception(f"Error {response.status_code}: {response.text}")
            return response.json()
        finally:
            self.CLIENT_ID, self.KEY_ID, self.CLIENT_PRIVATE_KEY = original_creds

    @handle_exception
    def get_all_watchlists(self) -> dict:
        try:
            original_creds = self._apply_credentials("I6413690")
            url = f"{self.BASE_URL}/v1/api/iserver/watchlists"
            headers = self._require_sso_headers()
            payload = self._request_iserver_json("GET", url, headers, "get_all_watchlists")
            logger.success("All watchlists fetched successfully")
            return payload
        finally:
            self.CLIENT_ID, self.KEY_ID, self.CLIENT_PRIVATE_KEY = original_creds

    @handle_exception
    def get_watchlist_information(self, watchlist_id: str) -> dict:
        try:
            original_creds = self._apply_credentials("I6413690")
            url = f"{self.BASE_URL}/v1/api/iserver/watchlist?id={watchlist_id}"
            headers = self._require_sso_headers()
            payload = self._request_iserver_json(
                "GET",
                url,
                headers,
                f"get_watchlist_information id={watchlist_id}",
            )
            logger.success("Watchlist information fetched successfully")
            return payload
        finally:
            self.CLIENT_ID, self.KEY_ID, self.CLIENT_PRIVATE_KEY = original_creds

    @handle_exception
    def get_market_data_snapshot(self, conids: str):
        try:
            original_creds = self._apply_credentials("I6413690")
            url = f"{self.BASE_URL}/v1/api/iserver/marketdata/snapshot?conids={conids}"

            desired_fields = [
                MarketDataField.SYMBOL,
                MarketDataField.COMPANY_NAME,
                MarketDataField.CONID_EXCHANGE,
                MarketDataField.SECTYPE,
                MarketDataField.TEXT,
                MarketDataField.CONTRACT_DESCRIPTION_1,
                MarketDataField.CONTRACT_DESCRIPTION_2,
                MarketDataField.BID_PRICE,
                MarketDataField.BID_SIZE,
                MarketDataField.ASK_PRICE,
                MarketDataField.ASK_SIZE,
                MarketDataField.LAST_PRICE,
                MarketDataField.CHANGE,
                MarketDataField.CHANGE_PERCENT,
                MarketDataField.BID_YIELD,
                MarketDataField.ASK_YIELD,
                MarketDataField.LAST_YIELD,
                MarketDataField.AVG_PRICE,
                MarketDataField.DAILY_PNL,
                MarketDataField.FORMATTED_POSITION,
                MarketDataField.CATEGORY,
                MarketDataField.INDUSTRY,
                MarketDataField.RATINGS,
                MarketDataField.ISSUE_DATE,
                MarketDataField.REGULAR_EXPIRY,
                MarketDataField.LAST_TRADING_DATE,
                MarketDataField.LISTING_EXCHANGE,
                MarketDataField.BOND_TYPE,
                MarketDataField.BOND_STATE_CODE,
            ]
            fields_str = ",".join(str(field.value) for field in desired_fields)
            headers = self._require_sso_headers()

            response = requests.get(f"{url}&fields={fields_str}", headers=headers)
            if response.status_code != 200:
                logger.error(f"Error {response.status_code}: {response.text}")
                raise Exception(f"Error {response.status_code}: {response.text}")

            logger.info("Waiting for market data snapshot to be ready")
            time.sleep(10)
            response = requests.get(url, headers=headers)
            if response.status_code != 200:
                logger.error(f"Error {response.status_code}: {response.text}")
                raise Exception(f"Error {response.status_code}: {response.text}")

            raw_data = response.json()

            def _translate_fields(item: dict):
                mapped = {}
                for key, value in item.items():
                    if isinstance(key, str) and key.isdigit():
                        try:
                            mapped[MarketDataField(int(key)).name] = value
                        except ValueError:
                            mapped[key] = value
                    else:
                        mapped[key] = value
                return mapped

            if isinstance(raw_data, list):
                mapped_data = [_translate_fields(entry) for entry in raw_data]
            elif isinstance(raw_data, dict):
                mapped_data = _translate_fields(raw_data)
            else:
                mapped_data = raw_data

            logger.success("Market data snapshot fetched successfully")
            return mapped_data
        finally:
            self.CLIENT_ID, self.KEY_ID, self.CLIENT_PRIVATE_KEY = original_creds

    @handle_exception
    def get_market_scanner_params(self) -> dict:
        try:
            original_creds = self._apply_credentials("I6413690")
            url = f"{self.BASE_URL}/v1/api/iserver/scanner/params"
            headers = self._require_sso_headers()
            payload = self._request_iserver_json("GET", url, headers, "get_market_scanner_params")
            logger.success("Market scanner params fetched successfully")
            return payload
        finally:
            self.CLIENT_ID, self.KEY_ID, self.CLIENT_PRIVATE_KEY = original_creds

    @handle_exception
    def run_market_scanner(
        self,
        instrument: str,
        scan_type: str,
        location: str,
        filters: list = None,
    ) -> dict:
        try:
            original_creds = self._apply_credentials("I6413690")
            url = f"{self.BASE_URL}/v1/api/iserver/scanner/run"
            payload = {
                "instrument": instrument,
                "type": scan_type,
                "location": location,
                "filter": filters or [],
            }
            headers = self._require_sso_headers()
            response_payload = self._request_iserver_json(
                "POST",
                url,
                headers,
                f"run_market_scanner instrument={instrument} type={scan_type} location={location}",
                data=json.dumps(payload),
            )
            logger.success("Market scanner run successfully")
            return response_payload
        finally:
            self.CLIENT_ID, self.KEY_ID, self.CLIENT_PRIVATE_KEY = original_creds

    @handle_exception
    def get_historical_market_data(self, conid: str, period: str, bar: str) -> dict:
        try:
            original_creds = self._apply_credentials("I6413690")
            url = f"{self.BASE_URL}/v1/api/iserver/marketdata/history"
            params = {
                "conid": conid,
                "period": period,
                "bar": bar,
                "outsideRth": True,
            }
            logger.info(f"Fetching historical market data for {conid} (period={period}, bar={bar})")
            response = requests.get(url, headers=self._require_sso_headers(None), params=params)
            if response.status_code != 200:
                logger.error(f"Error {response.status_code}: {response.text}")
                raise Exception(f"Error {response.status_code}: {response.text}")
            logger.success("Historical market data fetched successfully")
            return response.json()
        finally:
            self.CLIENT_ID, self.KEY_ID, self.CLIENT_PRIVATE_KEY = original_creds

    @handle_exception
    def get_securities_by_symbol(self, symbol: str, sec_type: str) -> dict:
        try:
            original_creds = self._apply_credentials("I6413690")
            url = f"{self.BASE_URL}/v1/api/iserver/secdef/search"
            payload = {
                "symbol": symbol,
                "secType": sec_type,
            }
            response = requests.post(url, headers=self._require_sso_headers(), data=json.dumps(payload))
            if response.status_code != 200:
                logger.error(f"Error {response.status_code}: {response.text}")
                raise Exception(f"Error {response.status_code}: {response.text}")
            logger.success("Bonds searched successfully")
            return response.json()
        finally:
            self.CLIENT_ID, self.KEY_ID, self.CLIENT_PRIVATE_KEY = original_creds

    @handle_exception
    def get_security_info(self, issuer_id: str, sec_type: str) -> dict:
        try:
            original_creds = self._apply_credentials("I6413690")
            url = f"{self.BASE_URL}/v1/api/iserver/secdef/info?issuerId={issuer_id}&secType={sec_type}"
            response = requests.get(url, headers=self._require_sso_headers())
            if response.status_code != 200:
                logger.error(f"Error {response.status_code}: {response.text}")
                raise Exception(f"Error {response.status_code}: {response.text}")
            logger.success("Security info fetched successfully")
            return response.json()
        finally:
            self.CLIENT_ID, self.KEY_ID, self.CLIENT_PRIVATE_KEY = original_creds

    @handle_exception
    def get_all_conids_from_exchange(self, exchange: str) -> dict:
        try:
            original_creds = self._apply_credentials("I6413690")
            url = f"{self.BASE_URL}/v1/api/trsrv/all-conids?exchange={exchange}"
            response = requests.get(url, headers=self._require_sso_headers(None))
            if response.status_code != 200:
                logger.error(f"Error {response.status_code}: {response.text}")
                raise Exception(f"Error {response.status_code}: {response.text}")
            logger.success("Conids fetched successfully")
            return response.json()
        finally:
            self.CLIENT_ID, self.KEY_ID, self.CLIENT_PRIVATE_KEY = original_creds

    @handle_exception
    def get_contract_info(self, conid: int) -> dict:
        try:
            original_creds = self._apply_credentials("I6413690")
            url = f"{self.BASE_URL}/v1/api/iserver/contract/{conid}/info"
            response = requests.get(url, headers=self._require_sso_headers())
            if response.status_code != 200:
                logger.error(f"Error {response.status_code}: {response.text}")
                raise Exception(f"Error {response.status_code}: {response.text}")
            logger.success(f"Contract info fetched for conid {conid}")
            return response.json()
        finally:
            self.CLIENT_ID, self.KEY_ID, self.CLIENT_PRIVATE_KEY = original_creds

    @handle_exception
    def place_order(self, account_id: str, orders: list) -> dict:
        try:
            original_creds = self._apply_credentials("I6413690")
            url = f"{self.BASE_URL}/v1/api/iserver/account/{account_id}/orders"
            payload = {"orders": orders}
            response = requests.post(url, headers=self._require_sso_headers(), data=json.dumps(payload))
            logger.info(f"Place order response [{response.status_code}]: {response.text}")
            if response.status_code != 200:
                raise Exception(f"Error {response.status_code}: {response.text}")
            return response.json()
        finally:
            self.CLIENT_ID, self.KEY_ID, self.CLIENT_PRIVATE_KEY = original_creds

    @handle_exception
    def reply_to_order(self, reply_id: str, confirmed: bool = True) -> dict:
        try:
            original_creds = self._apply_credentials("I6413690")
            url = f"{self.BASE_URL}/v1/api/iserver/reply/{reply_id}"
            payload = {"confirmed": confirmed}
            response = requests.post(url, headers=self._require_sso_headers(), data=json.dumps(payload))
            logger.info(f"Reply response [{response.status_code}]: {response.text}")
            if response.status_code != 200:
                raise Exception(f"Error {response.status_code}: {response.text}")
            return response.json()
        finally:
            self.CLIENT_ID, self.KEY_ID, self.CLIENT_PRIVATE_KEY = original_creds

    @handle_exception
    def cancel_order(self, account_id: str, order_id: str) -> dict:
        try:
            original_creds = self._apply_credentials("I6413690")
            url = f"{self.BASE_URL}/v1/api/iserver/account/{account_id}/order/{order_id}"
            response = requests.delete(url, headers=self._require_sso_headers())
            logger.info(f"Cancel order response [{response.status_code}]: {response.text}")
            if response.status_code != 200:
                raise Exception(f"Error {response.status_code}: {response.text}")
            logger.success(f"Order {order_id} cancelled successfully")
            return response.json()
        finally:
            self.CLIENT_ID, self.KEY_ID, self.CLIENT_PRIVATE_KEY = original_creds

    @handle_exception
    def get_open_orders(self) -> dict:
        try:
            original_creds = self._apply_credentials("I6413690")
            url = f"{self.BASE_URL}/v1/api/iserver/account/orders"
            headers = self._require_sso_headers()
            payload = self._request_iserver_json("GET", url, headers, "get_open_orders")
            logger.success("Open orders fetched successfully")
            return payload
        finally:
            self.CLIENT_ID, self.KEY_ID, self.CLIENT_PRIVATE_KEY = original_creds


for _method_name in [
    "create_sso_session",
    "initialize_brokerage_session",
    "logout_of_brokerage_session",
    "get_brokerage_accounts",
    "get_portfolio_analyst_performance",
    "get_all_watchlists",
    "get_watchlist_information",
    "get_market_data_snapshot",
    "get_market_scanner_params",
    "run_market_scanner",
    "get_historical_market_data",
    "get_securities_by_symbol",
    "get_security_info",
    "get_all_conids_from_exchange",
    "get_contract_info",
    "place_order",
    "reply_to_order",
    "cancel_order",
    "get_open_orders",
]:
    if hasattr(IBKRTradingAPI, _method_name):
        setattr(
            IBKRTradingAPI,
            _method_name,
            retry_on_connection_error()(getattr(IBKRTradingAPI, _method_name)),
        )
