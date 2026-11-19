import json
import math
import time
from contextlib import contextmanager
from urllib.parse import urlsplit

import requests

from src.lib.ibkr_trading_api import MarketDataField
from src.utils.connectors.ibkr_web_api import IBKRWebAPI, retry_on_connection_error
from src.utils.exception import ServiceError, handle_exception
from src.utils.logger import logger
from src.utils.ibkr_errors import raise_ibkr_response_error

IBKR_TRADING_USERNAME = "aguiagm2024"


class IBKRTradingAPI(IBKRWebAPI):
    def __init__(self):
        super().__init__()
        self.sso_token = None
        self._market_http_session = requests.Session()

    @property
    def _market_http(self):
        # Keep IBKR's api/affinity cookies and connections through the lifecycle.
        # The fallback supports clients constructed without external dependencies.
        return getattr(self, '_market_http_session', None) or requests

    def _require_sso_headers(self, content_type: str | None = "application/json") -> dict:
        if not self.sso_token:
            raise ServiceError("Connect to IBKR before continuing.", status_code=409, code="ibkr_session_required")
        headers = {"Authorization": f"Bearer {self.sso_token}"}
        if content_type:
            headers["Content-Type"] = content_type
        return headers

    @handle_exception
    def create_sso_session(self, credential: str | None = None, ip: str | None = None) -> dict:
        if credential is not None and credential != IBKR_TRADING_USERNAME:
            raise ServiceError("The IBKR Trading API does not allow a username override.",
                               status_code=400, code="ibkr_trading_username_override")

        original_creds = self._apply_credentials("I6413690")
        try:
            if not ip:
                ip = requests.get("https://api.ipify.org", timeout=10).text.strip()
            logger.info("Creating IBKR Trading API SSO session")
            url = f"{self.BASE_URL}/gw/api/v1/sso-sessions"
            token = self.get_bearer_token()
            if not token:
                raise ServiceError("IBKR service authentication failed.", status_code=502, code="ibkr_authentication_failed")

            headers = {
                "Authorization": f"Bearer {token}",
                "Content-Type": "application/jwt",
            }
            payload = {
                "credential": IBKR_TRADING_USERNAME,
                "ip": ip,
            }

            signed_jwt = self.sign_request(payload)
            transport = getattr(self, '_market_http_session', None)
            if transport is not None:
                transport.cookies.clear()
            response = self._market_http.post(url, data=signed_jwt, headers=headers, timeout=(10, 45))
            if response.status_code != 200:
                raise_ibkr_response_error(response, validation=False)
            data = response.json()
            if "access_token" not in data:
                raise Exception(f"No access token found in response: {data}")
            self.sso_token = data["access_token"]
            return data
        finally:
            self.CLIENT_ID, self.KEY_ID, self.CLIENT_PRIVATE_KEY = original_creds

    @handle_exception
    def read_account_positions(self, account_id: str, credential: str | None = None) -> dict:
        """Open a short-lived IBKR session and read positions for one account."""
        if not account_id:
            raise ServiceError("account_id is required", status_code=400, code="validation_error")

        self.create_sso_session(credential=credential)
        self.initialize_brokerage_session()
        try:
            # IBKR requires this request before /portfolio2/{accountId}/positions.
            self.get_portfolio_accounts()
            return self.get_portfolio_positions(account_id=account_id)
        finally:
            try:
                self.logout_of_brokerage_session()
            except Exception as logout_error:
                logger.warning(f"Unable to close IBKR brokerage session: {logout_error}")

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
            response = self._market_http.post(url, data=json.dumps(payload), headers=headers, timeout=(10, 45))
            if response.status_code != 200:
                raise_ibkr_response_error(response, trading_session=True, account_access=True)
            return response.json()
        finally:
            self.CLIENT_ID, self.KEY_ID, self.CLIENT_PRIVATE_KEY = original_creds

    @contextmanager
    def market_data_session(self, credential: str | None = None, ip: str | None = None):
        """Run market-data calls inside one complete IBKR brokerage lifecycle.

        IBKR's ``iserver`` endpoints require the account bootstrap request after
        SSO/session initialization.  Keeping that bootstrap and logout in one
        context prevents callers from using a half-ready session or leaking a
        brokerage session when an ETL step fails.
        """
        self._market_data_context = {'ip': ip, 'recoveries': [], 'failures': []}
        try:
            self.create_sso_session(credential=credential, ip=ip)
            self.initialize_brokerage_session()
            # IBKR documents this settlement interval before further /iserver calls.
            time.sleep(2)
            accounts = self.get_brokerage_accounts()
            readiness = self._wait_for_market_data_ready()
            logger.info(
                f"IBKR market-data session ready: selected_account={accounts.get('selectedAccount')} "
                f"established={readiness.get('established', 'unavailable')}"
            )
            yield self
        finally:
            if self.sso_token:
                try:
                    self.logout_of_brokerage_session()
                except Exception as logout_error:
                    logger.warning(f"Unable to close IBKR market-data session: {logout_error}")
                finally:
                    self.sso_token = None
            self._market_data_context = None
            transport = getattr(self, '_market_http_session', None)
            if transport is not None:
                transport.close()
                self._market_http_session = requests.Session()

    def _market_auth_snapshot(self):
        """Retain only authentication flags; diagnostic failure cannot replace the read error."""
        try:
            response = self._market_http.post(self.BASE_URL + '/v1/api/iserver/auth/status',
                                     headers=self._require_sso_headers(), timeout=(10, 10))
            payload = response.json()
            flags = {key: payload[key] for key in ('authenticated', 'connected', 'established', 'competing')
                     if isinstance(payload, dict) and type(payload.get(key)) is bool}
            return {'http_status': response.status_code, **flags}
        except (requests.RequestException, ValueError, ServiceError):
            return {'unavailable': True}

    def _wait_for_market_data_ready(self):
        """Initial authentication is not final brokerage establishment.

        Some older IBKR responses omit the optional established flag. Honor it
        whenever supplied; never accept an explicit false or missing auth flags.
        This polls status only and never competes for/recreates another session.
        """
        for attempt in range(20):
            status = self._market_auth_snapshot()
            if status.get('competing') is True:
                raise ServiceError('Another IBKR brokerage session is competing with this session.',
                                   409, code='ibkr_session_competing')
            if status.get('http_status') == 401:
                raise ServiceError('The IBKR trading session has expired. Reconnect to IBKR and try again.',
                                   409, code='ibkr_session_expired')
            if (status.get('http_status') == 200 and status.get('authenticated') is True
                    and status.get('connected') is True and status.get('established') is not False):
                context = getattr(self, '_market_data_context', None)
                if context is not None:
                    context['readiness'] = status
                return status
            if attempt < 19:
                time.sleep(1)
        raise ServiceError('IBKR brokerage session did not become fully established.',
                           409, code='ibkr_session_not_ready')

    def _request_market_data_json(self, method: str, url: str, **kwargs):
        """Recover one explicitly safe read per extract, retaining its warning.

        Only the observed expired-session/accounts-bootstrap errors qualify.
        Orders and other mutations can never enter this recovery path.
        """
        endpoint = urlsplit(url).path.removeprefix('/v1/api/iserver/')
        allowed = {('GET', 'watchlists'), ('GET', 'watchlist'),
                   ('POST', 'secdef/search'), ('GET', 'marketdata/snapshot'),
                   ('GET', 'marketdata/history')}
        if (method, endpoint) not in allowed or urlsplit(url).netloc != urlsplit(self.BASE_URL).netloc:
            raise ValueError('Market session recovery only permits allowlisted market-data reads')
        kwargs.pop('headers', None)
        kwargs.setdefault('timeout', (10, 45))
        for attempt in range(2):
            response = self._market_http.request(method, url, headers=self._require_sso_headers(), **kwargs)
            if response.status_code == 200:
                return response.json()
            try:
                raise_ibkr_response_error(response, trading_session=True, account_access=True)
            except ServiceError as error:
                context = getattr(self, '_market_data_context', None)
                auth_status = {}
                if context and error.code in ('ibkr_session_expired', 'ibkr_session_not_ready'):
                    auth_status = self._market_auth_snapshot()
                    context['failures'].append({'reason': error.code, 'endpoint': endpoint,
                        'provider_diagnostics': error.details, 'auth_status': auth_status})
                if (attempt or not context or context['recoveries']
                        or error.code not in ('ibkr_session_expired', 'ibkr_session_not_ready')):
                    raise
                if auth_status.get('competing') is True:
                    raise
                record = {'reason': error.code, 'endpoint': endpoint}
                context['recoveries'].append(record)
                logger.warning(f'IBKR_MARKET_SESSION_RECOVERY reason={error.code} endpoint={endpoint} attempt=1 limit=1')
                # The accounts-first 500 also occurs after the brokerage session
                # disconnects. An accounts read cannot restore that session.
                if (error.code == 'ibkr_session_expired' or auth_status.get('authenticated') is False
                        or auth_status.get('connected') is False):
                    record['session_recreated'] = True
                    self.create_sso_session(ip=context['ip'])
                    self.initialize_brokerage_session()
                    time.sleep(2)
                self.get_brokerage_accounts()
                self._wait_for_market_data_ready()
                if endpoint == 'marketdata/history':
                    time.sleep(1.25)

    @handle_exception
    def logout_of_brokerage_session(self) -> dict:
        try:
            original_creds = self._apply_credentials("I6413690")
            url = f"{self.BASE_URL}/v1/api/logout"
            response = self._market_http.post(url, headers=self._require_sso_headers(), timeout=(10, 20))
            if response.status_code != 200:
                raise_ibkr_response_error(response, trading_session=True, account_access=True)
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

            # IBKR can acknowledge session initialization before its accounts
            # are loaded. Poll this read-only readiness endpoint for a bounded
            # interval during market-data sessions; never use an empty response.
            market_context = getattr(self, '_market_data_context', None)
            max_attempts = 20 if market_context else 3
            for attempt in range(1, max_attempts + 1):
                response = self._market_http.get(url, headers=headers, timeout=(10, 20))
                if response.status_code != 200:
                    raise_ibkr_response_error(response, trading_session=True, account_access=True)

                payload = response.json()
                has_accounts_shape = (
                    isinstance(payload, dict)
                    and isinstance(payload.get("accounts"), list)
                    and payload.get("selectedAccount")
                    and isinstance(payload.get("aliases"), dict)
                )
                if has_accounts_shape:
                    break

                if attempt < max_attempts:
                    if market_context and attempt == 1:
                        logger.warning('IBKR_ACCOUNTS_NOT_READY polling read-only account bootstrap')
                    time.sleep(1 if market_context else 0.5)

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
                raise ServiceError('IBKR brokerage accounts did not become ready.',
                                   status_code=502, code='ibkr_accounts_not_ready')

            if market_context and attempt > 1:
                logger.warning(f'IBKR_ACCOUNTS_READY attempts={attempt}')

            return payload
        finally:
            self.CLIENT_ID, self.KEY_ID, self.CLIENT_PRIVATE_KEY = original_creds

    def _prime_iserver_session(self, reason: str):
        return self.get_brokerage_accounts()

    def _request_iserver_json(self, method: str, url: str, headers: dict, reason: str, **kwargs):
        if (getattr(self, '_market_data_context', None)
                and urlsplit(url).path in ('/v1/api/iserver/watchlists', '/v1/api/iserver/watchlist')):
            return self._request_market_data_json(method, url, **kwargs)
        self._prime_iserver_session(reason)
        response = self._market_http.request(method, url, headers=headers, **kwargs)

        should_retry = (
            response.status_code in (410, 500)
            and (
                response.status_code == 410
                or "Please query /accounts first" in response.text
            )
        )
        if should_retry:
            self._prime_iserver_session(f"{reason} retry")
            response = self._market_http.request(method, url, headers=headers, **kwargs)

        if response.status_code != 200:
            raise_ibkr_response_error(response, trading_session=True, account_access=True)
        return response.json()

    @handle_exception
    def get_portfolio_analyst_performance(
        self,
        acct_ids: list | None = None,
        period: str | None = None,
        freq: str | None = None,
    ) -> dict:
        try:
            original_creds = self._apply_credentials("I6413690")
            url = f"{self.BASE_URL}/v1/api/pa/performance"
            resolved_period = period or freq
            payload = {
                "acctIds": acct_ids or [],
                "period": resolved_period,
            }
            response = requests.post(url, headers=self._require_sso_headers(), data=json.dumps(payload))
            if response.status_code != 200:
                raise_ibkr_response_error(response, trading_session=True, account_access=True)
            return response.json()
        finally:
            self.CLIENT_ID, self.KEY_ID, self.CLIENT_PRIVATE_KEY = original_creds

    @handle_exception
    def get_portfolio_analyst_allocation(
        self,
        acct_ids: list | None = None,
        allocation_type: str = "ALL",
        currency: str = "USD",
        date: str | None = None,
        model: str | None = None,
    ) -> dict:
        try:
            original_creds = self._apply_credentials("I6413690")
            url = f"{self.BASE_URL}/v1/api/pa/allocation"
            payload = {
                "acctIds": acct_ids or [],
                "type": allocation_type,
                "currency": currency,
            }
            if date:
                payload["date"] = date
            if model:
                payload["model"] = model

            response = requests.post(url, headers=self._require_sso_headers(), data=json.dumps(payload))
            if response.status_code != 200:
                raise_ibkr_response_error(response, trading_session=True, account_access=True)
            return response.json()
        finally:
            self.CLIENT_ID, self.KEY_ID, self.CLIENT_PRIVATE_KEY = original_creds

    @handle_exception
    def get_portfolio_analyst_transactions(
        self,
        acct_ids: list | None = None,
        conids: list | None = None,
        currency: str = "USD",
        days: int = 90,
    ) -> dict:
        try:
            original_creds = self._apply_credentials("I6413690")
            url = f"{self.BASE_URL}/v1/api/pa/transactions"
            payload = {
                "acctIds": acct_ids or [],
                "currency": currency,
                "days": days,
            }
            if conids:
                payload["conids"] = conids

            response = requests.post(url, headers=self._require_sso_headers(), data=json.dumps(payload))
            if response.status_code != 200:
                raise_ibkr_response_error(response, trading_session=True, account_access=True)
            return response.json()
        finally:
            self.CLIENT_ID, self.KEY_ID, self.CLIENT_PRIVATE_KEY = original_creds

    @handle_exception
    def get_portfolio_accounts(self) -> dict:
        try:
            original_creds = self._apply_credentials("I6413690")
            url = f"{self.BASE_URL}/v1/api/portfolio/accounts"
            headers = self._require_sso_headers()
            payload = self._request_iserver_json("GET", url, headers, "get_portfolio_accounts")
            logger.success("Portfolio accounts fetched successfully")
            return payload
        finally:
            self.CLIENT_ID, self.KEY_ID, self.CLIENT_PRIVATE_KEY = original_creds

    @handle_exception
    def get_portfolio_positions(self, account_id: str, model: str | None = None, direction: str = "a") -> dict:
        try:
            original_creds = self._apply_credentials("I6413690")
            params = [f"direction={direction}"]
            if model:
                params.append(f"model={model}")
            query = "&".join(params)
            url = f"{self.BASE_URL}/v1/api/portfolio2/{account_id}/positions?{query}"
            headers = self._require_sso_headers()
            payload = self._request_iserver_json(
                "GET",
                url,
                headers,
                f"get_portfolio_positions account_id={account_id}",
            )
            logger.success(f"Portfolio positions fetched successfully for {account_id}")
            return payload
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
    def get_market_data_snapshot(self, conids: str, *, poll_interval: float | None = None,
                                 timeout: float = 10.0, settle_seconds: float = 0.75):
        if poll_interval is not None and (not all(math.isfinite(value) for value in (poll_interval, timeout, settle_seconds))
                                         or poll_interval < 0.1 or timeout <= 0 or settle_seconds < 0):
            raise ValueError('Snapshot polling requires interval >= 0.1, timeout > 0 and settle_seconds >= 0')
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
            if poll_interval is not None:
                raw_data = self._poll_market_data_snapshot(
                    f"{url}&fields={fields_str}", conids.split(','), poll_interval, timeout, settle_seconds)
                return self._translate_market_data_snapshot(raw_data)
            self._request_market_data_json('GET', f"{url}&fields={fields_str}")

            logger.info("Waiting for market data snapshot to be ready")
            time.sleep(10)
            context = getattr(self, '_market_data_context', None) or {}
            recovery_count = len(context.get('recoveries', []))
            raw_data = self._request_market_data_json('GET', f"{url}&fields={fields_str}")
            # A recovery during retrieval opens fresh subscriptions. An identity-
            # only acknowledgement is preflight, not the completed snapshot.
            recreated = any(record.get('session_recreated') for record in
                            context.get('recoveries', [])[recovery_count:])
            if recreated and isinstance(raw_data, list) and any(
                isinstance(row, dict) and not any(
                    isinstance(key, str) and key.isdigit() and int(key) not in (6119, 6509)
                    for key in row
                ) for row in raw_data
            ):
                logger.warning('IBKR_SNAPSHOT_REPRIMED waiting for recovered market-data subscriptions')
                time.sleep(10)
                raw_data = self._request_market_data_json('GET', f"{url}&fields={fields_str}")

            logger.success("Market data snapshot fetched successfully")
            return self._translate_market_data_snapshot(raw_data)
        finally:
            self.CLIENT_ID, self.KEY_ID, self.CLIENT_PRIVATE_KEY = original_creds

    @staticmethod
    def _translate_market_data_snapshot(raw_data):
        def translate(item):
            mapped = {}
            for key, value in item.items():
                if isinstance(key, str) and key.isdigit():
                    try:
                        key = MarketDataField(int(key)).name
                    except ValueError:
                        pass
                mapped[key] = value
            return mapped
        if isinstance(raw_data, list):
            return [translate(row) for row in raw_data]
        return translate(raw_data) if isinstance(raw_data, dict) else raw_data

    def _poll_market_data_snapshot(self, url, conids, interval, timeout, settle_seconds):
        """Merge incremental fields; stabilize coverage rather than changing prices.

        A deadline is reported as partial readiness, never as complete quote data.
        Recreating SSO invalidates all previously accumulated subscription data.
        """
        expected = set(conids)
        if not expected or any(not value.isdigit() for value in expected) or len(expected) > 100:
            raise ValueError('Snapshot polling accepts 1–100 numeric contract IDs')
        started = time.monotonic()
        deadline = started + timeout
        rows, previous_coverage, stable_since = {}, None, started
        polls = 0
        context = getattr(self, '_market_data_context', None) or {}
        if context:
            context.pop('snapshot_polling', None)
        def usable(value):
            return value is not None and str(value).strip().lower() not in ('', '-', '--', 'n/a', 'nan', 'none', 'null')
        while True:
            recoveries_before = len(context.get('recoveries', []))
            payload = self._request_market_data_json('GET', url)
            polls += 1
            if any(record.get('session_recreated') for record in context.get('recoveries', [])[recoveries_before:]):
                rows.clear()
                previous_coverage = None
            if not isinstance(payload, list):
                raise ValueError('Snapshot polling expected a list of rows')
            seen = set()
            for row in payload:
                conid = str(row.get('conid', '')) if isinstance(row, dict) else ''
                if conid not in expected or conid in seen:
                    raise ValueError('Snapshot polling returned an unknown or duplicate contract')
                seen.add(conid)
                rows.setdefault(conid, {}).update(row)
            now = time.monotonic()
            coverage = frozenset((conid, key) for conid, row in rows.items()
                                 for key, value in row.items() if key.isdigit() and usable(value))
            if coverage != previous_coverage:
                previous_coverage, stable_since = coverage, now
            quote_ready = {conid for conid, row in rows.items()
                           if not row.get('error') and usable(row.get('55')) and usable(row.get('7220'))
                           and any(usable(row.get(key)) for key in ('31', '84', '86'))}
            ready = quote_ready == expected and now - stable_since >= settle_seconds
            timed_out = now >= deadline and not ready
            metrics = {'requests': polls, 'elapsed_seconds': now - started,
                       'quote_ready_contracts': len(quote_ready), 'requested_contracts': len(expected),
                       'coverage_stable': ready, 'timed_out': timed_out}
            if context:
                context['snapshot_polling'] = metrics
            if ready or timed_out:
                logger.info(f"IBKR_SNAPSHOT_POLLING requests={polls} ready={len(quote_ready)}/{len(expected)} "
                            f"seconds={now - started:.3f} timed_out={timed_out}")
                return [rows[conid] for conid in conids if conid in rows]
            time.sleep(min(interval, max(0, deadline - now)))

    @handle_exception
    def unsubscribe_all_market_data(self):
        """Release this owned market-data session's streams without reconnecting.

        Subscription cleanup must be confirmed before the next batch. It is not
        included in automatic safe-read recovery, despite its HTTP GET method.
        """
        if not getattr(self, '_market_data_context', None):
            raise ServiceError('Subscription cleanup requires an owned market-data session.',
                               409, code='ibkr_session_required')
        response = self._market_http.get(self.BASE_URL + '/v1/api/iserver/marketdata/unsubscribeall',
                                        headers=self._require_sso_headers(), timeout=(10, 20))
        if response.status_code != 200:
            raise_ibkr_response_error(response, trading_session=True, account_access=True)
        payload = response.json()
        if not isinstance(payload, dict) or payload.get('unsubscribed') is not True:
            raise ServiceError('IBKR did not confirm market-data subscription cleanup.',
                               502, code='ibkr_market_data_cleanup_unconfirmed')
        return payload

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
    def get_historical_market_data(
        self,
        conid: str,
        period: str,
        bar: str,
        *,
        start_time: str | None = None,
        direction: int | None = None,
        source: str = "Last",
        exchange: str | None = None,
        outside_rth: bool = True,
    ) -> dict:
        try:
            original_creds = self._apply_credentials("I6413690")
            url = f"{self.BASE_URL}/v1/api/iserver/marketdata/history"
            params = {
                "conid": conid,
                "period": period,
                "bar": bar,
                "outsideRth": outside_rth,
                "source": source,
            }
            if start_time:
                params["startTime"] = start_time
            if direction is not None:
                params["direction"] = direction
            if exchange:
                params["exchange"] = exchange
            logger.info(
                f"IBKR_HISTORY_REQUEST conid={conid} period={period} bar={bar} "
                f"start_time={start_time or '-'} direction={direction or '-'}"
            )
            payload = self._request_market_data_json('GET', url, params=params)
            if isinstance(payload, dict) and (payload.get('error') or payload.get('message')):
                logger.warning(
                    f"IBKR_HISTORY_FAILED conid={conid} status=200 "
                    f"reason={payload.get('error') or payload.get('message')}"
                )
            else:
                logger.success(f"IBKR_HISTORY_SUCCESS conid={conid}")
            return payload
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
            result = self._request_market_data_json('POST', url, data=json.dumps(payload))
            logger.success("Bonds searched successfully")
            return result
        finally:
            self.CLIENT_ID, self.KEY_ID, self.CLIENT_PRIVATE_KEY = original_creds

    @handle_exception
    def get_security_info(self, issuer_id: str, sec_type: str) -> dict:
        try:
            original_creds = self._apply_credentials("I6413690")
            url = f"{self.BASE_URL}/v1/api/iserver/secdef/info?issuerId={issuer_id}&secType={sec_type}"
            response = requests.get(url, headers=self._require_sso_headers())
            if response.status_code != 200:
                raise_ibkr_response_error(response, trading_session=True, account_access=True)
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
                raise_ibkr_response_error(response, trading_session=True, account_access=True)
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
                raise_ibkr_response_error(response, trading_session=True, account_access=True)
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
                raise_ibkr_response_error(response, trading_session=True, account_access=True)
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
                raise_ibkr_response_error(response, trading_session=True, account_access=True)
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
                raise_ibkr_response_error(response, trading_session=True, account_access=True)
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
    "read_account_positions",
    "initialize_brokerage_session",
    "logout_of_brokerage_session",
    "get_brokerage_accounts",
    "get_portfolio_accounts",
    "get_portfolio_positions",
    "get_portfolio_analyst_allocation",
    "get_portfolio_analyst_performance",
    "get_portfolio_analyst_transactions",
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
