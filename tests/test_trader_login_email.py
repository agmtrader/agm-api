"""Exercise Trader authorization with real JWTs and a simulated Gmail API."""
import base64
from email import policy
from email.parser import BytesParser
import importlib
import sys
import types
import unittest
from unittest.mock import Mock, patch

from flask import Flask
from flask_jwt_extended import JWTManager, create_access_token


class TraderMailTest(unittest.TestCase):
    def setUp(self):
        logger = types.ModuleType("src.utils.logger")
        logger.logger = Mock()
        supabase = types.ModuleType("src.utils.connectors.supabase")
        supabase.db = Mock()
        gmail = types.ModuleType("src.utils.connectors.gmail")
        gmail.GmailConnector = Mock()
        self.send = gmail.GmailConnector.return_value.service.users.return_value.messages.return_value.send
        self.send.return_value.execute.return_value = {"id": "test-message"}
        self.modules = patch.dict(sys.modules, {
            "src.utils.logger": logger,
            "src.utils.connectors.supabase": supabase,
            "src.utils.connectors.gmail": gmail,
        })
        self.modules.start()
        self.addCleanup(self.modules.stop)
        authz = importlib.import_module("src.utils.authz")
        trader = importlib.import_module("src.app.tools.private.trader")

        self.scopes = {
            "trader": authz.TRADER_LOGIN_CODE_SCOPE,
            "github": "actions/run_screening_process",
            "broad": "all",
            "mixed": "all " + authz.TRADER_LOGIN_CODE_SCOPE,
        }
        supabase.db.read.side_effect = lambda table, query: (
            [{"id": query["id"], "scopes": self.scopes[query["id"]]}]
            if query["id"] in self.scopes else []
        )
        app = Flask(__name__)
        app.config["JWT_SECRET_KEY"] = "fixture-only-secret-key-at-least-thirty-two-bytes"
        JWTManager(app)
        app.before_request(authz.authenticate_request)
        app.before_request(authz.enforce_route_scope)
        app.register_blueprint(trader.bp, url_prefix="/trader")
        app.add_url_rule("/users/read", "users_read", lambda: {"unexpected": True})
        app.add_url_rule("/other", "other", lambda: {"unexpected": True})
        self.client = app.test_client()
        with app.app_context():
            self.headers = {
                identity: {"Authorization": "Bearer " + create_access_token(identity)}
                for identity in self.scopes
            }

    def send_code(self, identity=None, **fields):
        return self.client.post(
            "/trader/send_login_code",
            headers=self.headers.get(identity, {}),
            json={"email": "customer@example.test", "code": "00123456", **fields},
        )

    def test_trader_can_send_only_the_fixed_message_to_one_address(self):
        result = self.send_code("trader")
        self.assertEqual(result.status_code, 200)
        self.assertEqual(result.json, {"accepted": True})
        payload = self.send.call_args.kwargs
        self.assertEqual(payload["userId"], "me")
        message = BytesParser(policy=policy.default).parsebytes(
            base64.urlsafe_b64decode(payload["body"]["raw"])
        )
        self.assertEqual(message["To"], "customer@example.test")
        self.assertIn("00123456", message.get_content())
        self.assertNotIn("00123456", message["Subject"])
        self.assertIsNone(message["Cc"])
        self.assertIsNone(message["Bcc"])

    def test_credentials_cannot_be_used_outside_their_designated_routes(self):
        self.assertEqual(self.send_code().status_code, 401)
        for identity in ("github", "broad", "mixed"):
            self.assertEqual(self.send_code(identity).status_code, 403)
        for path in ("/users/read", "/other"):
            self.assertEqual(self.client.get(path, headers=self.headers["trader"]).status_code, 403)
        self.send.assert_not_called()

    def test_invalid_recipient_or_code_cannot_reach_gmail(self):
        for fields in (
            {"cc": "other@example.test"},
            {"email": "customer@example.test,other@example.test"},
            {"email": "customer@example.test\r\nBcc: other@example.test"},
            {"code": "1234567"},
            {"code": 12345678},
        ):
            self.assertEqual(self.send_code("trader", **fields).status_code, 400)
        self.send.assert_not_called()

    def test_gmail_failure_is_redacted(self):
        self.send.return_value.execute.side_effect = RuntimeError("private-gmail-data")
        result = self.send_code("trader")
        self.assertEqual(result.status_code, 502)
        self.assertNotIn(b"private-gmail-data", result.data)


if __name__ == "__main__":
    unittest.main()
