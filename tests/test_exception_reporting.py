"""Synthetic HTTP and log regressions; no database or external integrations."""
import io
import logging
import unittest
from unittest.mock import patch
from flask import Flask, g
from src.utils import exception as errors
from src.utils.response import format_response


class ExceptionReportingTests(unittest.TestCase):
    def setUp(self):
        self.output = io.StringIO()
        self.handler = logging.StreamHandler(self.output)
        self.log = logging.Logger('exception-acceptance', level=logging.ERROR)
        self.log.addHandler(self.handler)
        self.swap = patch.object(errors.logger, 'logger', self.log)
        self.swap.start()
        self.app = Flask(__name__)
        self.app.before_request(lambda: setattr(g, 'request_id', 'safe-correlation-id'))

    def tearDown(self):
        self.swap.stop()

    def route(self, operation):
        decorated = errors.handle_exception(errors.handle_exception(operation))
        self.app.add_url_rule('/failure/<item>', 'failure', format_response(decorated))
        return self.app.test_client()

    def test_expected_http_failures_preserve_messages_and_do_not_report(self):
        def operation(item):
            raise errors.ServiceError('Correct the input', int(item), code='validation_error', details={'validation_errors': ['Correct this field']})
        client = self.route(operation)
        for status in (400, 401, 403, 409, 422, 429):
            response = client.get(f'/failure/{status}')
            self.assertEqual(response.status_code, status)
            self.assertEqual(response.json['error'], 'Correct the input')
            self.assertEqual(response.json['error_id'], 'safe-correlation-id')
            self.assertEqual(response.json['details']['validation_errors'], ['Correct this field'])
        self.assertEqual(self.output.getvalue(), '')

    def test_server_failure_reports_once_without_query_details_or_actual_path(self):
        def operation(item):
            raise errors.ServiceError('synthetic-message-secret', 502, code='upstream_failed', details={'token': 'synthetic-detail-secret'})
        response = self.route(operation).get('/failure/synthetic-path-secret?token=synthetic-query-secret&email=synthetic-email-secret')
        logged = self.output.getvalue()
        self.assertEqual(response.status_code, 502)
        # The controlled response contract is unchanged; logs use an allowlist.
        self.assertEqual(response.json['error'], 'synthetic-message-secret')
        self.assertEqual(response.json['details']['token'], 'synthetic-detail-secret')
        self.assertEqual(logged.count('failed [error_id='), 1)
        self.assertIn('safe-correlation-id', logged)
        self.assertIn('/failure/<item>', logged)
        self.assertIn('status=502', logged)
        self.assertIn('upstream_failed', logged)
        self.assertIn('src.utils.exception.ServiceError', logged)
        self.assertIn('in operation', logged)
        for marker in ('synthetic-message-secret', 'synthetic-detail-secret', 'synthetic-path-secret', 'synthetic-query-secret', 'synthetic-email-secret'):
            self.assertNotIn(marker, logged)

    def test_unexpected_bug_keeps_trace_type_but_omits_message_and_source_line(self):
        def operation(item):
            raise RuntimeError('synthetic-literal-secret')
        response = self.route(operation).get('/failure/bug')
        logged = self.output.getvalue()
        self.assertEqual(response.status_code, 500)
        self.assertEqual(response.json['error'], 'Internal server error')
        self.assertEqual(response.json['code'], 'internal_error')
        self.assertEqual(logged.count('Unhandled error in'), 1)
        self.assertIn('builtins.RuntimeError', logged)
        self.assertIn('Traceback (most recent call last):', logged)
        self.assertIn('in operation', logged)
        self.assertNotIn('synthetic-literal-secret', logged)
        self.assertIn(response.json['error_id'], logged)

    def test_chained_failures_preserve_both_types_without_either_message(self):
        def operation(item):
            try:
                raise ValueError('synthetic-cause-secret')
            except ValueError as cause:
                raise RuntimeError('synthetic-outer-secret') from cause
        self.route(operation).get('/failure/bug')
        logged = self.output.getvalue()
        self.assertEqual(logged.count('Unhandled error in'), 1)
        self.assertIn('builtins.ValueError', logged)
        self.assertIn('builtins.RuntimeError', logged)
        self.assertIn('direct cause', logged)
        self.assertNotIn('synthetic-cause-secret', logged)
        self.assertNotIn('synthetic-outer-secret', logged)

    def test_suppressed_context_is_not_reported(self):
        def operation(item):
            try:
                raise ValueError('synthetic-suppressed-secret')
            except ValueError:
                raise RuntimeError('synthetic-visible-secret') from None
        self.route(operation).get('/failure/bug')
        self.assertNotIn('ValueError', self.output.getvalue())
        self.assertIn('RuntimeError', self.output.getvalue())

    def test_background_failure_without_request_context_is_reportable_once(self):
        @errors.handle_exception
        def background():
            raise errors.ServiceError('synthetic-job-secret', 500)
        with self.assertRaises(errors.ServiceError) as caught:
            background()
        logged = self.output.getvalue()
        errors.log_service_error(caught.exception, 'second_boundary')
        self.assertEqual(self.output.getvalue(), logged)
        self.assertIn('context={}', logged)
        self.assertIn(caught.exception.error_id, logged)
        self.assertNotIn('synthetic-job-secret', logged)

    def test_cycle_does_not_break_reporting(self):
        first = ValueError('synthetic-first-secret')
        second = RuntimeError('synthetic-second-secret')
        first.__cause__ = second
        second.__cause__ = first
        result = errors._safe_traceback(first)
        self.assertEqual(result.count('builtins.ValueError'), 1)
        self.assertEqual(result.count('builtins.RuntimeError'), 1)
        self.assertNotIn('synthetic-first-secret', result)

    def test_success_response_stays_successful_and_emits_no_error(self):
        response = self.route(lambda item: {'saved': True}).get('/failure/success')
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json, {'saved': True})
        self.assertEqual(self.output.getvalue(), '')


if __name__ == '__main__':
    unittest.main()
