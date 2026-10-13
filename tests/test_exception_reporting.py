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
        self.assertIn('src.utils.exception.ServiceError: synthetic-message-secret', logged)
        self.assertNotIn('message omitted', logged)
        self.assertIn('in operation', logged)
        for marker in ('synthetic-detail-secret', 'synthetic-path-secret', 'synthetic-query-secret', 'synthetic-email-secret'):
            self.assertNotIn(marker, logged)

    def test_unexpected_bug_keeps_message_but_omits_source_line(self):
        def operation(item):
            raise RuntimeError('synthetic-literal-secret')
        response = self.route(operation).get('/failure/bug')
        logged = self.output.getvalue()
        self.assertEqual(response.status_code, 500)
        self.assertEqual(response.json['error'], 'Internal server error')
        self.assertEqual(response.json['code'], 'internal_error')
        self.assertEqual(logged.count('failed [error_id='), 1)
        self.assertIn('src.utils.exception.ServiceError', logged)
        self.assertIn('Traceback (most recent call last):', logged)
        self.assertIn('in operation', logged)
        self.assertIn('ServiceError: Internal server error', logged)
        self.assertNotIn('synthetic-literal-secret', logged)
        self.assertNotIn("raise RuntimeError(", logged)
        self.assertIn(response.json['error_id'], logged)

    def test_chained_failures_propagate_one_final_error(self):
        def operation(item):
            try:
                raise ValueError('synthetic-cause-secret')
            except ValueError as cause:
                raise RuntimeError('synthetic-outer-secret') from cause
        self.route(operation).get('/failure/bug')
        logged = self.output.getvalue()
        self.assertEqual(logged.count('failed [error_id='), 1)
        self.assertIn('ServiceError: Internal server error', logged)
        self.assertEqual(logged.count('Traceback (most recent call last):'), 1)
        for marker in ('builtins.ValueError', 'builtins.RuntimeError', 'direct cause', 'synthetic-cause-secret', 'synthetic-outer-secret'):
            self.assertNotIn(marker, logged)

    def test_suppressed_context_is_not_reported(self):
        def operation(item):
            try:
                raise ValueError('synthetic-suppressed-secret')
            except ValueError:
                raise RuntimeError('synthetic-visible-secret') from None
        self.route(operation).get('/failure/bug')
        self.assertNotIn('ValueError', self.output.getvalue())
        self.assertIn('ServiceError', self.output.getvalue())

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
        self.assertIn('synthetic-job-secret', logged)

    def test_cycle_does_not_break_reporting(self):
        first = ValueError('synthetic-first-secret')
        second = RuntimeError('synthetic-second-secret')
        first.__cause__ = second
        second.__cause__ = first
        result = errors._safe_traceback(first)
        self.assertEqual(result.count('builtins.ValueError'), 1)
        self.assertEqual(result.count('builtins.RuntimeError'), 1)
        self.assertIn('synthetic-first-secret', result)

    def test_google_drive_http_error_returns_one_final_failure(self):
        from googleapiclient.errors import HttpError
        from httplib2 import Response

        def operation(item):
            raise HttpError(
                Response({'status': '403', 'reason': 'Forbidden'}),
                b'{"error":{"message":"User rate limit exceeded","errors":[{"reason":"userRateLimitExceeded","message":"User rate limit exceeded"}]}}',
                uri='https://www.googleapis.com/drive/v3/files/synthetic-file',
            )

        response = self.route(operation).get('/failure/drive')
        logged = self.output.getvalue()
        self.assertEqual(response.status_code, 500)
        self.assertIn('ServiceError: Internal server error', logged)
        self.assertNotIn('googleapiclient.errors.HttpError:', logged)
        self.assertNotIn('synthetic-file', logged)
        self.assertNotIn('User rate limit exceeded', logged)
        self.assertNotIn('message omitted', logged)
        self.assertEqual(logged.count('failed [error_id='), 1)

    def test_success_response_stays_successful_and_emits_no_error(self):
        response = self.route(lambda item: {'saved': True}).get('/failure/success')
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json, {'saved': True})
        self.assertEqual(self.output.getvalue(), '')


if __name__ == '__main__':
    unittest.main()


class PropagationBoundaryTests(unittest.TestCase):
    setUp = ExceptionReportingTests.setUp
    tearDown = ExceptionReportingTests.tearDown
    route = ExceptionReportingTests.route
    def test_inner_handlers_preserve_one_failure_object_without_reporting(self):
        failure = errors.ServiceError('final controlled failure', 503, code='final_failure')
        seen = []
        @errors.handle_exception
        def inner():
            raise failure
        @errors.handle_exception
        def middle():
            try:
                inner()
            except errors.ServiceError as err:
                seen.append(err)
                self.assertEqual(self.output.getvalue(), '')
                raise
        def outer(item):
            try:
                middle()
            except errors.ServiceError as err:
                seen.append(err)
                self.assertIs(err, failure)
                self.assertEqual(self.output.getvalue(), '')
                raise
        response = self.route(outer).get('/failure/one')
        self.assertEqual(response.status_code, 503)
        self.assertEqual(seen, [failure, failure])
        self.assertEqual(self.output.getvalue().count('Traceback (most recent call last):'), 1)
        self.assertIn('outer failed', self.output.getvalue())

    def test_final_replacement_error_is_the_only_propagated_chain(self):
        @errors.handle_exception
        def inner():
            raise errors.ServiceError('private inner marker', 502)
        @errors.handle_exception
        def outer(item):
            try:
                inner()
            except errors.ServiceError as exc:
                raise errors.ServiceError('final operation failed', 503, code='final_failure') from exc
        response = self.route(outer).get('/failure/one')
        logged = self.output.getvalue()
        self.assertEqual(response.status_code, 503)
        self.assertIn('final operation failed', logged)
        self.assertNotIn('private inner marker', logged)
        self.assertNotIn('direct cause', logged)
        self.assertEqual(logged.count('Traceback (most recent call last):'), 1)

    def test_handled_nested_failure_does_not_report_or_poison_next_call(self):
        @errors.handle_exception
        def inner():
            raise RuntimeError('synthetic handled failure')
        @errors.handle_exception
        def job():
            try:
                inner()
            except errors.ServiceError:
                return {'handled': True}
        self.assertEqual(job(), {'handled': True})
        self.assertEqual(self.output.getvalue(), '')
        self.assertEqual(errors._component_depth.get(), 0)
        @errors.handle_exception
        def failing_job():
            raise RuntimeError('next failure')
        with self.assertRaises(errors.ServiceError):
            failing_job()
        self.assertIn('failing_job failed', self.output.getvalue())
        self.assertEqual(self.output.getvalue().count('Traceback (most recent call last):'), 1)
        self.assertEqual(errors._component_depth.get(), 0)

    def test_undecorated_final_service_error_has_one_visible_exception(self):
        final = errors.ServiceError('final operation failure', 503, code='final_failure')
        final.__cause__ = ValueError('private lower-level marker')
        errors.log_service_error(final, 'final_flask_boundary')
        logged = self.output.getvalue()
        self.assertIn('ServiceError: final operation failure', logged)
        self.assertNotIn('private lower-level marker', logged)
        self.assertEqual(logged.count('Traceback (most recent call last):'), 1)
        self.assertIsNone(final.__cause__)

    def test_nested_unexpected_failure_is_normalized_only_once(self):
        @errors.handle_exception
        def inner():
            raise RuntimeError('private inner marker')
        @errors.handle_exception
        def middle():
            return inner()
        def outer(item):
            return middle()
        with patch.object(errors, 'wrap_unhandled_exception', wraps=errors.wrap_unhandled_exception) as normalize:
            response = self.route(outer).get('/failure/one')
        self.assertEqual(response.status_code, 500)
        self.assertEqual(normalize.call_count, 1)
        self.assertEqual(self.output.getvalue().count('Traceback (most recent call last):'), 1)
        self.assertNotIn('private inner marker', self.output.getvalue())

    def test_concurrent_direct_flows_keep_independent_exception_depth(self):
        from concurrent.futures import ThreadPoolExecutor
        from threading import Barrier
        barrier = Barrier(2)
        @errors.handle_exception
        def inner():
            barrier.wait(timeout=5)
            raise RuntimeError('private thread marker')
        @errors.handle_exception
        def job():
            return inner()
        def invoke():
            try:
                job()
            except errors.ServiceError:
                return errors._component_depth.get()
        with ThreadPoolExecutor(max_workers=2) as pool:
            depths = list(pool.map(lambda _: invoke(), range(2)))
        self.assertEqual(depths, [0, 0])
        logged = self.output.getvalue()
        self.assertEqual(logged.count('job failed'), 2)
        self.assertNotIn('inner failed', logged)
        self.assertEqual(logged.count('Traceback (most recent call last):'), 2)
        self.assertNotIn('private thread marker', logged)
