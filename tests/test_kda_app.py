#!/usr/bin/python

import library.kda_app as kda_app
from library.kda_app import KinesisDataAnalyticsApp
import mock
from mock import patch
import unittest
from botocore.exceptions import ClientError


class TestKinesisDataAnalyticsApp(unittest.TestCase):

    def setUp(self):
        self.module = mock.MagicMock()
        self.module.check_mode = False
        self.module.exit_json = mock.MagicMock()
        self.module.fail_json = mock.MagicMock()
        self.app = KinesisDataAnalyticsApp(self.module)
        self.app.client = mock.MagicMock()
        self.app.module.params = {
            'name': 'testifyApp',
        }
        reload(kda_app)

    def test_boto_module_not_found(self):
        import __builtin__ as builtins
        real_import = builtins.__import__

        def mock_import(name, *args):
            if name == 'boto': raise ImportError
            return real_import(name, *args)

        with mock.patch('__builtin__.__import__', side_effect=mock_import):
            reload(kda_app)
            KinesisDataAnalyticsApp(self.module)

        self.module.fail_json.assert_called_with(msg='boto and boto3 are required for this module')

    def test_boto3_module_not_found(self):
        import __builtin__ as builtins
        real_import = builtins.__import__

        def mock_import(name, *args):
            if name == 'boto3': raise ImportError
            return real_import(name, *args)

        with mock.patch('__builtin__.__import__', side_effect=mock_import):
            reload(kda_app)
            KinesisDataAnalyticsApp(self.module)

        self.module.fail_json.assert_called_with(msg='boto and boto3 are required for this module')

    @patch.object(kda_app, 'boto3')
    def test_boto3_client_properly_instantiated(self, mock_boto):
        KinesisDataAnalyticsApp(self.module)
        mock_boto.client.assert_called_once_with('kinesisanalytics')

    def test_process_request_calls_describe_application_and_stores_result_when_invoked(self):
        resp = {
            'app': 'lol',
        }
        self.app.client.describe_application = mock.MagicMock(return_value=resp)

        self.app.process_request()

        self.assertEqual(resp, self.app.current_state)
        self.app.client.describe_application.assert_called_once_with(ApplicationName='testifyApp')

    def test_process_request_calls_create_application_when_application_not_found(self):
        resource_not_found = {'Error': {'Code': 'ResourceNotFoundException'}}
        self.app.client.describe_application = mock.MagicMock(side_effect=ClientError(resource_not_found, ''))
        self.app.client.create_application = mock.MagicMock()

        self.app.process_request()

        self.app.client.describe_application.assert_called_once()
        self.app.client.create_application.assert_called_once()

    def test_process_request_do_not_call_create_application_when_describe_application_call_fails(self):
        unknown_exception = {'Error': {'Code': 'lol'}}
        self.app.client.describe_application = mock.MagicMock(side_effect=ClientError(unknown_exception, ''))
        self.app.client.create_application = mock.MagicMock()

        self.app.process_request()

        self.app.client.describe_application.assert_called_once()
        self.app.client.create_application.assert_not_called()


if __name__ == '__main__':
    unittest.main()
