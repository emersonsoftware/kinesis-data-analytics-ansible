#!/usr/bin/python

import library.kda_app as kda_app
from library.kda_app import KinesisDataAnalyticsApp
import mock
from mock import patch
import unittest


class TestKinesisDataAnalyticsApp(unittest.TestCase):

    def setUp(self):
        self.module = mock.MagicMock()
        self.module.check_mode = False
        self.module.exit_json = mock.MagicMock()
        self.module.fail_json = mock.MagicMock()
        self.kda_app = KinesisDataAnalyticsApp(self.module)
        self.kda_app.client = mock.MagicMock()

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


if __name__ == '__main__':
    unittest.main()
