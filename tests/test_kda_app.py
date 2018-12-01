#!/usr/bin/python

import library.kda_app as kda_app
from library.kda_app import KinesisDataAnalyticsApp
import mock
from mock import patch
from botocore.exceptions import ClientError
from ddt import ddt, data, unpack
import unittest


@ddt
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
            'description': 'maDescription',
            'code': 'mycode',
            'inputs': {
                'name_prefix': 'mayBeinMemoRyAppNaMe',
                'parallelism': 1,
                'kinesis': {
                    'type': 'streams',
                    'resource_arn': 'some::kindaa::arn',
                    'role_arn': 'some::kindaa::arn'
                },
                'schema': {
                    'columns': [
                        {
                            'name': 'sensor',
                            'type': 'VARCHAR(1)',
                            'mapping': '$.sensor_id',
                        },
                        {
                            'name': 'temp',
                            'type': 'NUMERIC',
                            'mapping': '$.temp',
                        },
                    ],
                    'format': {
                        'type': 'JSON',
                        'json_mapping_row_path': '$',
                    }
                }

            },
            'outputs': [
                {
                    'name': 'inmemoryOutPutStream',
                    'type': 'streams',
                    'resource_arn': 'some::kindaa::arn',
                    'role_arn': 'some::kindaa::arn',
                    'format_type': 'JSON',
                },
                {
                    'name': 'inmemoryOutPutStream1',
                    'type': 'firehose',
                    'resource_arn': 'some::kindaa1::arn',
                    'role_arn': 'some::kindaa1::arn',
                    'format_type': 'CSV',
                },
                {
                    'name': 'inmemoryOutPutStream2',
                    'type': 'lambda',
                    'resource_arn': 'some::kindaa2::arn',
                    'role_arn': 'some::kindaa2::arn',
                    'format_type': 'JSON',
                },
            ],
            'logs': [
                {
                    'stream_arn': 'some::kindaalog::arn',
                    'role_arn': 'some::kindaalog::arn',
                },
                {
                    'stream_arn': 'some::kindaalog1::arn',
                    'role_arn': 'some::kindaalog1::arn',
                },
            ],
            'starting_position': 'LAST_STOPPED_POINT',
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
            'ApplicationDetail': {
                'ApplicationVersionId': 55,
                'ApplicationCode': 'doYouCare?'

            }
        }
        self.app.client.describe_application = mock.MagicMock(return_value=resp)

        self.app.process_request()

        self.assertEqual(resp, self.app.current_state)
        self.app.client.describe_application.assert_called_once_with(ApplicationName='testifyApp')

    def test_process_request_calls_create_application_when_application_not_found(self):
        self.setup_for_create_application()

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

    def test_create_application_base_parameters_mapped_correctly(self):
        self.setup_for_create_application()

        self.app.process_request()

        self.app.client.create_application.assert_called_once_with(ApplicationName='testifyApp',
                                                                   ApplicationDescription='maDescription',
                                                                   ApplicationCode='mycode', Inputs=mock.ANY,
                                                                   Outputs=mock.ANY, CloudWatchLoggingOptions=mock.ANY)

    @data(('streams', 0, 'JSON'), ('streams', 1, 'CSV'), ('firehose', 0, 'CSV'), ('firehose', 1, 'JSON'))
    @unpack
    def test_create_application_input_parameter_mapped_correctly(self, stream_type, pre_processor, format):
        self.setup_for_create_application()
        self.app.module.params['inputs']['kinesis']['type'] = stream_type
        self.app.module.params['inputs']['schema']['format']['type'] = format
        if pre_processor == 1:
            self.app.module.params['inputs']['pre_processor'] = {
                'resource_arn': 'some::kindaaprepo::arn',
                'role_arn': 'some::kindaapreporole::arn'
            }
        if format == 'CSV':
            self.app.module.params['inputs']['schema']['format']['csv_mapping_row_delimiter'] = '\n'
            self.app.module.params['inputs']['schema']['format']['csv_mapping_column_delimiter'] = ','

        self.app.process_request()

        self.app.client.create_application.assert_called_once_with(ApplicationName=mock.ANY,
                                                                   ApplicationDescription=mock.ANY,
                                                                   ApplicationCode=mock.ANY,
                                                                   Inputs=[self.get_expected_input_configuration()],
                                                                   Outputs=mock.ANY, CloudWatchLoggingOptions=mock.ANY)

    def test_create_application_output_parameter_mapped_correctly(self):
        self.setup_for_create_application()

        self.app.process_request()

        self.app.client.create_application.assert_called_once_with(ApplicationName=mock.ANY,
                                                                   ApplicationDescription=mock.ANY,
                                                                   ApplicationCode=mock.ANY, Inputs=mock.ANY,
                                                                   Outputs=self.get_expected_output_configuration(),
                                                                   CloudWatchLoggingOptions=mock.ANY)

    def test_create_application_log_parameter_mapped_correctly(self):
        self.setup_for_create_application()

        self.app.process_request()

        self.app.client.create_application.assert_called_once_with(ApplicationName=mock.ANY,
                                                                   ApplicationDescription=mock.ANY,
                                                                   ApplicationCode=mock.ANY, Inputs=mock.ANY,
                                                                   Outputs=mock.ANY,
                                                                   CloudWatchLoggingOptions=self.get_expected_log_configuration())

    def test_create_application_log_parameter_not_mapped_when_logs_not_supplied(self):
        self.setup_for_create_application()
        del self.app.module.params['logs']

        self.app.process_request()

        self.app.client.create_application.assert_called_once_with(ApplicationName=mock.ANY,
                                                                   ApplicationDescription=mock.ANY,
                                                                   ApplicationCode=mock.ANY, Inputs=mock.ANY,
                                                                   Outputs=mock.ANY)

    def test_start_application_when_create_application_succeed(self):
        self.setup_for_create_application()

        self.app.process_request()

        self.app.client.start_application.assert_called_once()

    @data('NOW', 'TRIM_HORIZON', 'LAST_STOPPED_POINT')
    def test_start_application_called_with_expected_parameters(self, value):
        self.setup_for_create_application()
        self.app.module.params['starting_position'] = value

        self.app.process_request()

        self.app.client.start_application.assert_called_once_with(ApplicationName='testifyApp',
                                                                  InputConfigurations=self.get_input_start_configuration())

    def test_update_application_gets_called_when_code_changes(self):
        self.setup_for_update_application(app_code='codeontheserver')

        self.app.process_request()

        self.app.client.update_application.assert_called_once_with(ApplicationName='testifyApp',
                                                                   CurrentApplicationVersionId=11,
                                                                   ApplicationUpdate=self.get_expected_app_update_configuration())

    def get_expected_input_configuration(self):
        expected = {
            'NamePrefix': self.app.module.params['inputs']['name_prefix'],
            'InputParallelism': {
                'Count': self.app.module.params['inputs']['parallelism']
            },
            'InputSchema': {
                'RecordFormat': {
                    'RecordFormatType': self.app.module.params['inputs']['schema']['format']['type'],
                    'MappingParameters': {}
                },
                'RecordColumns': [],
            }
        }

        if self.app.module.params['inputs']['kinesis']['type'] == 'streams':
            expected['KinesisStreamsInput'] = {
                'ResourceARN': self.app.module.params['inputs']['kinesis']['resource_arn'],
                'RoleARN': self.app.module.params['inputs']['kinesis']['role_arn'],
            }
        elif self.app.module.params['inputs']['kinesis']['type'] == 'firehose':
            expected['KinesisFirehoseInput'] = {
                'ResourceARN': self.app.module.params['inputs']['kinesis']['resource_arn'],
                'RoleARN': self.app.module.params['inputs']['kinesis']['role_arn'],
            }

        if 'pre_processor' in self.app.module.params['inputs']:
            expected['InputProcessingConfiguration'] = {}
            expected['InputProcessingConfiguration']['InputLambdaProcessor'] = {
                'ResourceARN': self.app.module.params['inputs']['pre_processor']['resource_arn'],
                'RoleARN': self.app.module.params['inputs']['pre_processor']['role_arn'],
            }

        if self.app.module.params['inputs']['schema']['format']['type'] == 'JSON':
            expected['InputSchema']['RecordFormat']['MappingParameters']['JSONMappingParameters'] = {
                'RecordRowPath': self.app.module.params['inputs']['schema']['format']['json_mapping_row_path'],
            }
        elif self.app.module.params['inputs']['schema']['format']['type'] == 'CSV':
            expected['InputSchema']['RecordFormat']['MappingParameters']['CSVMappingParameters'] = {
                'RecordRowDelimiter': self.app.module.params['inputs']['schema']['format']['csv_mapping_row_delimiter'],
                'RecordColumnDelimiter': self.app.module.params['inputs']['schema']['format'][
                    'csv_mapping_column_delimiter'],
            }

        for column in self.app.module.params['inputs']['schema']['columns']:
            expected['InputSchema']['RecordColumns'].append({
                'Mapping': column['mapping'],
                'Name': column['name'],
                'SqlType': column['type'],
            })

        return expected

    def get_expected_output_configuration(self):
        expected = []

        for item in self.app.module.params['outputs']:
            output = {
                'Name': item['name'],
                'DestinationSchema': {
                    'RecordFormatType': item['format_type']
                }
            }
            if item['type'] == 'streams':
                output['KinesisStreamsOutput'] = {
                    'ResourceARN': item['resource_arn'],
                    'RoleARN': item['role_arn'],
                }
            elif item['type'] == 'firehose':
                output['KinesisFirehoseOutput'] = {
                    'ResourceARN': item['resource_arn'],
                    'RoleARN': item['role_arn'],
                }
            elif item['type'] == 'lambda':
                output['LambdaOutput'] = {
                    'ResourceARN': item['resource_arn'],
                    'RoleARN': item['role_arn'],
                }
            expected.append(output)

        return expected

    def get_expected_log_configuration(self):
        expected = []

        for item in self.app.module.params['logs']:
            expected.append({
                'LogStreamARN': item['stream_arn'],
                'RoleARN': item['role_arn'],
            })

        return expected

    def get_expected_app_update_configuration(self):
        expected = {}

        if self.app.module.params['code'] != self.app.current_state['ApplicationDetail']['ApplicationCode']:
            expected['ApplicationCodeUpdate'] = self.app.module.params['code']

        return expected

    def get_input_start_configuration(self):
        expected = []

        item = {
            'Id': '1.1',
            'InputStartingPositionConfiguration': {}
        }

        item['InputStartingPositionConfiguration']['InputStartingPosition'] = self.app.module.params['starting_position']

        expected.append(item)

        return expected

    def setup_for_create_application(self):
        resource_not_found = {'Error': {'Code': 'ResourceNotFoundException'}}
        self.app.client.describe_application = mock.MagicMock(side_effect=ClientError(resource_not_found, ''))
        self.app.client.create_application = mock.MagicMock()
        self.app.client.start_application = mock.MagicMock()

    def setup_for_update_application(self, app_code=''):
        mock_describe_application_response = {
            'ApplicationDetail': {
                'ApplicationVersionId': 11,

            }
        }

        if app_code != '':
            mock_describe_application_response['ApplicationDetail']['ApplicationCode'] = app_code

        self.app.client.describe_application = mock.MagicMock(return_value=mock_describe_application_response)


if __name__ == '__main__':
    unittest.main()
