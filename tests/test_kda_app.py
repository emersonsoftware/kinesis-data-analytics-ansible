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
            'inputs': [{
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

            }],
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
                'ApplicationCode': 'doYouCare?',
                'InputDescriptions': self.get_expected_describe_input_configuration(),
                'OutputDescriptions': self.get_expected_describe_output_configuration(),
                'CloudWatchLoggingOptionDescriptions': self.get_expected_describe_logs_configuration(),

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
        for input in self.app.module.params['inputs']:
            input['kinesis']['type'] = stream_type
            input['schema']['format']['type'] = format
            if pre_processor == 1:
                input['pre_processor'] = {
                    'resource_arn': 'some::kindaaprepo::arn',
                    'role_arn': 'some::kindaapreporole::arn'
                }
            if format == 'CSV':
                input['schema']['format']['csv_mapping_row_delimiter'] = '\n'
                input['schema']['format']['csv_mapping_column_delimiter'] = ','

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
        self.setup_for_update_application(app_code='codeontheserver',
                                          inputs=self.get_expected_describe_input_configuration(),
                                          outputs=self.get_expected_describe_output_configuration(),
                                          logs=self.get_expected_describe_logs_configuration())

        self.app.process_request()

        self.app.client.update_application.assert_called_once_with(ApplicationName='testifyApp',
                                                                   CurrentApplicationVersionId=11,
                                                                   ApplicationUpdate=self.get_expected_app_update_configuration())

    @data(
        (('streams', 'firehose'), (0, 0), ('JSON', 'JSON'), 0),
        (('streams', 'streams'), (1, 0), ('CSV', 'CSV'), 0),
        (('firehose', 'firehose'), (0, 0), ('CSV', 'JSON'), 0),
        (('firehose', 'streams'), (1, 0), ('JSON', 'CSV'), 0),
        (('streams', 'streams'), (0, 0), ('CSV', 'CSV'), 1),
    )
    @unpack
    def test_update_application_gets_called_when_input_changes(self, stream_type, pre_processor, format, schema_change):
        new_index = 0
        old_index = 1
        for input in self.app.module.params['inputs']:
            input['kinesis']['type'] = stream_type[new_index]
            input['schema']['format']['type'] = format[new_index]
            if pre_processor[new_index] == 1:
                input['pre_processor'] = {
                    'resource_arn': 'some::kindaaprepo::arn',
                    'role_arn': 'some::kindaapreporole::arn'
                }
            if format[new_index] == 'CSV':
                input['schema']['format']['csv_mapping_row_delimiter'] = '\n'
                input['schema']['format']['csv_mapping_column_delimiter'] = ','

        describe_inputs = self.get_expected_describe_input_configuration()
        for describe_input in describe_inputs:
            if stream_type[old_index] != stream_type[new_index]:
                if stream_type[new_index] == 'streams':
                    del describe_input['KinesisStreamsInputDescription']
                    describe_input['KinesisFirehoseInputDescription'] = {
                        'ResourceARN': 'oldArnResorce',
                        'RoleARN': 'oldArnRole',
                    }
                elif stream_type[new_index] == 'firehose':
                    del describe_input['KinesisFirehoseInputDescription']
                    describe_input['KinesisStreamsInputDescription'] = {
                        'ResourceARN': 'oldArnResorce',
                        'RoleARN': 'oldArnRole',
                    }
            if pre_processor[old_index] != pre_processor[new_index]:
                if pre_processor[new_index] == 1:
                    del describe_input['InputProcessingConfigurationDescription']
                else:
                    describe_input['InputProcessingConfigurationDescription'] = {}
                    describe_input['InputProcessingConfigurationDescription']['InputLambdaProcessorDescription'] = {
                        'ResourceARN': 'oldIpcResourceArn',
                        'RoleARN': 'oldIpcRoleArn',
                    }
            if format[old_index] != format[new_index]:
                describe_input['InputSchema']['RecordFormat']['RecordFormatType'] = format[old_index]
            if schema_change == 1:
                del describe_input['InputSchema']['RecordColumns'][0]

        self.setup_for_update_application(inputs=describe_inputs, outputs=self.get_expected_describe_output_configuration(), logs=self.get_expected_describe_logs_configuration())

        self.app.process_request()

        self.app.client.update_application.assert_called_once_with(ApplicationName='testifyApp',
                                                                   CurrentApplicationVersionId=11,
                                                                   ApplicationUpdate=self.get_expected_app_update_configuration())

    @data(
        (1, 0, 0, 0),
        (0, 1, 0, 0),
        (0, 0, 1, 0),
        (0, 0, 0, 1),
        (0, 1, 0, 1),
        (1, 0, 0, 1),
        (1, 1, 0, 1),
        (1, 1, 1, 1),
    )
    @unpack
    def test_update_application_gets_called_when_output_changes(self, changed_resource, changed_role,
                                                                changed_format_type, changed_output_type):
        describe_outputs = self.get_expected_describe_output_configuration()

        for output in describe_outputs:
            if changed_resource == 1:
                if 'KinesisStreamsOutputDescription' in output:
                    output['KinesisStreamsOutputDescription']['ResourceARN'] = 'different:arn'
                if 'KinesisFirehoseOutputDescription' in output:
                    output['KinesisFirehoseOutputDescription']['ResourceARN'] = 'different:arn'
                if 'LambdaOutputDescription' in output:
                    output['LambdaOutputDescription']['ResourceARN'] = 'different:arn'
            if changed_role == 1:
                if 'KinesisStreamsOutputDescription' in output:
                    output['KinesisStreamsOutputDescription']['RoleARN'] = 'different:arn'
                if 'KinesisFirehoseOutputDescription' in output:
                    output['KinesisFirehoseOutputDescription']['RoleARN'] = 'different:arn'
                if 'LambdaOutputDescription' in output:
                    output['LambdaOutputDescription']['RoleARN'] = 'different:arn'
            if changed_format_type == 1:
                if output['DestinationSchema']['RecordFormatType'] == 'JSON':
                    output['DestinationSchema']['RecordFormatType'] = 'CSV'
                elif output['DestinationSchema']['RecordFormatType'] == 'CSV':
                    output['DestinationSchema']['RecordFormatType'] = 'JSON'
            if changed_output_type == 1:
                if 'KinesisStreamsOutputDescription' in output:
                    output['KinesisFirehoseOutputDescription'] = output.pop('KinesisStreamsOutputDescription')
                if 'KinesisFirehoseOutputDescription' in output:
                    output['KinesisStreamsOutputDescription'] = output.pop('KinesisFirehoseOutputDescription')
                if 'LambdaOutputDescription' in output:
                    output['KinesisFirehoseOutputDescription'] = output.pop('LambdaOutputDescription')

        self.setup_for_update_application(inputs=self.get_expected_describe_input_configuration(), outputs=describe_outputs, logs=self.get_expected_describe_logs_configuration())

        self.app.process_request()

        self.app.client.update_application.assert_called_once_with(ApplicationName='testifyApp',
                                                                   CurrentApplicationVersionId=11,
                                                                   ApplicationUpdate=self.get_expected_app_update_configuration())

    def test_update_application_gets_called_when_logs_changes(self):
        describe_logs = self.get_expected_describe_logs_configuration()

        for log in describe_logs:
            log['RoleARN'] = 'diffrent::arn'

        self.setup_for_update_application(inputs=self.get_expected_describe_input_configuration(), outputs=self.get_expected_describe_output_configuration(), logs=describe_logs)

        self.app.process_request()

        self.app.client.update_application.assert_called_once_with(ApplicationName='testifyApp',
                                                                   CurrentApplicationVersionId=11,
                                                                   ApplicationUpdate=self.get_expected_app_update_configuration())

    def get_expected_input_configuration(self):
        expected = []
        for item in self.app.module.params['inputs']:
            input_item = {
                'NamePrefix': item['name_prefix'],
                'InputParallelism': {
                    'Count': item['parallelism']
                },
                'InputSchema': {
                    'RecordFormat': {
                        'RecordFormatType': item['schema']['format']['type'],
                        'MappingParameters': {}
                    },
                    'RecordColumns': [],
                }
            }

            if item['kinesis']['type'] == 'streams':
                input_item['KinesisStreamsInput'] = {
                    'ResourceARN': item['kinesis']['resource_arn'],
                    'RoleARN': item['kinesis']['role_arn'],
                }
            elif item['kinesis']['type'] == 'firehose':
                input_item['KinesisFirehoseInput'] = {
                    'ResourceARN': item['kinesis']['resource_arn'],
                    'RoleARN': item['kinesis']['role_arn'],
                }

            if 'pre_processor' in item:
                input_item['InputProcessingConfiguration'] = {}
                input_item['InputProcessingConfiguration']['InputLambdaProcessor'] = {
                    'ResourceARN': item['pre_processor']['resource_arn'],
                    'RoleARN': item['pre_processor']['role_arn'],
                }

            if item['schema']['format']['type'] == 'JSON':
                input_item['InputSchema']['RecordFormat']['MappingParameters']['JSONMappingParameters'] = {
                    'RecordRowPath': item['schema']['format']['json_mapping_row_path'],
                }
            elif item['schema']['format']['type'] == 'CSV':
                input_item['InputSchema']['RecordFormat']['MappingParameters']['CSVMappingParameters'] = {
                    'RecordRowDelimiter': item['schema']['format']['csv_mapping_row_delimiter'],
                    'RecordColumnDelimiter': item['schema']['format'][
                        'csv_mapping_column_delimiter'],
                }

            for column in item['schema']['columns']:
                input_item['InputSchema']['RecordColumns'].append({
                    'Mapping': column['mapping'],
                    'Name': column['name'],
                    'SqlType': column['type'],
                })
            expected.append(input_item)

        return expected

    def get_expected_input_update_configuration(self):
        expected = []
        for item in self.app.module.params['inputs']:
            matched_describe_inputs = [i for i in self.app.current_state['ApplicationDetail']['InputDescriptions'] if
                                       i['NamePrefix'] == item['name_prefix']]
            if len(matched_describe_inputs) != 1:
                continue

            input_item = {
                'InputId': matched_describe_inputs[0]['InputId'],
                'NamePrefixUpdate': item['name_prefix'],
                'InputParallelismUpdate': {
                    'CountUpdate': item['parallelism']
                },
                'InputSchemaUpdate': {
                    'RecordFormatUpdate': {
                        'RecordFormatType': item['schema']['format']['type'],
                        'MappingParameters': {}
                    },
                    'RecordColumnUpdates': [],
                }
            }

            if item['kinesis']['type'] == 'streams':
                input_item['KinesisStreamsInputUpdate'] = {
                    'ResourceARNUpdate': item['kinesis']['resource_arn'],
                    'RoleARNUpdate': item['kinesis']['role_arn'],
                }
            elif item['kinesis']['type'] == 'firehose':
                input_item['KinesisFirehoseInputUpdate'] = {
                    'ResourceARNUpdate': item['kinesis']['resource_arn'],
                    'RoleARNUpdate': item['kinesis']['role_arn'],
                }

            if 'pre_processor' in item:
                input_item['InputProcessingConfigurationUpdate'] = {}
                input_item['InputProcessingConfigurationUpdate']['InputLambdaProcessorUpdate'] = {
                    'ResourceARNUpdate': item['pre_processor']['resource_arn'],
                    'RoleARNUpdate': item['pre_processor']['role_arn'],
                }

            if item['schema']['format']['type'] == 'JSON':
                input_item['InputSchemaUpdate']['RecordFormatUpdate']['MappingParameters']['JSONMappingParameters'] = {
                    'RecordRowPath': item['schema']['format']['json_mapping_row_path'],
                }
            elif item['schema']['format']['type'] == 'CSV':
                input_item['InputSchemaUpdate']['RecordFormatUpdate']['MappingParameters']['CSVMappingParameters'] = {
                    'RecordRowDelimiter': item['schema']['format']['csv_mapping_row_delimiter'],
                    'RecordColumnDelimiter': item['schema']['format'][
                        'csv_mapping_column_delimiter'],
                }

            for column in item['schema']['columns']:
                input_item['InputSchemaUpdate']['RecordColumnUpdates'].append({
                    'Mapping': column['mapping'],
                    'Name': column['name'],
                    'SqlType': column['type'],
                })
            expected.append(input_item)

        return expected

    def get_expected_output_update_configuration(self):
        expected = []

        for item in self.app.module.params['outputs']:
            matched_describe_outputs = [i for i in self.app.current_state['ApplicationDetail']['OutputDescriptions'] if
                                        i['Name'] == item['name']]

            if len(matched_describe_outputs) != 1:
                continue

            output = {
                'OutputId': matched_describe_outputs[0]['OutputId'],
                'NameUpdate': item['name'],
                'DestinationSchemaUpdate': {
                    'RecordFormatType': item['format_type']
                }
            }
            if item['type'] == 'streams':
                output['KinesisStreamsOutputUpdate'] = {
                    'ResourceARNUpdate': item['resource_arn'],
                    'RoleARNUpdate': item['role_arn'],
                }
            elif item['type'] == 'firehose':
                output['KinesisFirehoseOutputUpdate'] = {
                    'ResourceARNUpdate': item['resource_arn'],
                    'RoleARNUpdate': item['role_arn'],
                }
            elif item['type'] == 'lambda':
                output['LambdaOutputUpdate'] = {
                    'ResourceARNUpdate': item['resource_arn'],
                    'RoleARNUpdate': item['role_arn'],
                }
            expected.append(output)

        return expected

    def get_expected_log_update_configuration(self):
        expected = []

        for item in self.app.module.params['logs']:
            matched_describe_logs = [i for i in self.app.current_state['ApplicationDetail']['CloudWatchLoggingOptionDescriptions'] if
                                        i['LogStreamARN'] == item['stream_arn']]

            if len(matched_describe_logs) != 1:
                continue

            log = {
                'CloudWatchLoggingOptionId': matched_describe_logs[0]['CloudWatchLoggingOptionId'],
                'LogStreamARNUpdate': item['stream_arn'],
                'RoleARNUpdate': item['role_arn']
            }
            expected.append(log)

        return expected

    def get_expected_describe_output_configuration(self):
        expected = []
        outputId = 0
        for item in self.app.module.params['outputs']:
            outputId += 1
            output = {
                'OutputId': str(outputId),
                'Name': item['name'],
                'DestinationSchema': {
                    'RecordFormatType': item['format_type']
                }
            }
            if item['type'] == 'streams':
                output['KinesisStreamsOutputDescription'] = {
                    'ResourceARN': item['resource_arn'],
                    'RoleARN': item['role_arn'],
                }
            elif item['type'] == 'firehose':
                output['KinesisFirehoseOutputDescription'] = {
                    'ResourceARN': item['resource_arn'],
                    'RoleARN': item['role_arn'],
                }
            elif item['type'] == 'lambda':
                output['LambdaOutputDescription'] = {
                    'ResourceARN': item['resource_arn'],
                    'RoleARN': item['role_arn'],
                }
            expected.append(output)

        return expected

    def get_expected_describe_logs_configuration(self):
        expected = []
        logId = 0
        for item in self.app.module.params['logs']:
            logId += 1
            log = {
                'CloudWatchLoggingOptionId': str(logId),
                'LogStreamARN': item['stream_arn'],
                'RoleARN': item['role_arn']
            }
            expected.append(log)

        return expected

    def get_expected_describe_input_configuration(self):
        expected = []
        inputId = 0
        for item in self.app.module.params['inputs']:
            inputId += 1
            input_item = {
                'InputId': str(inputId),
                'NamePrefix': item['name_prefix'],
                'InputParallelism': {
                    'Count': item['parallelism']
                },
                'InputSchema': {
                    'RecordFormat': {
                        'RecordFormatType': item['schema']['format']['type'],
                        'MappingParameters': {}
                    },
                    'RecordColumns': [],
                }
            }

            if item['kinesis']['type'] == 'streams':
                input_item['KinesisStreamsInputDescription'] = {
                    'ResourceARN': item['kinesis']['resource_arn'],
                    'RoleARN': item['kinesis']['role_arn'],
                }
            elif item['kinesis']['type'] == 'firehose':
                input_item['KinesisFirehoseInputDescription'] = {
                    'ResourceARN': item['kinesis']['resource_arn'],
                    'RoleARN': item['kinesis']['role_arn'],
                }

            if 'pre_processor' in item:
                input_item['InputProcessingConfigurationDescription'] = {}
                input_item['InputProcessingConfigurationDescription']['InputLambdaProcessorDescription'] = {
                    'ResourceARN': item['pre_processor']['resource_arn'],
                    'RoleARN': item['pre_processor']['role_arn'],
                }

            if item['schema']['format']['type'] == 'JSON':
                input_item['InputSchema']['RecordFormat']['MappingParameters']['JSONMappingParameters'] = {
                    'RecordRowPath': item['schema']['format']['json_mapping_row_path'],
                }
            elif item['schema']['format']['type'] == 'CSV':
                input_item['InputSchema']['RecordFormat']['MappingParameters']['CSVMappingParameters'] = {
                    'RecordRowDelimiter': item['schema']['format']['csv_mapping_row_delimiter'],
                    'RecordColumnDelimiter': item['schema']['format'][
                        'csv_mapping_column_delimiter'],
                }

            for column in item['schema']['columns']:
                input_item['InputSchema']['RecordColumns'].append({
                    'Mapping': column['mapping'],
                    'Name': column['name'],
                    'SqlType': column['type'],
                })
            expected.append(input_item)

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

        if self.is_input_configuration_changed():
            expected['InputUpdates'] = self.get_expected_input_update_configuration()

        if self.is_output_configuration_changed():
            expected['OutputUpdates'] = self.get_expected_output_update_configuration()

        if self.is_log_configuration_changed():
            expected['CloudWatchLoggingOptionUpdates'] = self.get_expected_log_update_configuration()

        return expected

    def is_output_configuration_changed(self):
        if len(self.app.module.params['outputs']) != len(
                self.app.current_state['ApplicationDetail']['OutputDescriptions']):
            return True

        for output in self.app.module.params['outputs']:
            matched_describe_outputs = [i for i in self.app.current_state['ApplicationDetail']['OutputDescriptions'] if
                                       i['Name'] == output['name']]
            if len(matched_describe_outputs) != 1:
                return True
            describe_output = matched_describe_outputs[0]

            if output['type'] == 'streams':
                if 'KinesisStreamsOutputDescription' not in describe_output:
                    return True
                if output['resource_arn'] != describe_output['KinesisStreamsOutputDescription']['ResourceARN']:
                    return True
                if output['role_arn'] != describe_output['KinesisStreamsOutputDescription']['RoleARN']:
                    return True

            if output['type'] == 'firehose':
                if 'KinesisFirehoseOutputDescription' not in describe_output:
                    return True
                if output['resource_arn'] != describe_output['KinesisFirehoseOutputDescription']['ResourceARN']:
                    return True
                if output['role_arn'] != describe_output['KinesisFirehoseOutputDescription']['RoleARN']:
                    return True

            if output['type'] == 'lambda':
                if 'LambdaOutputDescription' not in describe_output:
                    return True
                if output['resource_arn'] != describe_output['LambdaOutputDescription']['ResourceARN']:
                    return True
                if output['role_arn'] != describe_output['LambdaOutputDescription']['RoleARN']:
                    return True

            if output['format_type'] != describe_output['DestinationSchema']['RecordFormatType']:
                return True

        return False

    def is_log_configuration_changed(self):
        if len(self.app.module.params['logs']) != len(
                self.app.current_state['ApplicationDetail']['CloudWatchLoggingOptionDescriptions']):
            return True

        for log in self.app.module.params['logs']:
            matched_describe_logs = [i for i in self.app.current_state['ApplicationDetail']['CloudWatchLoggingOptionDescriptions'] if
                                        i['LogStreamARN'] == log['stream_arn']]
            if len(matched_describe_logs) != 1:
                return True
            describe_log = matched_describe_logs[0]

            if log['role_arn'] != describe_log['RoleARN']:
                return True

        return False

    def is_input_configuration_changed(self):
        if len(self.app.module.params['inputs']) != len(
                self.app.current_state['ApplicationDetail']['InputDescriptions']):
            return True

        for input in self.app.module.params['inputs']:
            matched_describe_inputs = [i for i in self.app.current_state['ApplicationDetail']['InputDescriptions'] if
                                       i['NamePrefix'] == input['name_prefix']]
            if len(matched_describe_inputs) != 1:
                return True
            describe_input = matched_describe_inputs[0]

            if input['schema']['format']['type'] != describe_input['InputSchema']['RecordFormat']['RecordFormatType']:
                return True

            if input['schema']['format']['type'] == 'JSON':
                if input['schema']['format']['json_mapping_row_path'] != \
                        describe_input['InputSchema']['RecordFormat']['MappingParameters']['JSONMappingParameters'][
                            'RecordRowPath']:
                    return True

            if input['schema']['format']['type'] == 'CSV':
                if input['schema']['format']['csv_mapping_row_delimiter'] != \
                        describe_input['InputSchema']['RecordFormat']['MappingParameters']['CSVMappingParameters'][
                            'RecordRowDelimiter']:
                    return True
                if input['schema']['format']['csv_mapping_column_delimiter'] != \
                        describe_input['InputSchema']['RecordFormat']['MappingParameters']['CSVMappingParameters'][
                            'RecordColumnDelimiter']:
                    return True

            if len(input['schema']['columns']) != len(describe_input['InputSchema']['RecordColumns']):
                return True

            for col in input['schema']['columns']:
                matched_describe_cols = [i for i in describe_input['InputSchema']['RecordColumns'] if
                                         i['Name'] == col['name']]
                if len(matched_describe_cols) != 1:
                    return True
                describe_col = matched_describe_cols[0]
                if describe_col['SqlType'] != col['type'] or describe_col['Mapping'] != col['mapping']:
                    return True

            if input['parallelism'] != describe_input['InputParallelism']['Count']:
                return True

            if input['kinesis']['type'] == 'streams':
                if 'KinesisStreamsInputDescription' in describe_input:
                    if input['kinesis']['resource_arn'] != describe_input['KinesisStreamsInputDescription'][
                        'ResourceARN']:
                        return True
                    if input['kinesis']['role_arn'] != describe_input['KinesisStreamsInputDescription']['RoleARN']:
                        return True

            if input['kinesis']['type'] == 'firehose':
                if 'KinesisFirehoseInputDescription' in describe_input:
                    if input['kinesis']['resource_arn'] != describe_input['KinesisFirehoseInputDescription'][
                        'ResourceARN']:
                        return True
                    if input['kinesis']['role_arn'] != describe_input['KinesisFirehoseInputDescription']['RoleARN']:
                        return True

            if 'pre_processor' in input:
                if 'InputProcessingConfigurationDescription' not in describe_input:
                    return True
                if input['pre_processor']['resource_arn'] != \
                        describe_input['InputProcessingConfigurationDescription']['InputLambdaProcessorDescription'][
                            'ResourceARN']:
                    return True
                if input['pre_processor']['role_arn'] != \
                        describe_input['InputProcessingConfigurationDescription']['InputLambdaProcessorDescription'][
                            'RoleARN']:
                    return True

        return False

    def get_input_start_configuration(self):
        expected = []

        item = {
            'Id': '1.1',
            'InputStartingPositionConfiguration': {}
        }

        item['InputStartingPositionConfiguration']['InputStartingPosition'] = self.app.module.params[
            'starting_position']

        expected.append(item)

        return expected

    def setup_for_create_application(self):
        resource_not_found = {'Error': {'Code': 'ResourceNotFoundException'}}
        self.app.client.describe_application = mock.MagicMock(side_effect=ClientError(resource_not_found, ''))
        self.app.client.create_application = mock.MagicMock()
        self.app.client.start_application = mock.MagicMock()

    def setup_for_update_application(self, app_code='', inputs=None, outputs=None, logs=None):
        mock_describe_application_response = {
            'ApplicationDetail': {
                'ApplicationVersionId': 11,
            }
        }

        if app_code != '':
            mock_describe_application_response['ApplicationDetail']['ApplicationCode'] = app_code
        else:
            mock_describe_application_response['ApplicationDetail']['ApplicationCode'] = 'testifyApp'

        if inputs is not None:
            mock_describe_application_response['ApplicationDetail']['InputDescriptions'] = inputs

        if outputs is not None:
            mock_describe_application_response['ApplicationDetail']['OutputDescriptions'] = outputs

        if logs is not None:
            mock_describe_application_response['ApplicationDetail']['CloudWatchLoggingOptionDescriptions'] = logs

        self.app.client.describe_application = mock.MagicMock(return_value=mock_describe_application_response)


if __name__ == '__main__':
    unittest.main()
