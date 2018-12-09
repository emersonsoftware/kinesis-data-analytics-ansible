#!/usr/bin/python

__version__ = '${version}'

try:
    import boto3
    import boto
    from botocore.exceptions import BotoCoreError

    HAS_BOTO3 = True
except ImportError:
    HAS_BOTO3 = False


class KinesisDataAnalyticsApp:
    current_state = None

    def __init__(self, module):
        self.module = module
        if not HAS_BOTO3:
            self.module.fail_json(msg='boto and boto3 are required for this module')
        self.client = boto3.client('kinesisanalytics')

    def process_request(self):
        status = self.get_current_state()
        if status is 'AppNotFound':
            self.create_new_application()
            self.start_application()
        elif status is 'AppFound':
            if self.is_app_updatable_state_changed():
                self.update_application()
            self.patch_application()

    def start_application(self):
        self.client.start_application(ApplicationName=self.module.params['name'],
                                      InputConfigurations=self.get_input_start_configuration())

    def create_new_application(self):
        if 'logs' in self.module.params:
            self.client.create_application(ApplicationName=self.module.params['name'],
                                           ApplicationDescription=self.module.params['description'],
                                           Inputs=[self.get_input_configuration()],
                                           Outputs=self.get_output_configuration(),
                                           CloudWatchLoggingOptions=self.get_log_configuration(),
                                           ApplicationCode=self.module.params['code'])
        else:
            self.client.create_application(ApplicationName=self.module.params['name'],
                                           ApplicationDescription=self.module.params['description'],
                                           Inputs=self.get_input_configuration(),
                                           Outputs=self.get_output_configuration(),
                                           ApplicationCode=self.module.params['code'])

    def update_application(self):
        self.client.update_application(ApplicationName=self.module.params['name'],
                                       CurrentApplicationVersionId=self.current_state['ApplicationDetail'][
                                           'ApplicationVersionId'],
                                       ApplicationUpdate=self.get_app_update_configuration())

    def patch_application(self):

        self.patch_outputs()

        self.patch_logs()

    def patch_inputs(self):
        for item in self.module.params['inputs']:
            matched_describe_inputs = [i for i in self.current_state['ApplicationDetail']['InputDescriptions'] if
                                       i['NamePrefix'] == item['name_prefix']]
            if len(matched_describe_inputs) <= 0:
                self.client.add_application_input(ApplicationName=self.module.params['name'],
                                                  CurrentApplicationVersionId=self.current_state['ApplicationDetail'][
                                                      'ApplicationVersionId'],
                                                  Input=self.get_single_input_configuration(item))

        for item in self.current_state['ApplicationDetail']['InputDescriptions']:
            matched_desired_inputs = [i for i in self.module.params['inputs'] if
                                      i['name_prefix'] == item['NamePrefix']]
            if len(matched_desired_inputs) <= 0:
                self.client.delete_application_input_processing_configuration(
                    ApplicationName=self.module.params['name'],
                    CurrentApplicationVersionId=self.current_state['ApplicationDetail'][
                        'ApplicationVersionId'],
                    InputId=item['InputId'])

    def patch_outputs(self):
        for item in self.module.params['outputs']:
            matched_describe_outputs = [i for i in self.current_state['ApplicationDetail']['OutputDescriptions'] if
                                        i['Name'] == item['name']]
            if len(matched_describe_outputs) <= 0:
                self.client.add_application_output(ApplicationName=self.module.params['name'],
                                                   CurrentApplicationVersionId=self.current_state['ApplicationDetail'][
                                                       'ApplicationVersionId'],
                                                   Output=self.get_single_output_configuration(item))

        for item in self.current_state['ApplicationDetail']['OutputDescriptions']:
            matched_desired_outputs = [i for i in self.module.params['outputs'] if
                                      i['name'] == item['Name']]
            if len(matched_desired_outputs) <= 0:
                self.client.delete_application_output(
                    ApplicationName=self.module.params['name'],
                    CurrentApplicationVersionId=self.current_state['ApplicationDetail'][
                        'ApplicationVersionId'],
                    OutputId=item['OutputId'])

    def patch_logs(self):
        if 'logs' in self.module.params:
            for item in self.module.params['logs']:
                if 'CloudWatchLoggingOptionDescriptions' in self.current_state['ApplicationDetail']:
                    matched_describe_logs = [i for i in self.current_state['ApplicationDetail']['CloudWatchLoggingOptionDescriptions'] if
                                                i['LogStreamARN'] == item['stream_arn']]
                    if len(matched_describe_logs) <= 0:
                        self.client.add_application_cloud_watch_logging_option(ApplicationName=self.module.params['name'],
                                                           CurrentApplicationVersionId=self.current_state['ApplicationDetail'][
                                                               'ApplicationVersionId'],
                                                                               CloudWatchLoggingOption={
                                                               'LogStreamARN': item['stream_arn'],
                                                               'RoleARN': item['role_arn']
                                                           })
                else:
                    self.client.add_application_cloud_watch_logging_option(ApplicationName=self.module.params['name'],
                                                                           CurrentApplicationVersionId=self.current_state['ApplicationDetail'][
                                                                               'ApplicationVersionId'],
                                                                           CloudWatchLoggingOption={
                                                                               'LogStreamARN': item['stream_arn'],
                                                                               'RoleARN': item['role_arn']
                                                                           })

        if 'CloudWatchLoggingOptionDescriptions' in self.current_state['ApplicationDetail']:
            for item in self.current_state['ApplicationDetail']['CloudWatchLoggingOptionDescriptions']:
                if 'logs' in self.module.params:
                    matched_desired_logs = [i for i in self.module.params['logs'] if
                                               i['stream_arn'] == item['LogStreamARN']]
                    if len(matched_desired_logs) <= 0:
                        self.client.delete_application_cloud_watch_logging_option(ApplicationName=self.module.params['name'],
                                                                                  CurrentApplicationVersionId=self.current_state['ApplicationDetail'][
                                                                                      'ApplicationVersionId'],
                                                                                  CloudWatchLoggingOptionId=item['CloudWatchLoggingOptionId'])

                else:
                    self.client.delete_application_cloud_watch_logging_option(ApplicationName=self.module.params['name'],
                                                                           CurrentApplicationVersionId=self.current_state['ApplicationDetail'][
                                                                               'ApplicationVersionId'],
                                                                              CloudWatchLoggingOptionId=item['CloudWatchLoggingOptionId'])

    def get_current_state(self):
        from botocore.exceptions import ClientError
        try:
            self.current_state = self.client.describe_application(ApplicationName=self.module.params['name'])
            return 'AppFound'
        except ClientError as err:
            if err.response['Error']['Code'] == "ResourceNotFoundException":
                return 'AppNotFound'
            else:
                return 'Unknown'

    def get_input_configuration(self):
        inputs = []
        for item in self.module.params['inputs']:
            inputs.append(self.get_single_input_configuration(item))

        return inputs

    def get_single_input_configuration(self, item):
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
        return input_item

    def get_output_configuration(self):
        outputs = []

        for item in self.module.params['outputs']:
            outputs.append(self.get_single_output_configuration(item))

        return outputs

    def get_single_output_configuration(self, item):
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

        return output

    def get_log_configuration(self):
        logs = []
        for item in self.module.params['logs']:
            logs.append({
                'LogStreamARN': item['stream_arn'],
                'RoleARN': item['role_arn'],
            })

        return logs

    def get_input_start_configuration(self):
        input_config = []

        item = {
            'Id': '1.1',
            'InputStartingPositionConfiguration': {}
        }

        item['InputStartingPositionConfiguration']['InputStartingPosition'] = self.module.params['starting_position']

        input_config.append(item)

        return input_config

    def get_app_update_configuration(self):
        update_config = {}

        if self.module.params['code'] != self.current_state['ApplicationDetail']['ApplicationCode']:
            update_config['ApplicationCodeUpdate'] = self.module.params['code']

        if self.is_input_configuration_change():
            update_config['InputUpdates'] = self.get_input_update_configuration()

        if self.is_output_configuration_change():
            update_config['OutputUpdates'] = self.get_output_update_configuration()

        if self.is_log_configuration_changed():
            update_config['CloudWatchLoggingOptionUpdates'] = self.get_log_update_configuration()

        return update_config

    def is_app_updatable_state_changed(self):
        return self.module.params['code'] != self.current_state['ApplicationDetail'][
            'ApplicationCode'] or self.is_input_configuration_change() or self.is_output_configuration_change() or self.is_log_configuration_changed()

    def is_output_configuration_change(self):
        for output in self.module.params['outputs']:
            matched_describe_outputs = [i for i in self.current_state['ApplicationDetail']['OutputDescriptions'] if
                                        i['Name'] == output['name']]
            if len(matched_describe_outputs) != 1:
                continue
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

    def is_input_configuration_change(self):
        for input in self.module.params['inputs']:
            matched_describe_inputs = [i for i in self.current_state['ApplicationDetail']['InputDescriptions'] if
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

    def is_log_configuration_changed(self):
        if 'logs' not in self.module.params:
            return False

        for log in self.module.params['logs']:
            matched_describe_logs = [i for i in
                                     self.current_state['ApplicationDetail']['CloudWatchLoggingOptionDescriptions'] if
                                     i['LogStreamARN'] == log['stream_arn']]
            if len(matched_describe_logs) != 1:
                continue
            describe_log = matched_describe_logs[0]

            if log['role_arn'] != describe_log['RoleARN']:
                return True

        return False

    def get_input_update_configuration(self):
        expected = []
        for item in self.module.params['inputs']:
            describe_inputs = self.current_state['ApplicationDetail']['InputDescriptions']

            input_item = {
                'InputId': describe_inputs[0]['InputId'],
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

    def get_output_update_configuration(self):
        expected = []

        for item in self.module.params['outputs']:
            matched_describe_outputs = [i for i in self.current_state['ApplicationDetail']['OutputDescriptions'] if
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

    def get_log_update_configuration(self):
        expected = []

        for item in self.module.params['logs']:
            matched_describe_logs = [i for i in
                                     self.current_state['ApplicationDetail']['CloudWatchLoggingOptionDescriptions'] if
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


def main():
    print('who cares')


if __name__ == '__main__':
    main()
