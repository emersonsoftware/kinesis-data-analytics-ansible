#!/usr/bin/python

__version__ = '${version}'

import time

try:
    import boto3
    import boto
    from botocore.exceptions import BotoCoreError

    HAS_BOTO3 = True
except ImportError:
    HAS_BOTO3 = False


# BJF: Top-level thoughts:
#      1: There is typically a "flower-box" style comment at the top where you claim authorship and document your license (needed for open sourcing)
#      2: It is customary to document your module's usage.  See the APIGW modules for examples.
#         Well-formed docs of this style can be used to generate documentation
#      3: Error handling is insufficient.  Basically, any failure path should cause a well-behaved
#         invocation of fail_json with an appropriate error message.  As-is, exceptions will bubble straight
#         to the user and crash the run.
#      4: There are a lot of happy path assumptions about how well-formed inputs and outputs will be.
#         For things not enforced by the module argument spec and especially for boto output, I would
#         strongly advise switching from []-based key lookup to using .get() with valid default arguments
#      5: It is idiomatic for your module when things are happy to return an object that shows the state of
#         the thing you just created/changed.  This is very useful when debugging.
class KinesisDataAnalyticsApp:
    current_state = None
    changed = False

    def __init__(self, module):
        self.module = module
        if not HAS_BOTO3:
            self.module.fail_json(msg='boto and boto3 are required for this module')
        self.client = boto3.client('kinesisanalytics')

    @staticmethod
    def _define_module_argument_spec():
        # BJF: This would be a bit more complete if the dictionary more aggressively enforced the full schema.
        #      As it stands, there are likely a number of corner cases that can/will arise due to unexpected
        #      permutations of invalid input
        return dict(name=dict(required=True),
                    description=dict(required=True),
                    code=dict(required=True),
                    inputs=dict(required=True, type='list'),
                    outputs=dict(required=True, type='list'),
                    logs=dict(required=False, type='list'),
                    starting_position=dict(default='LAST_STOPPED_POINT',
                                           choices=['NOW', 'TRIM_HORIZON', 'LAST_STOPPED_POINT']),
                    )

    def process_request(self):
        status = self.get_current_state()
        if status is 'AppNotFound':
            self.create_new_application()
            self.changed = True
        elif status is 'AppFound':
            if self.is_app_updatable_state_changed():
                self.update_application()
                self.changed = True
            self.patch_application()

        self.module.exit_json(changed=self.changed, kda_app=self.get_final_state())

    def start_application(self):
        self.client.start_application(ApplicationName=self.module.params['name'],
                                      InputConfigurations=self.get_input_start_configuration())

    def create_new_application(self):
        args = {'ApplicationName': self.module.params['name'],
                'ApplicationDescription': self.module.params['description'],
                'Inputs': [self.get_input_configuration()],
                'Outputs': self.get_output_configuration(),
                'ApplicationCode': self.module.params['code']
                }

        # BJF: Error handling?
        if 'logs' in self.module.params and self.module.params['logs'] is not None:
            args['CloudWatchLoggingOptions'] = self.get_log_configuration()

        self.client.create_application(**args)

    def update_application(self):
        # BJF: Error handling?
        self.client.update_application(ApplicationName=self.module.params['name'],
                                       CurrentApplicationVersionId=self.current_state['ApplicationDetail'][
                                           'ApplicationVersionId'],
                                       ApplicationUpdate=self.get_app_update_configuration())

    def patch_application(self):
        self.patch_outputs()

        self.patch_logs()

    def patch_outputs(self):
        for item in self.module.params['outputs']:
            matched_describe_outputs = [i for i in self.current_state['ApplicationDetail']['OutputDescriptions'] if
                                        i['Name'] == item['name']]
            if len(matched_describe_outputs) <= 0:
                self.wait_till_updatable_state()
                self.client.add_application_output(ApplicationName=self.module.params['name'],
                                                   CurrentApplicationVersionId=self.current_state['ApplicationDetail'][
                                                       'ApplicationVersionId'],
                                                   Output=self.get_single_output_configuration(item))
                self.changed = True

        for item in self.current_state['ApplicationDetail']['OutputDescriptions']:
            matched_desired_outputs = [i for i in self.module.params['outputs'] if
                                       i['name'] == item['Name']]
            if len(matched_desired_outputs) <= 0:
                self.wait_till_updatable_state()
                self.client.delete_application_output(
                    ApplicationName=self.module.params['name'],
                    CurrentApplicationVersionId=self.current_state['ApplicationDetail'][
                        'ApplicationVersionId'],
                    OutputId=item['OutputId'])
                self.changed = True

    def patch_logs(self):
        if 'logs' in self.module.params and self.module.params['logs'] != None:
            for item in self.module.params['logs']:
                if 'CloudWatchLoggingOptionDescriptions' in self.current_state['ApplicationDetail']:
                    matched_describe_logs = [i for i in self.current_state['ApplicationDetail'][
                        'CloudWatchLoggingOptionDescriptions'] if
                                             i['LogStreamARN'] == item['stream_arn']]
                    if len(matched_describe_logs) <= 0:
                        self.wait_till_updatable_state()
                        self.client.add_application_cloud_watch_logging_option(
                            ApplicationName=self.module.params['name'],
                            CurrentApplicationVersionId=self.current_state['ApplicationDetail'][
                                'ApplicationVersionId'],
                            CloudWatchLoggingOption={
                                'LogStreamARN': item['stream_arn'],
                                'RoleARN': item['role_arn']
                            })
                        self.changed = True
                else:
                    self.wait_till_updatable_state()
                    self.client.add_application_cloud_watch_logging_option(ApplicationName=self.module.params['name'],
                                                                           CurrentApplicationVersionId=
                                                                           self.current_state['ApplicationDetail'][
                                                                               'ApplicationVersionId'],
                                                                           CloudWatchLoggingOption={
                                                                               'LogStreamARN': item['stream_arn'],
                                                                               'RoleARN': item['role_arn']
                                                                           })
                    self.changed = True

        if 'CloudWatchLoggingOptionDescriptions' in self.current_state['ApplicationDetail']:
            for item in self.current_state['ApplicationDetail']['CloudWatchLoggingOptionDescriptions']:
                if 'logs' in self.module.params:
                    matched_desired_logs = [i for i in self.module.params['logs'] if
                                            i['stream_arn'] == item['LogStreamARN']]
                    if len(matched_desired_logs) <= 0:
                        self.wait_till_updatable_state()
                        self.client.delete_application_cloud_watch_logging_option(
                            ApplicationName=self.module.params['name'],
                            CurrentApplicationVersionId=self.current_state['ApplicationDetail'][
                                'ApplicationVersionId'],
                            CloudWatchLoggingOptionId=item['CloudWatchLoggingOptionId'])
                        self.changed = True

                else:
                    self.wait_till_updatable_state()
                    self.client.delete_application_cloud_watch_logging_option(
                        ApplicationName=self.module.params['name'],
                        CurrentApplicationVersionId=self.current_state['ApplicationDetail'][
                            'ApplicationVersionId'],
                        CloudWatchLoggingOptionId=item['CloudWatchLoggingOptionId'])
                    self.changed = True

    def get_current_state(self):
        from botocore.exceptions import ClientError
        try:
            self.current_state = self.client.describe_application(ApplicationName=self.module.params['name'])
            return 'AppFound'
        except ClientError as err:
            # BJF: Not sure if ClientError is guaranteed to have this structure, but using .get() with a default
            #      arg is a safe way to avoid blowing up when accessing nested structures
            if err.response['Error']['Code'] == "ResourceNotFoundException":
                return 'AppNotFound'
            else:
                self.module.fail_json(msg="unable to obtain current state of application: {}".format(err))

    def get_final_state(self):
        try:
            return self.client.describe_application(ApplicationName=self.module.params['name'])
        except BotoCoreError as e:
            self.module.fail_json(msg="unable to obtain final state of application: {}".format(e))

    def wait_till_updatable_state(self):
        # BJF: This should be configurable with a documented default.  Giving the operator no choice is unfriendly.
        wait_complete = time.time() + 300
        while time.time() < wait_complete:
            self.current_state = self.client.describe_application(ApplicationName=self.module.params['name'])
            if self.current_state['ApplicationDetail']['ApplicationStatus'] in ['READY', 'RUNNING']:
                return
            # BJF: nitpick, but this could also be configurable
            time.sleep(5)
        self.module.fail_json(msg="wait for updatable application timeout on %s" % time.asctime())

    def get_input_configuration(self):
        inputs = []
        for item in self.module.params['inputs']:
            inputs.append(self.get_single_input_configuration(item))

        return inputs

    def get_single_input_configuration(self, item):
        # BJF: What happens in here if malformed input is provided?
        #      How are you informing the user to what's wrong?  E.g. what would happen if I provided schema.formmmat.type?
        #      This may be an oversimplification, but there are really two options here:
        #        1: Update the module spec to aggressively enforce the expected schema permutations
        #        2: Add a lot of defensive code here to enforce the schema and provide useful feedback on error
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
        # BJF: Same general comments about validation and error returns
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
        if 'logs' in self.module.params and self.module.params['logs'] != None:
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
        if 'logs' not in self.module.params or self.module.params['logs'] == None:
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
    module = AnsibleModule(
        argument_spec=KinesisDataAnalyticsApp._define_module_argument_spec(),
        supports_check_mode=False
    )

    kda_app = KinesisDataAnalyticsApp(module)
    kda_app.process_request()


#from ansible.module_utils.basic import *  # pylint: disable=W0614
if __name__ == '__main__':
    main()
