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
            self.client.create_application(ApplicationName=self.module.params['name'], ApplicationDescription=self.module.params['description'], Inputs=[self.get_input_configuration()], Outputs=[], CloudWatchLoggingOptions=[], ApplicationCode=self.module.params['code'])

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
        inputs = {
            'NamePrefix': self.module.params['inputs']['name_prefix'],
            'InputParallelism': {
                'Count': self.module.params['inputs']['parallelism']
            },
            'InputSchema': {
                'RecordFormat': {
                    'RecordFormatType': self.module.params['inputs']['schema']['format']['type'],
                    'MappingParameters': {}
                },
                'RecordColumns': [],
            }
        }

        if self.module.params['inputs']['kinesis']['type'] == 'streams':
            inputs['KinesisStreamsInput'] = {
                'ResourceARN': self.module.params['inputs']['kinesis']['resource_arn'],
                'RoleARN': self.module.params['inputs']['kinesis']['role_arn'],
            }
        elif self.module.params['inputs']['kinesis']['type'] == 'firehose':
            inputs['KinesisFirehoseInput'] = {
                'ResourceARN': self.module.params['inputs']['kinesis']['resource_arn'],
                'RoleARN': self.module.params['inputs']['kinesis']['role_arn'],
            }

        if 'pre_processor' in self.module.params['inputs']:
            inputs['InputProcessingConfiguration'] = {}
            inputs['InputProcessingConfiguration']['InputLambdaProcessor'] = {
                'ResourceARN': self.module.params['inputs']['pre_processor']['resource_arn'],
                'RoleARN': self.module.params['inputs']['pre_processor']['role_arn'],
            }

        if self.module.params['inputs']['schema']['format']['type'] == 'JSON':
            inputs['InputSchema']['RecordFormat']['MappingParameters']['JSONMappingParameters'] = {
                'RecordRowPath': self.module.params['inputs']['schema']['format']['json_mapping_row_path'],
            }
        elif self.module.params['inputs']['schema']['format']['type'] == 'CSV':
            inputs['InputSchema']['RecordFormat']['MappingParameters']['CSVMappingParameters'] = {
                'RecordRowDelimiter': self.module.params['inputs']['schema']['format']['csv_mapping_row_delimiter'],
                'RecordColumnDelimiter': self.module.params['inputs']['schema']['format']['csv_mapping_column_delimiter'],
            }

        for column in self.module.params['inputs']['schema']['columns']:
            inputs['InputSchema']['RecordColumns'].append({
                'Mapping': column['mapping'],
                'Name': column['name'],
                'SqlType': column['type'],
            })

        return inputs


def main():
    print('who cares')


if __name__ == '__main__':
    main()
