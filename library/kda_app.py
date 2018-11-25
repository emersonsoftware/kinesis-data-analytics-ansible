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
            self.client.create_application(ApplicationName='', ApplicationDescription='', Inputs=[], Outputs=[], CloudWatchLoggingOptions=[], ApplicationCode='')

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


def main():
    print('who cares')


if __name__ == '__main__':
    main()
