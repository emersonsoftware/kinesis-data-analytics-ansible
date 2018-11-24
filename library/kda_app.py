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
    def __init__(self, module):
        self.module = module
        if not HAS_BOTO3:
            self.module.fail_json(msg='boto and boto3 are required for this module')
        self.client = boto3.client('kinesisanalytics')


def main():
    print('who cares')


if __name__ == '__main__':
    main()
