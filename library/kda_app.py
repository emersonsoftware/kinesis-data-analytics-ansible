#!/usr/bin/python

# Kinesis Data Analytics Ansible Modules
#
# Modules in this project allow management of the AWS Kinesis Data Analytics service.
#
# Authors:
#  - Pratik Patel <github: patelpratikEmerson>
#
# kda_app
#    Manage creation, update, and removal of Kinesis Data Analytics resources

# MIT License
#
# Copyright (c) 2019 Pratik Patel, Emerson
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.


DOCUMENTATION = '''
module: kda_app
author: Pratik Patel
short_description: Add, update, or remove Kinesis Data Analytics resources
description:
  - Create if no Kinesis Data Analytics Application resource is found matching the provided name
  - Delete Kinesis Data Analytics Application resource matching the provided name
  - Updates Kinesis Data Analytics Application resource matching the provided name
version_added: "1.0"
options:
  name:
    description:
    - The name of the Kinesis Data Analytics Application
    type: string
    required: True
  description:
    description:
    - Description of the Kinesis Data Analytics Application
    type: string
    default: ''
    required: False
  inputs:
    description:
    - List of source stream, at the moment Kinesis Data Analytics Application allows only one input source
    type: list
    required: True
    options:
      name_prefix:
        description:
        - Name prefix to use when creating an in-application stream
        type: string
        required: True
      parallelism:
        description:
        - Number of in-application streams to create
        type: int
        default: 1
        required: False
      kinesis:
        description:
        - Specifies what type of input stream and their ARNs
        type: dict
        required: True
          options:
            input_type:
              description:
              - Specifies type of source to read from
              type: string
              choices: ['streams', 'firehose']
              required: True
            resource_arn:
              description:
              - ARN of the source to read from
              type: string
              required: True
            role_arn:
              description:
              - ARN of the IAM role that Amazon Kinesis Analytics can assume to read from the source
              type: string
              required: True
      pre_processor:
        description:
        - Specifies pre processor which applies transformation on records after reading from source stream and before feeding into kda_app
        type: dict
        required: False
          options:
            resource_arn:
              description:
              - The ARN of the AWS Lambda function that operates on records in the stream
              type: string
              required: True
            role_arn:
              description:
              - The ARN of the IAM role that is used to access the AWS Lambda function
              type: string
              required: True
      schema:
        description:
        - Describes the format of the data in the streaming source
        type: dict
        required: True
          options:
            columns:
              description:
              - A list of RecordColumn objects
              type: list
              required: True
              options:
                name:
                  description:
                  - Name of the column created in the in-application input stream or reference table
                  type: string
                  required: True
                column_type:
                  description:
                  - Type of column created in the in-application input stream or reference table
                  type: string
                  required: True
                mapping:
                  description:
                  - Reference to the data element in the streaming input of the reference data source
                  type: string
                  required: True
            format:
              description:
              - Specifies the format of the records on the streaming source
              type: dict
              required: True
              options:
                format_type:
                  description:
                  - The type of record format
                  type: string
                  choices: ['JSON', 'CSV']
                  required: True
                json_mapping_row_path:
                  description:
                  - Path to the top-level parent that contains the records
                  type: string
                  required: False
                csv_mapping_row_delimiter:
                  description:
                  - Row delimiter. For example, in a CSV format, 'n' is the typical row delimiter
                  type: string
                  required: False
                csv_mapping_column_delimiter:
                  description:
                  - Column delimiter. For example, in a CSV format, a comma (",") is the typical column delimiter
                  type: string
                  required: False
  outputs:
    description:
    - List of destinations where application output can write data from any of the in-application streams
    - These destinations can be Amazon Kinesis streams, Amazon Kinesis Firehose delivery streams, AWS Lambda destinations, or any combination of the three
    type: list
    required: False
    options:
      name:
        description:
        - Name of the in-application stream
        type: string
        required: True
      output_type:
        description:
        - Specifies type of destination to wite to
        type: string
        choices: ['streams', 'firehose', 'lambda']
        required: True
      resource_arn:
        description:
        - ARN of the destination to write to
        type: string
        required: True
      role_arn:
        description:
        - ARN of the IAM role that Amazon Kinesis Analytics can assume to write to the destination
        type: string
        required: True
      format_type:
        description:
        - Specifies the format of the records on the output stream
        type: string
        choices: ['JSON', 'CSV']
        required: True
  logs:
    description:
    - List of CloudWatch log streams to monitor application configuration errors
    type: list
    required: False
    options:
      stream_arn:
        description:
        - ARN of the CloudWatch log to receive application messages
        type: string
        required: True
      role_arn:
        description:
        - IAM ARN of the role to use to send application messages
        type: string
        required: True
  check_timeout:
    description:
    - Specifies maximum amount of time to wait for kda_app to become updatable
    type: int
    default: 300
    required: False
  wait_between_check:
    description:
    - Specifies how many seconds to wait before checking status of kda_app again
    type: int
    default: 5
    required: False
  state:
    description:
    - Should kda_app exist or not
    choices: ['present', 'absent']
    default: 'present'
    required: False
requirements:
    - python = 2.7
    - boto
    - boto3
notes:
    - While it is possible via the boto api to create/update/delete Amazon Kinesis Analytics application with Flink runtime, this module does not support runtime Flink it only supports applications with SQL runtime.
    - This module requires that you have boto and boto3 installed and that your credentials are created or stored in a way that is compatible (see U(https://boto3.readthedocs.io/en/latest/guide/quickstart.html#configuration)).
'''

EXAMPLES = '''
---
- hosts: localhost
  gather_facts: False
  tasks:
  - name: kinesis data analytics creation
    kda_app:
      name: "testApp"
      description: "my testApp does cool things"
      state: "present"
      code: "CREATE OR REPLACE STREAM ..."
      inputs:
        - name_prefix: "SOURCE_SQL_STREAM"
          parallelism: 1
          kinesis:
            input_type: streams
            resource_arn: "arn:aws:kinesis:us-east-1:myAccount:stream/input"
            role_arn: "arn:aws:iam::myAccount:role/someRole"
          schema:
            columns:
              - name: "sensor"
                column_type: "VARCHAR(10)"
                mapping: "$.sensorId"
              - name: "isIndoorSensor"
                column_type: "BOOLEAN"
                mapping: "$.isIndoorSensor"
            format:
              format_type: "JSON"
              json_mapping_row_path: "$"
      outputs:
        - name: "DESTINATION_SQL_STREAM"
          output_type: "streams"
          resource_arn: "arn:aws:kinesis:us-east-1:myAccount:stream/output"
          role_arn: "arn:aws:iam::myAccount:role/someRole"
          format_type: "JSON"
    register: kdaapp

  - debug: var=kdaapp
'''

RETURN = '''
{
    "kdaapp": {
        "changed": false, 
        "failed": false, 
        "kda_app": {
            "ApplicationDetail": {
                "ApplicationARN": "arn:aws:kinesisanalytics:us-east-1:myAccount:application/testApp", 
                "ApplicationCode": "CREATE OR REPLACE STREAM ...", 
                "ApplicationDescription": "my testApp does cool things", 
                "ApplicationName": "testApp", 
                "ApplicationStatus": "READY", 
                "ApplicationVersionId": 1, 
                "CreateTimestamp": "2018-12-28T15:49:37-06:00", 
                "InputDescriptions": [
                    {
                        "InAppStreamNames": [
                            "SOURCE_SQL_STREAM_001"
                        ], 
                        "InputId": "1.1", 
                        "InputParallelism": {
                            "Count": 1
                        }, 
                        "InputSchema": {
                            "RecordColumns": [
                                {
                                    "Mapping": "$.sensorId", 
                                    "Name": "sensor", 
                                    "SqlType": "VARCHAR(10)"
                                }, 
                                {
                                    "Mapping": "$.isIndoorSensor", 
                                    "Name": "isIndoorSensor", 
                                    "SqlType": "BOOLEAN"
                                }
                            ], 
                            "RecordFormat": {
                                "MappingParameters": {
                                    "JSONMappingParameters": {
                                        "RecordRowPath": "$"
                                    }
                                }, 
                                "RecordFormatType": "JSON"
                            }
                        }, 
                        "InputStartingPositionConfiguration": {}, 
                        "KinesisStreamsInputDescription": {
                            "ResourceARN": "arn:aws:kinesis:us-east-1:myAccount:stream/input", 
                            "RoleARN": "arn:aws:iam::myAccount:role/someRole"
                        }, 
                        "NamePrefix": "SOURCE_SQL_STREAM"
                    }
                ], 
                "LastUpdateTimestamp": "2018-12-28T15:49:37-06:00", 
                "OutputDescriptions": [
                    {
                        "DestinationSchema": {
                            "RecordFormatType": "JSON"
                        }, 
                        "KinesisStreamsOutputDescription": {
                            "ResourceARN": "arn:aws:kinesis:us-east-1:myAccount:stream/output", 
                            "RoleARN": "arn:aws:iam::myAccount:role/someRole"
                        }, 
                        "Name": "DESTINATION_SQL_STREAM", 
                        "OutputId": "1.1"
                    }
                ]
            }, 
            "ResponseMetadata": {
                "HTTPHeaders": {
                    "content-length": "16053", 
                    "content-type": "application/x-amz-json-1.1", 
                    "date": "Mon, 31 Dec 2018 16:12:48 GMT", 
                    "x-amzn-requestid": "some id"
                }, 
                "HTTPStatusCode": 200, 
                "RequestId": "some id", 
                "RetryAttempts": 0
            }
        }
    }
}
'''

__version__ = "${version}"

import time
from ansible.module_utils.basic import *  # pylint: disable=W0614

try:
    import boto3
    import boto
    from botocore.exceptions import BotoCoreError
    from botocore.exceptions import ClientError

    HAS_BOTO3 = True
except ImportError:
    HAS_BOTO3 = False

FIREHOSE = "firehose"
STREAMS = "streams"
LAMBDA = "lambda"
FORMAT_JSON = "JSON"
FORMAT_CSV = "CSV"
STATE_PRESENT = "present"
STATE_ABSENT = "absent"


class KinesisDataAnalyticsApp:
    current_state = None
    changed = False

    def __init__(self, module):
        self.module = module
        if not HAS_BOTO3:
            self.module.fail_json(msg="boto and boto3 are required for this module")
        self.client = boto3.client("kinesisanalytics")

    @staticmethod
    def _define_module_argument_spec():
        return dict(name=dict(required=True, type="str"),
                    description=dict(required=False, default="", type="str"),
                    code=dict(required=True, type="str"),
                    inputs=dict(
                        required=True,
                        type="list",
                        name_prefix=dict(required=True, type="str"),
                        parallelism=dict(required=False, default=1, type="int"),
                        kinesis=dict(required=True,
                                     input_type=dict(required=True,
                                                     default=STREAMS,
                                                     choices=[STREAMS, FIREHOSE],
                                                     type="str"
                                                     ),
                                     resource_arn=dict(required=True, type="str"),
                                     role_arn=dict(required=True, type="str"),
                                     ),
                        pre_processor=dict(required=False,
                                           resource_arn=dict(required=True, type="str"),
                                           role_arn=dict(required=True, type="str"),
                                           ),
                        schema=dict(required=True,
                                    columns=dict(required=True,
                                                 type="list",
                                                 name=dict(required=True, type="str"),
                                                 column_type=dict(required=True, type="str"),
                                                 mapping=dict(required=True, type="str")
                                                 ),
                                    format=dict(required=True,
                                                format_type=dict(required=True,
                                                                 choices=[FORMAT_JSON, FORMAT_CSV],
                                                                 type="str",
                                                                 ),
                                                json_mapping_row_path=dict(required=False, type="str"),
                                                csv_mapping_row_delimiter=dict(required=False, type="str"),
                                                csv_mapping_column_delimiter=dict(required=False, type="str"),
                                                ),
                                    ),
                    ),
                    outputs=dict(required=False,
                                 type="list",
                                 name=dict(required=True, type="str"),
                                 output_type=dict(required=True,
                                                  options=[STREAMS, FIREHOSE, LAMBDA],
                                                  type="str",
                                                  ),
                                 resource_arn=dict(required=True, type="str"),
                                 role_arn=dict(required=True, type="str"),
                                 format_type=dict(required=True,
                                                  options=[FORMAT_JSON, FORMAT_CSV],
                                                  type="str"
                                                  ),
                                 ),
                    logs=dict(required=False,
                              type="list",
                              stream_arn=dict(required=True, type="str"),
                              role_arn=dict(required=True, type="str")
                              ),
                    check_timeout=dict(required=False, default=300, type="int"),
                    wait_between_check=dict(required=False, default=5, type="int"),
                    state=dict(default=STATE_PRESENT, choices=[STATE_PRESENT, STATE_ABSENT]),
                    )

    def process_request(self):
        try:
            current_app_state = self.get_current_state()
            desired_app_state = safe_get(self.module.params, "state", STATE_PRESENT)

            if current_app_state == desired_app_state == STATE_PRESENT:
                self.achieve_present_state(current_app_state)
            elif current_app_state != desired_app_state and desired_app_state == STATE_PRESENT:
                self.achieve_present_state(current_app_state)
            elif current_app_state != desired_app_state and desired_app_state == STATE_ABSENT:
                self.achieve_absent_state()

        except (BotoCoreError, ClientError):
            return
        except Exception as e:
            self.module.fail_json(msg="unknown error: {}".format(e))
            return

        self.module.exit_json(changed=self.changed, kda_app=self.current_state)

    def achieve_present_state(self, current_app_state):
        if current_app_state is STATE_ABSENT:
            self.create_new_application()
            self.changed = True
        elif current_app_state is STATE_PRESENT:
            if self.is_app_updatable_state_changed():
                self.update_application()
                self.changed = True
            self.patch_application()

        self.get_final_state()

    def achieve_absent_state(self):
        try:
            self.client.delete_application(ApplicationName=safe_get(self.module.params, "name", None),
                                           CreateTimestamp=safe_get(self.current_state,
                                                                         "ApplicationDetail.CreateTimestamp", None))
        except BotoCoreError as e:
            self.module.fail_json(msg="delete application failed: {}".format(e))

    def create_new_application(self):
        args = {"ApplicationName": safe_get(self.module.params, "name", None),
                "ApplicationDescription": safe_get(self.module.params, "description", None),
                "Inputs": self.get_input_configuration(),
                "Outputs": self.get_output_configuration(),
                "ApplicationCode": safe_get(self.module.params, "code", None)
                }

        if "logs" in self.module.params and self.module.params["logs"] is not None:
            args["CloudWatchLoggingOptions"] = self.get_log_configuration()
        try:
            self.client.create_application(**args)
        except BotoCoreError as e:
            self.module.fail_json(msg="create application failed: {}".format(e))

    def update_application(self):
        try:
            self.client.update_application(ApplicationName=safe_get(self.module.params, "name", None),
                                           CurrentApplicationVersionId=
                                           safe_get(self.current_state, "ApplicationDetail.ApplicationVersionId",
                                                         None),
                                           ApplicationUpdate=self.get_app_update_configuration())
        except BotoCoreError as e:
            self.module.fail_json(msg="update application failed: {}".format(e))

    def patch_application(self):
        self.patch_outputs()
        self.patch_logs()

    def patch_outputs(self):
        for item in safe_get(self.module.params, "outputs", []):
            matched_describe_outputs = [i for i in
                                        safe_get(self.current_state, "ApplicationDetail.OutputDescriptions", []) if
                                        safe_get(i, "Name", "") == item["name"]]
            if len(matched_describe_outputs) <= 0:
                self.wait_till_updatable_state()
                try:
                    self.client.add_application_output(ApplicationName=safe_get(self.module.params, "name", None),
                                                       CurrentApplicationVersionId=
                                                       safe_get(self.current_state,
                                                                     "ApplicationDetail.ApplicationVersionId", None),
                                                       Output=self.get_single_output_configuration(item))
                except BotoCoreError as e:
                    self.module.fail_json(msg="add application output failed: {}".format(e))
                self.changed = True

        for item in safe_get(self.current_state, "ApplicationDetail.OutputDescriptions", []):
            matched_desired_outputs = [i for i in safe_get(self.module.params, "outputs", []) if
                                       safe_get(i, "name", "") == item["Name"]]
            if len(matched_desired_outputs) <= 0:
                self.wait_till_updatable_state()
                try:
                    self.client.delete_application_output(
                        ApplicationName=safe_get(self.module.params, "name", None),
                        CurrentApplicationVersionId=safe_get(self.current_state,
                                                                  "ApplicationDetail.ApplicationVersionId", None),
                        OutputId=safe_get(item, "OutputId", None))
                except BotoCoreError as e:
                    self.module.fail_json(msg="delete application output failed: {}".format(e))
                self.changed = True

    def patch_logs(self):
        if "logs" in self.module.params and self.module.params["logs"] != None:
            for item in self.module.params["logs"]:
                if "CloudWatchLoggingOptionDescriptions" in safe_get(self.current_state, "ApplicationDetail", {}):
                    matched_describe_logs = [i for i in safe_get(self.current_state,
                                                                      "ApplicationDetail.CloudWatchLoggingOptionDescriptions",
                                                                      []) if
                                             safe_get(i, "LogStreamARN", "") == safe_get(item, "stream_arn",
                                                                                                   "")]
                    if len(matched_describe_logs) <= 0:
                        self.wait_till_updatable_state()
                        try:
                            self.client.add_application_cloud_watch_logging_option(
                                ApplicationName=safe_get(self.module.params, "name", None),
                                CurrentApplicationVersionId=safe_get(self.current_state,
                                                                          "ApplicationDetail.ApplicationVersionId",
                                                                          None),
                                CloudWatchLoggingOption={
                                    "LogStreamARN": safe_get(item, "stream_arn", ""),
                                    "RoleARN": safe_get(item, "role_arn", "")
                                })
                        except BotoCoreError as e:
                            self.module.fail_json(msg="add application logging failed: {}".format(e))
                        self.changed = True
                else:
                    self.wait_till_updatable_state()
                    try:
                        self.client.add_application_cloud_watch_logging_option(
                            ApplicationName=safe_get(self.module.params, "name", None),
                            CurrentApplicationVersionId=
                            safe_get(self.current_state, "ApplicationDetail.ApplicationVersionId", None),
                            CloudWatchLoggingOption={
                                "LogStreamARN": safe_get(item, "stream_arn", ""),
                                "RoleARN": safe_get(item, "role_arn", "")
                            })
                    except BotoCoreError as e:
                        self.module.fail_json(msg="add application logging failed: {}".format(e))
                    self.changed = True

        if "CloudWatchLoggingOptionDescriptions" in safe_get(self.current_state, "ApplicationDetail", {}):
            for item in safe_get(self.current_state, "ApplicationDetail.CloudWatchLoggingOptionDescriptions", []):
                if "logs" in self.module.params:
                    matched_desired_logs = [i for i in safe_get(self.module.params, "logs", []) if
                                            safe_get(i, "stream_arn", "") == safe_get(item, "LogStreamARN",
                                                                                                "")]
                    if len(matched_desired_logs) <= 0:
                        self.wait_till_updatable_state()
                        try:
                            self.client.delete_application_cloud_watch_logging_option(
                                ApplicationName=safe_get(self.module.params, "name", None),
                                CurrentApplicationVersionId=safe_get(self.current_state,
                                                                          "ApplicationDetail.ApplicationVersionId",
                                                                          None),
                                CloudWatchLoggingOptionId=safe_get(item, "CloudWatchLoggingOptionId", None))
                        except BotoCoreError as e:
                            self.module.fail_json(msg="delete application logging failed: {}".format(e))
                        self.changed = True

                else:
                    self.wait_till_updatable_state()
                    try:
                        self.client.delete_application_cloud_watch_logging_option(
                            ApplicationName=safe_get(self.module.params, "name", None),
                            CurrentApplicationVersionId=safe_get(self.current_state,
                                                                      "ApplicationDetail.ApplicationVersionId", None),
                            CloudWatchLoggingOptionId=safe_get(item, "CloudWatchLoggingOptionId", None))
                    except BotoCoreError as e:
                        self.module.fail_json(msg="delete application logging failed: {}".format(e))
                    self.changed = True

    def get_current_state(self):
        try:
            self.current_state = self.client.describe_application(
                ApplicationName=safe_get(self.module.params, "name", None))
            return STATE_PRESENT
        except ClientError as err:
            if safe_get(err.response, "Error.Code", "") == "ResourceNotFoundException":
                return STATE_ABSENT
            else:
                self.module.fail_json(msg="unable to obtain current state of application: {}".format(err))

    def get_final_state(self):
        try:
            self.current_state = self.client.describe_application(
                ApplicationName=safe_get(self.module.params, "name", None))
        except BotoCoreError as e:
            self.module.fail_json(msg="unable to obtain final state of application: {}".format(e))

    def wait_till_updatable_state(self):
        wait_complete = time.time() + safe_get(self.module.params, "check_timeout", 300)
        while time.time() < wait_complete:
            self.current_state = self.client.describe_application(
                ApplicationName=safe_get(self.module.params, "name", None))
            if safe_get(self.current_state, "ApplicationDetail.ApplicationStatus", "") in ["READY", "RUNNING"]:
                return
            time.sleep(safe_get(self.module.params, "wait_between_check", 5))
        self.module.fail_json(msg="wait for updatable application timeout on %s" % time.asctime())

    def get_input_configuration(self):
        inputs = []
        for item in safe_get(self.module.params, "inputs", []):
            inputs.append(self.get_single_input_configuration(item))

        return inputs

    def get_single_input_configuration(self, item):
        input_item = {
            "NamePrefix": safe_get(item, "name_prefix", ""),
            "InputParallelism": {
                "Count": safe_get(item, "parallelism", 0)
            },
            "InputSchema": {
                "RecordFormat": {
                    "RecordFormatType": safe_get(item, "schema.format.format_type", ""),
                    "MappingParameters": {}
                },
                "RecordColumns": [],
            }
        }

        if safe_get(item, "kinesis.input_type", "") == STREAMS:
            input_item["KinesisStreamsInput"] = {
                "ResourceARN": safe_get(item, "kinesis.resource_arn", ""),
                "RoleARN": safe_get(item, "kinesis.role_arn", ""),
            }
        elif safe_get(item, "kinesis.input_type", "") == FIREHOSE:
            input_item["KinesisFirehoseInput"] = {
                "ResourceARN": safe_get(item, "kinesis.resource_arn", ""),
                "RoleARN": safe_get(item, "kinesis.role_arn", ""),
            }

        if "pre_processor" in item:
            input_item["InputProcessingConfiguration"] = {}
            input_item["InputProcessingConfiguration"]["InputLambdaProcessor"] = {
                "ResourceARN": safe_get(item, "pre_processor.resource_arn", ""),
                "RoleARN": safe_get(item, "pre_processor.role_arn", ""),
            }

        if safe_get(item, "schema.format.format_type", "") == FORMAT_JSON:
            input_item["InputSchema"]["RecordFormat"]["MappingParameters"]["JSONMappingParameters"] = {
                "RecordRowPath": safe_get(item, "schema.format.json_mapping_row_path", ""),
            }
        elif safe_get(item, "schema.format.format_type", "") == FORMAT_CSV:
            input_item["InputSchema"]["RecordFormat"]["MappingParameters"]["CSVMappingParameters"] = {
                "RecordRowDelimiter": safe_get(item, "schema.format.csv_mapping_row_delimiter", ""),
                "RecordColumnDelimiter": safe_get(item, "schema.format.csv_mapping_column_delimiter", ""),
            }

        for column in safe_get(item, "schema.columns", []):
            input_item["InputSchema"]["RecordColumns"].append({
                "Mapping": safe_get(column, "mapping", ""),
                "Name": safe_get(column, "name", ""),
                "SqlType": safe_get(column, "column_type", ""),
            })
        return input_item

    def get_output_configuration(self):
        outputs = []

        for item in safe_get(self.module.params, "outputs", []):
            outputs.append(self.get_single_output_configuration(item))

        return outputs

    def get_single_output_configuration(self, item):
        output = {
            "Name": safe_get(item, "name", None),
            "DestinationSchema": {
                "RecordFormatType": safe_get(item, "format_type", "")
            }
        }
        if safe_get(item, "output_type", "") == STREAMS:
            output["KinesisStreamsOutput"] = {
                "ResourceARN": safe_get(item, "resource_arn", ""),
                "RoleARN": safe_get(item, "role_arn", ""),
            }
        elif safe_get(item, "output_type", "") == FIREHOSE:
            output["KinesisFirehoseOutput"] = {
                "ResourceARN": safe_get(item, "resource_arn", ""),
                "RoleARN": safe_get(item, "role_arn", ""),
            }
        elif safe_get(item, "output_type", "") == LAMBDA:
            output["LambdaOutput"] = {
                "ResourceARN": safe_get(item, "resource_arn", ""),
                "RoleARN": safe_get(item, "role_arn", ""),
            }

        return output

    def get_log_configuration(self):
        logs = []
        if "logs" in self.module.params and self.module.params["logs"] != None:
            for item in self.module.params["logs"]:
                logs.append({
                    "LogStreamARN": safe_get(item, "stream_arn", ""),
                    "RoleARN": safe_get(item, "role_arn", ""),
                })

        return logs

    def get_app_update_configuration(self):
        update_config = {}

        if safe_get(self.module.params, "code", "").replace("\n", "") != safe_get(self.current_state,
                                                                                            "ApplicationDetail.ApplicationCode",
                                                                                            "").replace("\n", ""):
            update_config["ApplicationCodeUpdate"] = safe_get(self.module.params, "code", None)

        if self.is_input_configuration_change():
            update_config["InputUpdates"] = self.get_input_update_configuration()

        if self.is_output_configuration_change():
            update_config["OutputUpdates"] = self.get_output_update_configuration()

        if self.is_log_configuration_changed():
            update_config["CloudWatchLoggingOptionUpdates"] = self.get_log_update_configuration()

        return update_config

    def is_app_updatable_state_changed(self):
        return safe_get(self.module.params, "code", "").replace("\n", "") != safe_get(self.current_state,
                                                                                                "ApplicationDetail.ApplicationCode",
                                                                                                "").replace("\n",
                                                                                                            "") or self.is_input_configuration_change() or self.is_output_configuration_change() or self.is_log_configuration_changed()

    def is_output_configuration_change(self):
        for output in safe_get(self.module.params, "outputs", []):
            matched_describe_outputs = [i for i in
                                        safe_get(self.current_state, "ApplicationDetail.OutputDescriptions", []) if
                                        safe_get(i, "Name", "") == safe_get(output, "name", "")]
            if len(matched_describe_outputs) != 1:
                continue
            describe_output = matched_describe_outputs[0]

            output_type = safe_get(output, "output_type", "")

            if output_type == STREAMS:
                if "KinesisStreamsOutputDescription" not in describe_output:
                    return True
                if output["resource_arn"] != safe_get(describe_output,
                                                           "KinesisStreamsOutputDescription.ResourceARN", ""):
                    return True
                if output["role_arn"] != safe_get(describe_output, "KinesisStreamsOutputDescription.RoleARN", ""):
                    return True

            if output_type == FIREHOSE:
                if "KinesisFirehoseOutputDescription" not in describe_output:
                    return True
                if output["resource_arn"] != safe_get(describe_output,
                                                           "KinesisFirehoseOutputDescription.ResourceARN", ""):
                    return True
                if output["role_arn"] != safe_get(describe_output, "KinesisFirehoseOutputDescription.RoleARN", ""):
                    return True

            if output_type == LAMBDA:
                if "LambdaOutputDescription" not in describe_output:
                    return True
                if output["resource_arn"] != safe_get(describe_output, "LambdaOutputDescription.ResourceARN", ""):
                    return True
                if output["role_arn"] != safe_get(describe_output, "LambdaOutputDescription.RoleARN", ""):
                    return True

            if safe_get(output, "format_type", "") != safe_get(describe_output,
                                                                         "DestinationSchema.RecordFormatType", ""):
                return True

        return False

    def is_input_configuration_change(self):
        for input in safe_get(self.module.params, "inputs", []):
            matched_describe_inputs = [i for i in
                                       safe_get(self.current_state, "ApplicationDetail.InputDescriptions", []) if
                                       safe_get(i, "NamePrefix", "") == safe_get(input, "name_prefix", "")]
            if len(matched_describe_inputs) != 1:
                return True
            describe_input = matched_describe_inputs[0]

            if safe_get(input, "schema.format.format_type", "") != safe_get(describe_input,
                                                                                      "InputSchema.RecordFormat.RecordFormatType",
                                                                                      ""):
                return True

            if safe_get(input, "schema.format.format_type", "") == FORMAT_JSON:
                if safe_get(input, "schema.format.json_mapping_row_path", "") != \
                        safe_get(describe_input,
                                      "InputSchema.RecordFormat.MappingParameters.JSONMappingParameters.RecordRowPath",
                                      ""):
                    return True

            if safe_get(input, "schema.format.format_type", "") == FORMAT_CSV:
                if safe_get(input, "schema.format.csv_mapping_row_delimiter", "") != \
                        safe_get(describe_input,
                                      "InputSchema.RecordFormat.MappingParameters.CSVMappingParameters.RecordRowDelimiter",
                                      ""):
                    return True
                if safe_get(input, "schema.format.csv_mapping_column_delimiter", "") != \
                        safe_get(describe_input,
                                      "InputSchema.RecordFormat.MappingParameters.CSVMappingParameters.RecordColumnDelimiter",
                                      ""):
                    return True

            if len(safe_get(input, "schema.columns", [])) != len(
                    safe_get(describe_input, "InputSchema.RecordColumns", [])):
                return True

            for col in safe_get(input, "schema.columns", []):
                matched_describe_cols = [i for i in safe_get(describe_input, "InputSchema.RecordColumns", []) if
                                         safe_get(i, "Name", "") == safe_get(col, "name", "")]
                if len(matched_describe_cols) != 1:
                    return True
                describe_col = matched_describe_cols[0]
                if safe_get(describe_col, "SqlType", "") != safe_get(col, "column_type", "") or safe_get(
                        describe_col, "Mapping", "") != safe_get(col,
                                                                      "mapping", ""):
                    return True

            if safe_get(input, "parallelism", 0) != safe_get(describe_input, "InputParallelism.Count", 0):
                return True

            input_type = safe_get(input, "kinesis.input_type", "")
            if input_type == STREAMS:
                if "KinesisStreamsInputDescription" in describe_input:
                    if safe_get(input, "kinesis.resource_arn", "") != safe_get(describe_input,
                                                                                         "KinesisStreamsInputDescription.ResourceARN",
                                                                                         ""):
                        return True
                    if safe_get(input, "kinesis.role_arn", "") != safe_get(describe_input,
                                                                                     "KinesisStreamsInputDescription.RoleARN",
                                                                                     ""):
                        return True

            if input_type == FIREHOSE:
                if "KinesisFirehoseInputDescription" in describe_input:
                    if safe_get(input, "kinesis.resource_arn", "") != safe_get(describe_input,
                                                                                         "KinesisFirehoseInputDescription.ResourceARN",
                                                                                         ""):
                        return True
                    if safe_get(input, "kinesis.role_arn", "") != safe_get(describe_input,
                                                                                     "KinesisFirehoseInputDescription.RoleARN",
                                                                                     ""):
                        return True

            if "pre_processor" in input:
                if "InputProcessingConfigurationDescription" not in describe_input:
                    return True
                if safe_get(input, "pre_processor.resource_arn", "") != \
                        safe_get(describe_input,
                                      "InputProcessingConfigurationDescription.InputLambdaProcessorDescription.ResourceARN",
                                      ""):
                    return True
                if safe_get(input, "pre_processor.role_arn", "") != \
                        safe_get(describe_input,
                                      "InputProcessingConfigurationDescription.InputLambdaProcessorDescription.RoleARN",
                                      ""):
                    return True

        return False

    def is_log_configuration_changed(self):
        if "logs" not in self.module.params or self.module.params["logs"] == None:
            return False

        for log in safe_get(self.module.params, "logs", []):
            matched_describe_logs = [i for i in
                                     safe_get(self.current_state,
                                                   "ApplicationDetail.CloudWatchLoggingOptionDescriptions", []) if
                                     safe_get(i, "LogStreamARN", "") == safe_get(log, "stream_arn", "")]
            if len(matched_describe_logs) != 1:
                continue
            describe_log = matched_describe_logs[0]

            if safe_get(log, "role_arn", "") != safe_get(describe_log, "RoleARN", ""):
                return True

        return False

    def get_input_update_configuration(self):
        expected = []
        for item in safe_get(self.module.params, "inputs", []):
            describe_inputs = safe_get(self.current_state, "ApplicationDetail.InputDescriptions", [])

            input_item = {
                "InputId": safe_get(describe_inputs[0], "InputId", None),
                "NamePrefixUpdate": safe_get(item, "name_prefix", None),
                "InputParallelismUpdate": {
                    "CountUpdate": safe_get(item, "parallelism", 0)
                },
                "InputSchemaUpdate": {
                    "RecordFormatUpdate": {
                        "RecordFormatType": safe_get(item, "schema.format.format_type", ""),
                        "MappingParameters": {}
                    },
                    "RecordColumnUpdates": [],
                }
            }

            input_type = safe_get(item, "kinesis.input_type", "")

            if input_type == STREAMS:
                input_item["KinesisStreamsInputUpdate"] = {
                    "ResourceARNUpdate": safe_get(item, "kinesis.resource_arn", ""),
                    "RoleARNUpdate": safe_get(item, "kinesis.role_arn", ""),
                }
            elif input_type == FIREHOSE:
                input_item["KinesisFirehoseInputUpdate"] = {
                    "ResourceARNUpdate": safe_get(item, "kinesis.resource_arn", ""),
                    "RoleARNUpdate": safe_get(item, "kinesis.role_arn", ""),
                }

            if "pre_processor" in item:
                input_item["InputProcessingConfigurationUpdate"] = {}
                input_item["InputProcessingConfigurationUpdate"]["InputLambdaProcessorUpdate"] = {
                    "ResourceARNUpdate": safe_get(item, "pre_processor.resource_arn", ""),
                    "RoleARNUpdate": safe_get(item, "pre_processor.role_arn", ""),
                }

            format_type = safe_get(item, "schema.format.format_type", "")
            if format_type == FORMAT_JSON:
                input_item["InputSchemaUpdate"]["RecordFormatUpdate"]["MappingParameters"]["JSONMappingParameters"] = {
                    "RecordRowPath": safe_get(item, "schema.format.json_mapping_row_path", ""),
                }
            elif format_type == FORMAT_CSV:
                input_item["InputSchemaUpdate"]["RecordFormatUpdate"]["MappingParameters"]["CSVMappingParameters"] = {
                    "RecordRowDelimiter": safe_get(item, "schema.format.csv_mapping_row_delimiter", ""),
                    "RecordColumnDelimiter": safe_get(item, "schema.format.csv_mapping_column_delimiter", ""),
                }

            for column in safe_get(item, "schema.columns", []):
                input_item["InputSchemaUpdate"]["RecordColumnUpdates"].append({
                    "Mapping": safe_get(column, "mapping", ""),
                    "Name": safe_get(column, "name", ""),
                    "SqlType": safe_get(column, "column_type", ""),
                })
            expected.append(input_item)

        return expected

    def get_output_update_configuration(self):
        expected = []

        for item in safe_get(self.module.params, "outputs", []):
            matched_describe_outputs = [i for i in
                                        safe_get(self.current_state, "ApplicationDetail.OutputDescriptions", []) if
                                        safe_get(i, "Name", "") == safe_get(item, "name", "")]

            if len(matched_describe_outputs) != 1:
                continue

            output = {
                "OutputId": safe_get(matched_describe_outputs[0], "OutputId", None),
                "NameUpdate": safe_get(item, "name", None),
                "DestinationSchemaUpdate": {
                    "RecordFormatType": safe_get(item, "format_type", "")
                }
            }
            output_type = safe_get(item, "output_type", "")
            if output_type == STREAMS:
                output["KinesisStreamsOutputUpdate"] = {
                    "ResourceARNUpdate": safe_get(item, "resource_arn", ""),
                    "RoleARNUpdate": safe_get(item, "role_arn", ""),
                }
            elif output_type == FIREHOSE:
                output["KinesisFirehoseOutputUpdate"] = {
                    "ResourceARNUpdate": safe_get(item, "resource_arn", ""),
                    "RoleARNUpdate": safe_get(item, "role_arn", ""),
                }
            elif output_type == LAMBDA:
                output["LambdaOutputUpdate"] = {
                    "ResourceARNUpdate": safe_get(item, "resource_arn", ""),
                    "RoleARNUpdate": safe_get(item, "role_arn", ""),
                }
            expected.append(output)

        return expected

    def get_log_update_configuration(self):
        expected = []

        for item in safe_get(self.module.params, "logs", []):
            matched_describe_logs = [i for i in
                                     safe_get(self.current_state,
                                                   "ApplicationDetail.CloudWatchLoggingOptionDescriptions", []) if
                                     safe_get(i, "LogStreamARN", "") == safe_get(item, "stream_arn", "")]

            if len(matched_describe_logs) != 1:
                continue

            log = {
                "CloudWatchLoggingOptionId": safe_get(matched_describe_logs[0], "CloudWatchLoggingOptionId", None),
                "LogStreamARNUpdate": safe_get(item, "stream_arn", ""),
                "RoleARNUpdate": safe_get(item, "role_arn", "")
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


def safe_get(dct, path, default_value):
    nested_keys = path.split(".")
    try:
        actual = dct
        for k in nested_keys:
            actual = actual[k]
        return actual
    except KeyError:
        return default_value


if __name__ == "__main__":
    main()
