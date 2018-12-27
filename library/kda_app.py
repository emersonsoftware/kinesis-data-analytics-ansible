#!/usr/bin/python

__version__ = "${version}"

import time

try:
    import boto3
    import boto
    from botocore.exceptions import BotoCoreError
    from botocore.exceptions import ClientError

    HAS_BOTO3 = True
except ImportError:
    HAS_BOTO3 = False


# BJF: Top-level thoughts:
#      1: There is typically a "flower-box" style comment at the top where you claim authorship and document your license (needed for open sourcing)
#      2: It is customary to document your module"s usage.  See the APIGW modules for examples.
#         Well-formed docs of this style can be used to generate documentation
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
                                                     default="streams",
                                                     choices=["streams", "firehose"],
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
                                                                 choices=["JSON", "CSV"],
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
                                                  options=["streams", "firehose", "lambda"],
                                                  type="str",
                                                  ),
                                 resource_arn=dict(required=True, type="str"),
                                 role_arn=dict(required=True, type="str"),
                                 format_type=dict(required=True,
                                                  options=["JSON", "CSV"],
                                                  type="str"
                                                  ),
                                 ),
                    logs=dict(required=False,
                              type="list",
                              stream_arn=dict(required=True, type="str"),
                              role_arn=dict(required=True, type="str")
                              ),
                    starting_position=dict(default="LAST_STOPPED_POINT",
                                           choices=["NOW", "TRIM_HORIZON", "LAST_STOPPED_POINT"],
                                           type="str"
                                           ),
                    check_timeout=dict(required=False, default=300, type="int"),
                    wait_between_check=dict(required=False, default=5, type="int"),
                    )

    def process_request(self):
        try:
            status = self.get_current_state()
            if status is "AppNotFound":
                self.create_new_application()
                self.changed = True
            elif status is "AppFound":
                if self.is_app_updatable_state_changed():
                    self.update_application()
                    self.changed = True
                self.patch_application()

            self.get_final_state()
        except (BotoCoreError, ClientError):
            return
        except Exception as e:
            self.module.fail_json(msg="unknown error: {}".format(e))
            return

        self.module.exit_json(changed=self.changed, kda_app=self.current_state)

    def start_application(self):
        self.client.start_application(ApplicationName=self.safe_get(self.module.params, "name", None),
                                      InputConfigurations=self.get_input_start_configuration())

    def create_new_application(self):
        args = {"ApplicationName": self.safe_get(self.module.params, "name", None),
                "ApplicationDescription": self.safe_get(self.module.params, "description", None),
                "Inputs": [self.get_input_configuration()],
                "Outputs": self.get_output_configuration(),
                "ApplicationCode": self.safe_get(self.module.params, "code", None)
                }

        if "logs" in self.module.params and self.module.params["logs"] is not None:
            args["CloudWatchLoggingOptions"] = self.get_log_configuration()
        try:
            self.client.create_application(**args)
        except BotoCoreError as e:
            self.module.fail_json(msg="create application failed: {}".format(e))
            raise e

    def update_application(self):
        try:
            self.client.update_application(ApplicationName=self.safe_get(self.module.params, "name", None),
                                           CurrentApplicationVersionId=
                                           self.safe_get(self.current_state, "ApplicationDetail.ApplicationVersionId",
                                                         None),
                                           ApplicationUpdate=self.get_app_update_configuration())
        except BotoCoreError as e:
            self.module.fail_json(msg="update application failed: {}".format(e))
            raise e

    def patch_application(self):
        self.patch_outputs()
        self.patch_logs()

    def patch_outputs(self):
        for item in self.safe_get(self.module.params, "outputs", []):
            matched_describe_outputs = [i for i in
                                        self.safe_get(self.current_state, "ApplicationDetail.OutputDescriptions", []) if
                                        self.safe_get(i, "Name", "") == item["name"]]
            if len(matched_describe_outputs) <= 0:
                self.wait_till_updatable_state()
                try:
                    self.client.add_application_output(ApplicationName=self.safe_get(self.module.params, "name", None),
                                                       CurrentApplicationVersionId=
                                                       self.safe_get(self.current_state,
                                                                     "ApplicationDetail.ApplicationVersionId", None),
                                                       Output=self.get_single_output_configuration(item))
                except BotoCoreError as e:
                    self.module.fail_json(msg="add application output failed: {}".format(e))
                    raise e
                self.changed = True

        for item in self.safe_get(self.current_state, "ApplicationDetail.OutputDescriptions", []):
            matched_desired_outputs = [i for i in self.safe_get(self.module.params, "outputs", []) if
                                       self.safe_get(i, "name", "") == item["Name"]]
            if len(matched_desired_outputs) <= 0:
                self.wait_till_updatable_state()
                try:
                    self.client.delete_application_output(
                        ApplicationName=self.safe_get(self.module.params, "name", None),
                        CurrentApplicationVersionId=self.safe_get(self.current_state,
                                                                  "ApplicationDetail.ApplicationVersionId", None),
                        OutputId=self.safe_get(item, "OutputId", None))
                except BotoCoreError as e:
                    self.module.fail_json(msg="delete application output failed: {}".format(e))
                    raise e
                self.changed = True

    def patch_logs(self):
        if "logs" in self.module.params and self.module.params["logs"] != None:
            for item in self.module.params["logs"]:
                if "CloudWatchLoggingOptionDescriptions" in self.safe_get(self.current_state, "ApplicationDetail", {}):
                    matched_describe_logs = [i for i in self.safe_get(self.current_state,
                                                                      "ApplicationDetail.CloudWatchLoggingOptionDescriptions",
                                                                      []) if
                                             self.safe_get(i, "LogStreamARN", "") == self.safe_get(item, "stream_arn",
                                                                                                   "")]
                    if len(matched_describe_logs) <= 0:
                        self.wait_till_updatable_state()
                        try:
                            self.client.add_application_cloud_watch_logging_option(
                                ApplicationName=self.safe_get(self.module.params, "name", None),
                                CurrentApplicationVersionId=self.safe_get(self.current_state,
                                                                          "ApplicationDetail.ApplicationVersionId",
                                                                          None),
                                CloudWatchLoggingOption={
                                    "LogStreamARN": self.safe_get(item, "stream_arn", ""),
                                    "RoleARN": self.safe_get(item, "role_arn", "")
                                })
                        except BotoCoreError as e:
                            self.module.fail_json(msg="add application logging failed: {}".format(e))
                            raise e
                        self.changed = True
                else:
                    self.wait_till_updatable_state()
                    try:
                        self.client.add_application_cloud_watch_logging_option(
                            ApplicationName=self.safe_get(self.module.params, "name", None),
                            CurrentApplicationVersionId=
                            self.safe_get(self.current_state, "ApplicationDetail.ApplicationVersionId", None),
                            CloudWatchLoggingOption={
                                "LogStreamARN": self.safe_get(item, "stream_arn", ""),
                                "RoleARN": self.safe_get(item, "role_arn", "")
                            })
                    except BotoCoreError as e:
                        self.module.fail_json(msg="add application logging failed: {}".format(e))
                        raise e
                    self.changed = True

        if "CloudWatchLoggingOptionDescriptions" in self.safe_get(self.current_state, "ApplicationDetail", {}):
            for item in self.safe_get(self.current_state, "ApplicationDetail.CloudWatchLoggingOptionDescriptions", []):
                if "logs" in self.module.params:
                    matched_desired_logs = [i for i in self.safe_get(self.module.params, "logs", []) if
                                            self.safe_get(i, "stream_arn", "") == self.safe_get(item, "LogStreamARN",
                                                                                                "")]
                    if len(matched_desired_logs) <= 0:
                        self.wait_till_updatable_state()
                        try:
                            self.client.delete_application_cloud_watch_logging_option(
                                ApplicationName=self.safe_get(self.module.params, "name", None),
                                CurrentApplicationVersionId=self.safe_get(self.current_state,
                                                                          "ApplicationDetail.ApplicationVersionId",
                                                                          None),
                                CloudWatchLoggingOptionId=self.safe_get(item, "CloudWatchLoggingOptionId", None))
                        except BotoCoreError as e:
                            self.module.fail_json(msg="delete application logging failed: {}".format(e))
                            raise e
                        self.changed = True

                else:
                    self.wait_till_updatable_state()
                    try:
                        self.client.delete_application_cloud_watch_logging_option(
                            ApplicationName=self.safe_get(self.module.params, "name", None),
                            CurrentApplicationVersionId=self.safe_get(self.current_state,
                                                                      "ApplicationDetail.ApplicationVersionId", None),
                            CloudWatchLoggingOptionId=self.safe_get(item, "CloudWatchLoggingOptionId", None))
                    except BotoCoreError as e:
                        self.module.fail_json(msg="delete application logging failed: {}".format(e))
                        raise e
                    self.changed = True

    def get_current_state(self):
        try:
            self.current_state = self.client.describe_application(
                ApplicationName=self.safe_get(self.module.params, "name", None))
            return "AppFound"
        except ClientError as err:
            if self.safe_get(err.response, "Error.Code", "") == "ResourceNotFoundException":
                return "AppNotFound"
            else:
                self.module.fail_json(msg="unable to obtain current state of application: {}".format(err))
                raise err

    def get_final_state(self):
        try:
            self.current_state = self.client.describe_application(
                ApplicationName=self.safe_get(self.module.params, "name", None))
        except BotoCoreError as e:
            self.module.fail_json(msg="unable to obtain final state of application: {}".format(e))
            raise e

    def wait_till_updatable_state(self):
        wait_complete = time.time() + self.safe_get(self.module.params, "check_timeout", 300)
        while time.time() < wait_complete:
            self.current_state = self.client.describe_application(
                ApplicationName=self.safe_get(self.module.params, "name", None))
            if self.safe_get(self.current_state, "ApplicationDetail.ApplicationStatus", "") in ["READY", "RUNNING"]:
                return
            time.sleep(self.safe_get(self.module.params, "wait_between_check", 5))
        self.module.fail_json(msg="wait for updatable application timeout on %s" % time.asctime())
        raise Exception("wait for updatable state timeout")

    def get_input_configuration(self):
        inputs = []
        for item in self.safe_get(self.module.params, "inputs", []):
            inputs.append(self.get_single_input_configuration(item))

        return inputs

    def get_single_input_configuration(self, item):
        input_item = {
            "NamePrefix": self.safe_get(item, "name_prefix", ""),
            "InputParallelism": {
                "Count": self.safe_get(item, "parallelism", 0)
            },
            "InputSchema": {
                "RecordFormat": {
                    "RecordFormatType": self.safe_get(item, "schema.format.format_type", ""),
                    "MappingParameters": {}
                },
                "RecordColumns": [],
            }
        }

        if self.safe_get(item, "kinesis.input_type", "") == "streams":
            input_item["KinesisStreamsInput"] = {
                "ResourceARN": self.safe_get(item, "kinesis.resource_arn", ""),
                "RoleARN": self.safe_get(item, "kinesis.role_arn", ""),
            }
        elif self.safe_get(item, "kinesis.input_type", "") == "firehose":
            input_item["KinesisFirehoseInput"] = {
                "ResourceARN": self.safe_get(item, "kinesis.resource_arn", ""),
                "RoleARN": self.safe_get(item, "kinesis.role_arn", ""),
            }

        if "pre_processor" in item:
            input_item["InputProcessingConfiguration"] = {}
            input_item["InputProcessingConfiguration"]["InputLambdaProcessor"] = {
                "ResourceARN": self.safe_get(item, "pre_processor.resource_arn", ""),
                "RoleARN": self.safe_get(item, "pre_processor.role_arn", ""),
            }

        if self.safe_get(item, "schema.format.format_type", "") == "JSON":
            input_item["InputSchema"]["RecordFormat"]["MappingParameters"]["JSONMappingParameters"] = {
                "RecordRowPath": self.safe_get(item, "schema.format.json_mapping_row_path", ""),
            }
        elif self.safe_get(item, "schema.format.format_type", "") == "CSV":
            input_item["InputSchema"]["RecordFormat"]["MappingParameters"]["CSVMappingParameters"] = {
                "RecordRowDelimiter": self.safe_get(item, "schema.format.csv_mapping_row_delimiter", ""),
                "RecordColumnDelimiter": self.safe_get(item, "schema.format.csv_mapping_column_delimiter", ""),
            }

        for column in self.safe_get(item, "schema.columns", []):
            input_item["InputSchema"]["RecordColumns"].append({
                "Mapping": self.safe_get(column, "mapping", ""),
                "Name": self.safe_get(column, "name", ""),
                "SqlType": self.safe_get(column, "column_type", ""),
            })
        return input_item

    def get_output_configuration(self):
        outputs = []

        for item in self.safe_get(self.module.params, "outputs", []):
            outputs.append(self.get_single_output_configuration(item))

        return outputs

    def get_single_output_configuration(self, item):
        output = {
            "Name": self.safe_get(item, "name", None),
            "DestinationSchema": {
                "RecordFormatType": self.safe_get(item, "format_type", "")
            }
        }
        if self.safe_get(item, "output_type", "") == "streams":
            output["KinesisStreamsOutput"] = {
                "ResourceARN": self.safe_get(item, "resource_arn", ""),
                "RoleARN": self.safe_get(item, "role_arn", ""),
            }
        elif self.safe_get(item, "output_type", "") == "firehose":
            output["KinesisFirehoseOutput"] = {
                "ResourceARN": self.safe_get(item, "resource_arn", ""),
                "RoleARN": self.safe_get(item, "role_arn", ""),
            }
        elif self.safe_get(item, "output_type", "") == "lambda":
            output["LambdaOutput"] = {
                "ResourceARN": self.safe_get(item, "resource_arn", ""),
                "RoleARN": self.safe_get(item, "role_arn", ""),
            }

        return output

    def get_log_configuration(self):
        logs = []
        if "logs" in self.module.params and self.module.params["logs"] != None:
            for item in self.module.params["logs"]:
                logs.append({
                    "LogStreamARN": self.safe_get(item, "stream_arn", ""),
                    "RoleARN": self.safe_get(item, "role_arn", ""),
                })

        return logs

    def get_input_start_configuration(self):
        input_config = []

        item = {
            "Id": "1.1",
            "InputStartingPositionConfiguration": {}
        }

        item["InputStartingPositionConfiguration"]["InputStartingPosition"] = self.safe_get(self.module.params,
                                                                                            "starting_position", "")

        input_config.append(item)

        return input_config

    def get_app_update_configuration(self):
        update_config = {}

        if self.safe_get(self.module.params, "code", None) != self.safe_get(self.current_state,
                                                                            "ApplicationDetail.ApplicationCode", None):
            update_config["ApplicationCodeUpdate"] = self.safe_get(self.module.params, "code", None)

        if self.is_input_configuration_change():
            update_config["InputUpdates"] = self.get_input_update_configuration()

        if self.is_output_configuration_change():
            update_config["OutputUpdates"] = self.get_output_update_configuration()

        if self.is_log_configuration_changed():
            update_config["CloudWatchLoggingOptionUpdates"] = self.get_log_update_configuration()

        return update_config

    def is_app_updatable_state_changed(self):
        return self.safe_get(self.module.params, "code", "") != self.safe_get(self.current_state,
                                                                              "ApplicationDetail.ApplicationCode",
                                                                              "") or self.is_input_configuration_change() or self.is_output_configuration_change() or self.is_log_configuration_changed()

    def is_output_configuration_change(self):
        for output in self.safe_get(self.module.params, "outputs", []):
            matched_describe_outputs = [i for i in
                                        self.safe_get(self.current_state, "ApplicationDetail.OutputDescriptions", []) if
                                        self.safe_get(i, "Name", "") == self.safe_get(output, "name", "")]
            if len(matched_describe_outputs) != 1:
                continue
            describe_output = matched_describe_outputs[0]

            output_type = self.safe_get(output, "output_type", "")

            if output_type == "streams":
                if "KinesisStreamsOutputDescription" not in describe_output:
                    return True
                if output["resource_arn"] != self.safe_get(describe_output,
                                                           "KinesisStreamsOutputDescription.ResourceARN", ""):
                    return True
                if output["role_arn"] != self.safe_get(describe_output, "KinesisStreamsOutputDescription.RoleARN", ""):
                    return True

            if output_type == "firehose":
                if "KinesisFirehoseOutputDescription" not in describe_output:
                    return True
                if output["resource_arn"] != self.safe_get(describe_output,
                                                           "KinesisFirehoseOutputDescription.ResourceARN", ""):
                    return True
                if output["role_arn"] != self.safe_get(describe_output, "KinesisFirehoseOutputDescription.RoleARN", ""):
                    return True

            if output_type == "lambda":
                if "LambdaOutputDescription" not in describe_output:
                    return True
                if output["resource_arn"] != self.safe_get(describe_output, "LambdaOutputDescription.ResourceARN", ""):
                    return True
                if output["role_arn"] != self.safe_get(describe_output, "LambdaOutputDescription.RoleARN", ""):
                    return True

            if self.safe_get(output, "format_type", "") != self.safe_get(describe_output,
                                                                         "DestinationSchema.RecordFormatType", ""):
                return True

        return False

    def is_input_configuration_change(self):
        for input in self.safe_get(self.module.params, "inputs", []):
            matched_describe_inputs = [i for i in
                                       self.safe_get(self.current_state, "ApplicationDetail.InputDescriptions", []) if
                                       self.safe_get(i, "NamePrefix", "") == self.safe_get(input, "name_prefix", "")]
            if len(matched_describe_inputs) != 1:
                return True
            describe_input = matched_describe_inputs[0]

            if self.safe_get(input, "schema.format.format_type", "") != self.safe_get(describe_input,
                                                                                      "InputSchema.RecordFormat.RecordFormatType",
                                                                                      ""):
                return True

            if self.safe_get(input, "schema.format.format_type", "") == "JSON":
                if self.safe_get(input, "schema.format.json_mapping_row_path", "") != \
                        self.safe_get(describe_input,
                                      "InputSchema.RecordFormat.MappingParameters.JSONMappingParameters.RecordRowPath",
                                      ""):
                    return True

            if self.safe_get(input, "schema.format.format_type", "") == "CSV":
                if self.safe_get(input, "schema.format.csv_mapping_row_delimiter", "") != \
                        self.safe_get(describe_input,
                                      "InputSchema.RecordFormat.MappingParameters.CSVMappingParameters.RecordRowDelimiter",
                                      ""):
                    return True
                if self.safe_get(input, "schema.format.csv_mapping_column_delimiter", "") != \
                        self.safe_get(describe_input,
                                      "InputSchema.RecordFormat.MappingParameters.CSVMappingParameters.RecordColumnDelimiter",
                                      ""):
                    return True

            if len(self.safe_get(input, "schema.columns", [])) != len(
                    self.safe_get(describe_input, "InputSchema.RecordColumns", [])):
                return True

            for col in self.safe_get(input, "schema.columns", []):
                matched_describe_cols = [i for i in self.safe_get(describe_input, "InputSchema.RecordColumns", []) if
                                         self.safe_get(i, "Name", "") == self.safe_get(col, "name", "")]
                if len(matched_describe_cols) != 1:
                    return True
                describe_col = matched_describe_cols[0]
                if self.safe_get(describe_col, "SqlType", "") != self.safe_get(col, "column_type", "") or self.safe_get(
                        describe_col, "Mapping", "") != self.safe_get(col,
                                                                      "mapping", ""):
                    return True

            if self.safe_get(input, "parallelism", 0) != self.safe_get(describe_input, "InputParallelism.Count", 0):
                return True

            input_type = self.safe_get(input, "kinesis.input_type", "")
            if input_type == "streams":
                if "KinesisStreamsInputDescription" in describe_input:
                    if self.safe_get(input, "kinesis.resource_arn", "") != self.safe_get(describe_input,
                                                                                         "KinesisStreamsInputDescription.ResourceARN",
                                                                                         ""):
                        return True
                    if self.safe_get(input, "kinesis.role_arn", "") != self.safe_get(describe_input,
                                                                                     "KinesisStreamsInputDescription.RoleARN",
                                                                                     ""):
                        return True

            if input_type == "firehose":
                if "KinesisFirehoseInputDescription" in describe_input:
                    if self.safe_get(input, "kinesis.resource_arn", "") != self.safe_get(describe_input,
                                                                                         "KinesisFirehoseInputDescription.ResourceARN",
                                                                                         ""):
                        return True
                    if self.safe_get(input, "kinesis.role_arn", "") != self.safe_get(describe_input,
                                                                                     "KinesisFirehoseInputDescription.RoleARN",
                                                                                     ""):
                        return True

            if "pre_processor" in input:
                if "InputProcessingConfigurationDescription" not in describe_input:
                    return True
                if self.safe_get(input, "pre_processor.resource_arn", "") != \
                        self.safe_get(describe_input,
                                      "InputProcessingConfigurationDescription.InputLambdaProcessorDescription.ResourceARN",
                                      ""):
                    return True
                if self.safe_get(input, "pre_processor.role_arn", "") != \
                        self.safe_get(describe_input,
                                      "InputProcessingConfigurationDescription.InputLambdaProcessorDescription.RoleARN",
                                      ""):
                    return True

        return False

    def is_log_configuration_changed(self):
        if "logs" not in self.module.params or self.module.params["logs"] == None:
            return False

        for log in self.safe_get(self.module.params, "logs", []):
            matched_describe_logs = [i for i in
                                     self.safe_get(self.current_state, "ApplicationDetail.CloudWatchLoggingOptionDescriptions", []) if
                                     self.safe_get(i, "LogStreamARN", "") == self.safe_get(log, "stream_arn", "")]
            if len(matched_describe_logs) != 1:
                continue
            describe_log = matched_describe_logs[0]

            if self.safe_get(log, "role_arn", "") != self.safe_get(describe_log, "RoleARN", ""):
                return True

        return False

    def get_input_update_configuration(self):
        expected = []
        for item in self.safe_get(self.module.params, "inputs", []):
            describe_inputs = self.safe_get(self.current_state, "ApplicationDetail.InputDescriptions", [])

            input_item = {
                "InputId": self.safe_get(describe_inputs[0], "InputId", None),
                "NamePrefixUpdate": self.safe_get(item, "name_prefix", None),
                "InputParallelismUpdate": {
                    "CountUpdate": self.safe_get(item, "parallelism", 0)
                },
                "InputSchemaUpdate": {
                    "RecordFormatUpdate": {
                        "RecordFormatType": self.safe_get(item, "schema.format.format_type", ""),
                        "MappingParameters": {}
                    },
                    "RecordColumnUpdates": [],
                }
            }

            input_type = self.safe_get(item, "kinesis.input_type", "")

            if input_type == "streams":
                input_item["KinesisStreamsInputUpdate"] = {
                    "ResourceARNUpdate": self.safe_get(item, "kinesis.resource_arn", ""),
                    "RoleARNUpdate": self.safe_get(item, "kinesis.role_arn", ""),
                }
            elif input_type == "firehose":
                input_item["KinesisFirehoseInputUpdate"] = {
                    "ResourceARNUpdate": self.safe_get(item, "kinesis.resource_arn", ""),
                    "RoleARNUpdate": self.safe_get(item, "kinesis.role_arn", ""),
                }

            if "pre_processor" in item:
                input_item["InputProcessingConfigurationUpdate"] = {}
                input_item["InputProcessingConfigurationUpdate"]["InputLambdaProcessorUpdate"] = {
                    "ResourceARNUpdate": self.safe_get(item, "pre_processor.resource_arn", ""),
                    "RoleARNUpdate": self.safe_get(item, "pre_processor.role_arn", ""),
                }

            format_type = self.safe_get(item, "schema.format.format_type", "")
            if format_type == "JSON":
                input_item["InputSchemaUpdate"]["RecordFormatUpdate"]["MappingParameters"]["JSONMappingParameters"] = {
                    "RecordRowPath": self.safe_get(item, "schema.format.json_mapping_row_path", ""),
                }
            elif format_type == "CSV":
                input_item["InputSchemaUpdate"]["RecordFormatUpdate"]["MappingParameters"]["CSVMappingParameters"] = {
                    "RecordRowDelimiter": self.safe_get(item, "schema.format.csv_mapping_row_delimiter", ""),
                    "RecordColumnDelimiter": self.safe_get(item, "schema.format.csv_mapping_column_delimiter", ""),
                }

            for column in self.safe_get(item, "schema.columns", []):
                input_item["InputSchemaUpdate"]["RecordColumnUpdates"].append({
                    "Mapping": self.safe_get(column, "mapping", ""),
                    "Name": self.safe_get(column, "name", ""),
                    "SqlType": self.safe_get(column, "column_type", ""),
                })
            expected.append(input_item)

        return expected

    def get_output_update_configuration(self):
        expected = []

        for item in self.safe_get(self.module.params, "outputs", []):
            matched_describe_outputs = [i for i in self.safe_get(self.current_state, "ApplicationDetail.OutputDescriptions", []) if
                                        self.safe_get(i, "Name", "") == self.safe_get(item, "name", "")]

            if len(matched_describe_outputs) != 1:
                continue

            output = {
                "OutputId": self.safe_get(matched_describe_outputs[0], "OutputId", None),
                "NameUpdate": self.safe_get(item, "name", None),
                "DestinationSchemaUpdate": {
                    "RecordFormatType": self.safe_get(item, "format_type", "")
                }
            }
            output_type = self.safe_get(item, "output_type", "")
            if output_type == "streams":
                output["KinesisStreamsOutputUpdate"] = {
                    "ResourceARNUpdate": self.safe_get(item, "resource_arn", ""),
                    "RoleARNUpdate": self.safe_get(item, "role_arn", ""),
                }
            elif output_type == "firehose":
                output["KinesisFirehoseOutputUpdate"] = {
                    "ResourceARNUpdate": self.safe_get(item, "resource_arn", ""),
                    "RoleARNUpdate": self.safe_get(item, "role_arn", ""),
                }
            elif output_type == "lambda":
                output["LambdaOutputUpdate"] = {
                    "ResourceARNUpdate": self.safe_get(item, "resource_arn", ""),
                    "RoleARNUpdate": self.safe_get(item, "role_arn", ""),
                }
            expected.append(output)

        return expected

    def get_log_update_configuration(self):
        expected = []

        for item in self.safe_get(self.module.params, "logs", []):
            matched_describe_logs = [i for i in
                                     self.safe_get(self.current_state, "ApplicationDetail.CloudWatchLoggingOptionDescriptions", []) if
                                     self.safe_get(i, "LogStreamARN", "") == self.safe_get(item, "stream_arn", "")]

            if len(matched_describe_logs) != 1:
                continue

            log = {
                "CloudWatchLoggingOptionId": self.safe_get(matched_describe_logs[0], "CloudWatchLoggingOptionId", None),
                "LogStreamARNUpdate": self.safe_get(item, "stream_arn", ""),
                "RoleARNUpdate": self.safe_get(item, "role_arn", "")
            }
            expected.append(log)

        return expected

    def safe_get(self, dct, path, default_value):
        nested_keys = path.split(".")
        try:
            actual = dct
            for k in nested_keys:
                actual = actual[k]
            return actual
        except KeyError:
            return default_value


def main():
    module = AnsibleModule(
        argument_spec=KinesisDataAnalyticsApp._define_module_argument_spec(),
        supports_check_mode=False
    )

    kda_app = KinesisDataAnalyticsApp(module)
    kda_app.process_request()


# from ansible.module_utils.basic import *  # pylint: disable=W0614
if __name__ == "__main__":
    main()
