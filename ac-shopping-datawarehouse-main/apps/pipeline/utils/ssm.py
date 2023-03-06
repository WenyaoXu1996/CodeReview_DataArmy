import base64
import json
import boto3
from botocore.exceptions import ClientError

AWS_REGION = "ap-southeast-2"


ssm_client = boto3.client("ssm", region_name=AWS_REGION)


def get_parameter_value(parameter_name):

    try:
        response = ssm_client.get_parameter(Name=parameter_name)
        parameter_value = response["Parameter"]["Value"]
        return parameter_value
    except Exception as e:
        print(e)
        raise


def create_parameter_value(parameter_name):

    if (
        ssm_client.describe_parameters(
            ParameterFilters=[{"Key": "Name", "Values": [parameter_name]}]
        )["Parameters"]
        == []
    ):
        try:
            ssm_client.put_parameter(
                Name=parameter_name, Value="2000-01-01", Type="String"
            )
            print(
                "Parameter {} is created successfully with value {}".format(
                    parameter_name, "2000-01-01"
                )
            )
        except Exception as e:
            print(e)
            raise


def update_parameter_value(parameter_name, parameter_value):
    try:
        ssm_client.put_parameter(
            Name=parameter_name, Value=parameter_value, Type="String", Overwrite=True
        )
    except Exception as e:
        print(e)
        raise

