# import os

# import boto3
# import pytest
# from botocore.exceptions import ClientError
# from moto.core import set_initial_no_auth_action_count
from moto import mock_dynamodb2

# from awd.dynamodb import DynamoDB
from awd.status import Status


@mock_dynamodb2
def test_dynamodb_add_doi_item_to_database(dynamodb_class):
    dynamodb_class.client.create_table(
        TableName="test_dois",
        KeySchema=[
            {"AttributeName": "doi", "KeyType": "HASH"},
        ],
        AttributeDefinitions=[
            {"AttributeName": "doi", "AttributeType": "S"},
        ],
    )
    dynamodb_class.client.describe_table(TableName="test_dois")
    add_response = dynamodb_class.add_doi_item_to_database("test_dois", "222.2/2222")
    assert add_response["ResponseMetadata"]["HTTPStatusCode"] == 200


@mock_dynamodb2
def test_check_read_permissions_success(
    dynamodb_class,
):
    dynamodb_class.client.create_table(
        TableName="test_dois",
        KeySchema=[
            {"AttributeName": "doi", "KeyType": "HASH"},
        ],
        AttributeDefinitions=[
            {"AttributeName": "doi", "AttributeType": "S"},
        ],
    )
    dynamodb_class.client.describe_table(TableName="test_dois")
    result = dynamodb_class.check_read_permissions("test_dois")
    assert result == "Read permissions confirmed for table: test_dois"


# While this function works as expected against stage AWS, the type of errors
# triggered by the moto mocks of other AWS services do not seem to be triggered
# for the moto mock of DynamoDB. Leaving this commented out for potential future
# reference.
#
# @mock_dynamodb2
# @set_initial_no_auth_action_count(2)
# def test_check_read_permissions_raises_error_if_no_permission(test_aws_user):
#     dynamodb_client = boto3.client(
#         "dynamodb",
#         region_name="us-east-1",
#     )
#     dynamodb_client.create_table(
#         TableName="test_dois",
#         KeySchema=[
#             {"AttributeName": "doi", "KeyType": "HASH"},
#         ],
#         AttributeDefinitions=[
#             {"AttributeName": "doi", "AttributeType": "S"},
#         ],
#     )
#     os.environ["AWS_ACCESS_KEY_ID"] = test_aws_user["AccessKeyId"]
#     os.environ["AWS_SECRET_ACCESS_KEY"] = test_aws_user["SecretAccessKey"]
#     boto3.setup_default_session()
#     dynamodb_class = DynamoDB()
#     with pytest.raises(ClientError) as e:
#         dynamodb_class.check_read_permissions("test_dois")
#     assert (
#         "User: arn:aws:iam::123456789012:user/test-user is not authorized to perform: "
#         "dynamodb:Scan"
#     ) in str(e.value)


@mock_dynamodb2
def test_check_write_permissions_success(dynamodb_class):
    dynamodb_class.client.create_table(
        TableName="test_dois",
        KeySchema=[
            {"AttributeName": "doi", "KeyType": "HASH"},
        ],
        AttributeDefinitions=[
            {"AttributeName": "doi", "AttributeType": "S"},
        ],
    )
    result = dynamodb_class.check_write_permissions("test_dois")

    assert result == "Write permissions confirmed for table: test_dois"
    assert "Item" not in (
        dynamodb_class.client.get_item(
            TableName="test_dois",
            Key={"doi": {"S": "SmokeTest"}},
        )
    )


# While this function works as expected against stage AWS, the type of errors
# triggered by the moto mocks of other AWS services do not seem to be triggered
# for the moto mock of DynamoDB. Leaving this commented out for potential future
# reference.
#
# @mock_dynamodb2
# @set_initial_no_auth_action_count(2)
# def test_check_write_permissions_raises_error_if_no_permission(test_aws_user):
#     dynamodb_client = boto3.client(
#         "dynamodb",
#         region_name="us-east-1",
#     )
#     dynamodb_client.create_table(
#         TableName="test_dois",
#         KeySchema=[
#             {"AttributeName": "doi", "KeyType": "HASH"},
#         ],
#         AttributeDefinitions=[
#             {"AttributeName": "doi", "AttributeType": "S"},
#         ],
#     )
#     os.environ["AWS_ACCESS_KEY_ID"] = test_aws_user["AccessKeyId"]
#     os.environ["AWS_SECRET_ACCESS_KEY"] = test_aws_user["SecretAccessKey"]
#     boto3.setup_default_session()
#     dynamodb_class = DynamoDB()
#     with pytest.raises(ClientError) as e:
#         dynamodb_class.check_write_permissions("test_dois")
#     assert (
#         "User: arn:aws:iam::123456789012:user/test-user is not authorized to perform: "
#         "dynamodb:PutItem"
#     ) in str(e.value)


@mock_dynamodb2
def test_dynamodb_retrieve_doi_items_from_database(dynamodb_class):
    dynamodb_class.client.create_table(
        TableName="test_dois",
        KeySchema=[
            {"AttributeName": "doi", "KeyType": "HASH"},
        ],
        AttributeDefinitions=[
            {"AttributeName": "doi", "AttributeType": "S"},
        ],
    )
    dynamodb_class.client.put_item(
        TableName="test_dois",
        Item={
            "doi": {"S": "111.1/1111"},
            "status": {"S": str(Status.FAILED.value)},
            "attempts": {"S": "1"},
        },
    )
    dois = dynamodb_class.retrieve_doi_items_from_database("test_dois")
    assert dois == [
        {
            "doi": "111.1/1111",
            "status": str(Status.FAILED.value),
            "attempts": "1",
        }
    ]


@mock_dynamodb2
def test_dynamodb_retry_threshold_exceeded_false(dynamodb_class):
    dynamodb_class.client.create_table(
        TableName="test_dois",
        KeySchema=[
            {"AttributeName": "doi", "KeyType": "HASH"},
        ],
        AttributeDefinitions=[
            {"AttributeName": "doi", "AttributeType": "S"},
        ],
    )
    dynamodb_class.client.put_item(
        TableName="test_dois",
        Item={
            "doi": {"S": "111.1/1111"},
            "status": {"S": str(Status.FAILED.value)},
            "attempts": {"S": "1"},
        },
    )
    validation_status = dynamodb_class.retry_attempts_exceeded(
        "test_dois", "111.1/1111", "10"
    )
    assert validation_status is False


@mock_dynamodb2
def test_dynamodb_retry_threshold_exceeded_true(dynamodb_class):
    dynamodb_class.client.create_table(
        TableName="test_dois",
        KeySchema=[
            {"AttributeName": "doi", "KeyType": "HASH"},
        ],
        AttributeDefinitions=[
            {"AttributeName": "doi", "AttributeType": "S"},
        ],
    )
    dynamodb_class.client.put_item(
        TableName="test_dois",
        Item={
            "doi": {"S": "111.1/1111"},
            "status": {"S": str(Status.FAILED.value)},
            "attempts": {"S": "10"},
        },
    )
    validation_status = dynamodb_class.retry_attempts_exceeded(
        "test_dois", "111.1/1111", "10"
    )
    assert validation_status is True


@mock_dynamodb2
def test_dynamodb_update_doi_item_attempts_in_database(dynamodb_class):
    dynamodb_class.client.create_table(
        TableName="test_dois",
        KeySchema=[
            {"AttributeName": "doi", "KeyType": "HASH"},
        ],
        AttributeDefinitions=[
            {"AttributeName": "doi", "AttributeType": "S"},
        ],
    )
    dynamodb_class.client.put_item(
        TableName="test_dois",
        Item={
            "doi": {"S": "111.1/1111"},
            "status": {"S": str(Status.FAILED.value)},
            "attempts": {"S": "1"},
        },
    )
    existing_item = dynamodb_class.client.get_item(
        TableName="test_dois",
        Key={"doi": {"S": "111.1/1111"}},
    )
    assert existing_item["Item"] == {
        "attempts": {"S": "1"},
        "doi": {"S": "111.1/1111"},
        "status": {"S": str(Status.FAILED.value)},
    }
    update_response = dynamodb_class.update_doi_item_attempts_in_database(
        "test_dois", "111.1/1111"
    )
    assert update_response["ResponseMetadata"]["HTTPStatusCode"] == 200
    updated_item = dynamodb_class.client.get_item(
        TableName="test_dois",
        Key={"doi": {"S": "111.1/1111"}},
    )
    assert updated_item["Item"] == {
        "attempts": {"S": "2"},
        "doi": {"S": "111.1/1111"},
        "status": {"S": str(Status.FAILED.value)},
    }


@mock_dynamodb2
def test_dynamodb_update_doi_item_status_in_database(dynamodb_class):
    dynamodb_class.client.create_table(
        TableName="test_dois",
        KeySchema=[
            {"AttributeName": "doi", "KeyType": "HASH"},
        ],
        AttributeDefinitions=[
            {"AttributeName": "doi", "AttributeType": "S"},
        ],
    )
    dynamodb_class.client.put_item(
        TableName="test_dois",
        Item={
            "doi": {"S": "111.1/1111"},
            "status": {"S": str(Status.FAILED.value)},
            "attempts": {"S": "1"},
        },
    )
    existing_item = dynamodb_class.client.get_item(
        TableName="test_dois",
        Key={"doi": {"S": "111.1/1111"}},
    )
    assert existing_item["Item"] == {
        "attempts": {"S": "1"},
        "doi": {"S": "111.1/1111"},
        "status": {"S": str(Status.FAILED.value)},
    }
    update_response = dynamodb_class.update_doi_item_status_in_database(
        "test_dois", "111.1/1111", Status.PROCESSING.value
    )
    assert update_response["ResponseMetadata"]["HTTPStatusCode"] == 200
    updated_item = dynamodb_class.client.get_item(
        TableName="test_dois",
        Key={"doi": {"S": "111.1/1111"}},
    )
    assert updated_item["Item"] == {
        "attempts": {"S": "1"},
        "doi": {"S": "111.1/1111"},
        "status": {"S": str(Status.PROCESSING.value)},
    }
