import logging

from moto import mock_dynamodb2


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
            "status": {"S": "Failed, will retry"},
            "attempts": {"S": "1"},
        },
    )
    dois = dynamodb_class.retrieve_doi_items_from_database("test_dois")
    assert dois == [
        {
            "doi": "111.1/1111",
            "status": "Failed, will retry",
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
            "status": {"S": "Failed, will retry"},
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
            "status": {"S": "Failed, will retry"},
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
            "status": {"S": "Failed, will retry"},
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
        "status": {"S": "Failed, will retry"},
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
        "status": {"S": "Failed, will retry"},
    }


@mock_dynamodb2
def test_dynamodb_update_doi_item_status_in_database_invalid_enum(
    caplog, dynamodb_class
):
    with caplog.at_level(logging.DEBUG):
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
                "status": {"S": "FAILED"},
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
            "status": {"S": "FAILED"},
        }
        dynamodb_class.update_doi_item_status_in_database(
            "test_dois", "111.1/1111", "Processing"
        )
        assert (
            "Invalid status_enum: Processing, 111.1/1111 not updated in the database."
            in caplog.text
        )
        updated_item = dynamodb_class.client.get_item(
            TableName="test_dois",
            Key={"doi": {"S": "111.1/1111"}},
        )
        assert updated_item["Item"] == {
            "attempts": {"S": "1"},
            "doi": {"S": "111.1/1111"},
            "status": {"S": "FAILED"},
        }


@mock_dynamodb2
def test_dynamodb_update_doi_item_status_in_database_valid_enum(dynamodb_class):
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
            "status": {"S": "Failed, will retry"},
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
        "status": {"S": "Failed, will retry"},
    }
    update_response = dynamodb_class.update_doi_item_status_in_database(
        "test_dois", "111.1/1111", "PROCESSING"
    )
    assert update_response["ResponseMetadata"]["HTTPStatusCode"] == 200
    updated_item = dynamodb_class.client.get_item(
        TableName="test_dois",
        Key={"doi": {"S": "111.1/1111"}},
    )
    assert updated_item["Item"] == {
        "attempts": {"S": "1"},
        "doi": {"S": "111.1/1111"},
        "status": {"S": "PROCESSING"},
    }
