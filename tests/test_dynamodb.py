from moto import mock_dynamodb2

from awd import dynamodb


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
        "test_dois", "111.1/1111", "Processing"
    )
    assert update_response["ResponseMetadata"]["HTTPStatusCode"] == 200
    updated_item = dynamodb_class.client.get_item(
        TableName="test_dois",
        Key={"doi": {"S": "111.1/1111"}},
    )
    assert updated_item["Item"] == {
        "attempts": {"S": "1"},
        "doi": {"S": "111.1/1111"},
        "status": {"S": "Processing"},
    }


def test_doi_to_be_added_true():
    doi_items = [{"doi": "111.1/111"}]
    validation_status = dynamodb.doi_to_be_added("222.2/2222", doi_items)
    assert validation_status is True


def test_doi_to_be_added_false():
    doi_items = [{"doi": "111.1/1111"}]
    validation_status = dynamodb.doi_to_be_added("111.1/1111", doi_items)
    assert validation_status is False


def test_doi_to_be_retried_true():
    doi_items = [{"doi": "111.1/111", "status": "Failed, will retry"}]
    validation_status = dynamodb.doi_to_be_retried("111.1/111", doi_items)
    assert validation_status is True


def test_doi_to_be_retried_false():
    doi_items = [{"doi": "111.1/111", "status": "Success"}]
    validation_status = dynamodb.doi_to_be_retried("111.1/111", doi_items)
    assert validation_status is False
