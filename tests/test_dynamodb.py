from http import HTTPStatus

from awd.status import Status


def test_dynamodb_add_doi_item_to_database(mocked_dynamodb, dynamodb_instance):
    add_response = dynamodb_instance.add_doi_item_to_database("test_dois", "222.2/2222")
    assert add_response["ResponseMetadata"]["HTTPStatusCode"] == HTTPStatus.OK


def test_dynamodb_retrieve_doi_items_from_database(mocked_dynamodb, dynamodb_instance):
    dynamodb_instance.client.put_item(
        TableName="test_dois",
        Item={
            "doi": {"S": "111.1/1111"},
            "status": {"S": str(Status.UNPROCESSED.value)},
            "attempts": {"S": "1"},
            "last_modified": {"S": "2022-01-28 10:28:53"},
        },
    )
    dois = dynamodb_instance.retrieve_doi_items_from_database("test_dois")
    assert dois == [
        {
            "doi": "111.1/1111",
            "status": str(Status.UNPROCESSED.value),
            "attempts": "1",
            "last_modified": "2022-01-28 10:28:53",
        }
    ]


def test_dynamodb_retry_threshold_exceeded_false(mocked_dynamodb, dynamodb_instance):
    dynamodb_instance.client.put_item(
        TableName="test_dois",
        Item={
            "doi": {"S": "111.1/1111"},
            "status": {"S": str(Status.UNPROCESSED.value)},
            "attempts": {"S": "1"},
            "last_modified": {"S": "2022-01-28 10:28:53"},
        },
    )
    validation_status = dynamodb_instance.attempts_exceeded(
        "test_dois", "111.1/1111", "10"
    )
    assert validation_status is False


def test_dynamodb_retry_threshold_exceeded_true(mocked_dynamodb, dynamodb_instance):
    dynamodb_instance.client.put_item(
        TableName="test_dois",
        Item={
            "doi": {"S": "111.1/1111"},
            "status": {"S": str(Status.UNPROCESSED.value)},
            "attempts": {"S": "10"},
            "last_modified": {"S": "2022-01-28 10:28:53"},
        },
    )
    validation_status = dynamodb_instance.attempts_exceeded(
        "test_dois", "111.1/1111", "10"
    )
    assert validation_status is True


def test_dynamodb_update_doi_item_attempts_in_database(
    mocked_dynamodb, dynamodb_instance
):
    dynamodb_instance.client.put_item(
        TableName="test_dois",
        Item={
            "doi": {"S": "111.1/1111"},
            "status": {"S": str(Status.UNPROCESSED.value)},
            "attempts": {"S": "1"},
            "last_modified": {"S": "2022-01-28 10:28:53"},
        },
    )
    existing_item = dynamodb_instance.client.get_item(
        TableName="test_dois",
        Key={"doi": {"S": "111.1/1111"}},
    )
    assert existing_item["Item"]["attempts"]["S"] == "1"
    assert existing_item["Item"]["doi"]["S"] == "111.1/1111"
    assert existing_item["Item"]["status"]["S"] == str(Status.UNPROCESSED.value)
    assert existing_item["Item"]["last_modified"]["S"] == "2022-01-28 10:28:53"
    update_response = dynamodb_instance.update_doi_item_attempts_in_database(
        "test_dois", "111.1/1111"
    )
    assert update_response["ResponseMetadata"]["HTTPStatusCode"] == HTTPStatus.OK
    updated_item = dynamodb_instance.client.get_item(
        TableName="test_dois",
        Key={"doi": {"S": "111.1/1111"}},
    )
    assert updated_item["Item"]["attempts"]["S"] == "2"
    assert updated_item["Item"]["doi"]["S"] == "111.1/1111"
    assert updated_item["Item"]["status"]["S"] == str(Status.UNPROCESSED.value)
    assert updated_item["Item"]["last_modified"]["S"] != "2022-01-28 10:28:53"


def test_dynamodb_update_doi_item_status_in_database(mocked_dynamodb, dynamodb_instance):
    dynamodb_instance.client.put_item(
        TableName="test_dois",
        Item={
            "doi": {"S": "111.1/1111"},
            "status": {"S": str(Status.UNPROCESSED.value)},
            "attempts": {"S": "1"},
            "last_modified": {"S": "2022-01-28 10:28:53"},
        },
    )
    existing_item = dynamodb_instance.client.get_item(
        TableName="test_dois",
        Key={"doi": {"S": "111.1/1111"}},
    )
    assert existing_item["Item"]["attempts"]["S"] == "1"
    assert existing_item["Item"]["doi"]["S"] == "111.1/1111"
    assert existing_item["Item"]["status"]["S"] == str(Status.UNPROCESSED.value)
    assert existing_item["Item"]["last_modified"]["S"] == "2022-01-28 10:28:53"
    update_response = dynamodb_instance.update_doi_item_status_in_database(
        "test_dois", "111.1/1111", Status.MESSAGE_SENT.value
    )
    assert update_response["ResponseMetadata"]["HTTPStatusCode"] == HTTPStatus.OK
    updated_item = dynamodb_instance.client.get_item(
        TableName="test_dois",
        Key={"doi": {"S": "111.1/1111"}},
    )
    assert updated_item["Item"]["attempts"]["S"] == "1"
    assert updated_item["Item"]["doi"]["S"] == "111.1/1111"
    assert updated_item["Item"]["status"]["S"] == str(Status.MESSAGE_SENT.value)
    assert updated_item["Item"]["last_modified"]["S"] != "2022-01-28 10:28:53"
