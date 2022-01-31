from awd.status import Status


def test_dynamodb_add_doi_item_to_database(mocked_dynamodb, dynamodb_class):
    add_response = dynamodb_class.add_doi_item_to_database("test_dois", "222.2/2222")
    assert add_response["ResponseMetadata"]["HTTPStatusCode"] == 200


def test_check_read_permissions_success(
    mocked_dynamodb,
    dynamodb_class,
):

    result = dynamodb_class.check_read_permissions("test_dois")
    assert result == "Read permissions confirmed for table: test_dois"


def test_check_write_permissions_success(mocked_dynamodb, dynamodb_class):
    result = dynamodb_class.check_write_permissions("test_dois")
    assert result == "Write permissions confirmed for table: test_dois"
    assert "Item" not in (
        dynamodb_class.client.get_item(
            TableName="test_dois",
            Key={"doi": {"S": "SmokeTest"}},
        )
    )


def test_dynamodb_retrieve_doi_items_from_database(mocked_dynamodb, dynamodb_class):
    dynamodb_class.client.put_item(
        TableName="test_dois",
        Item={
            "doi": {"S": "111.1/1111"},
            "status": {"S": str(Status.FAILED.value)},
            "attempts": {"S": "1"},
            "last_modified": {"S": "2022-01-28 10:28:53"},
        },
    )
    dois = dynamodb_class.retrieve_doi_items_from_database("test_dois")
    assert dois == [
        {
            "doi": "111.1/1111",
            "status": str(Status.FAILED.value),
            "attempts": "1",
            "last_modified": "2022-01-28 10:28:53",
        }
    ]


def test_dynamodb_retry_threshold_exceeded_false(mocked_dynamodb, dynamodb_class):
    dynamodb_class.client.put_item(
        TableName="test_dois",
        Item={
            "doi": {"S": "111.1/1111"},
            "status": {"S": str(Status.FAILED.value)},
            "attempts": {"S": "1"},
            "last_modified": {"S": "2022-01-28 10:28:53"},
        },
    )
    validation_status = dynamodb_class.attempts_exceeded(
        "test_dois", "111.1/1111", "10"
    )
    assert validation_status is False


def test_dynamodb_retry_threshold_exceeded_true(mocked_dynamodb, dynamodb_class):
    dynamodb_class.client.put_item(
        TableName="test_dois",
        Item={
            "doi": {"S": "111.1/1111"},
            "status": {"S": str(Status.FAILED.value)},
            "attempts": {"S": "10"},
            "last_modified": {"S": "2022-01-28 10:28:53"},
        },
    )
    validation_status = dynamodb_class.attempts_exceeded(
        "test_dois", "111.1/1111", "10"
    )
    assert validation_status is True


def test_dynamodb_update_doi_item_attempts_in_database(mocked_dynamodb, dynamodb_class):
    dynamodb_class.client.put_item(
        TableName="test_dois",
        Item={
            "doi": {"S": "111.1/1111"},
            "status": {"S": str(Status.FAILED.value)},
            "attempts": {"S": "1"},
            "last_modified": {"S": "2022-01-28 10:28:53"},
        },
    )
    existing_item = dynamodb_class.client.get_item(
        TableName="test_dois",
        Key={"doi": {"S": "111.1/1111"}},
    )
    assert existing_item["Item"]["attempts"]["S"] == "1"
    assert existing_item["Item"]["doi"]["S"] == "111.1/1111"
    assert existing_item["Item"]["status"]["S"] == str(Status.FAILED.value)
    assert existing_item["Item"]["last_modified"]["S"] == "2022-01-28 10:28:53"
    update_response = dynamodb_class.update_doi_item_attempts_in_database(
        "test_dois", "111.1/1111"
    )
    assert update_response["ResponseMetadata"]["HTTPStatusCode"] == 200
    updated_item = dynamodb_class.client.get_item(
        TableName="test_dois",
        Key={"doi": {"S": "111.1/1111"}},
    )
    assert updated_item["Item"]["attempts"]["S"] == "2"
    assert updated_item["Item"]["doi"]["S"] == "111.1/1111"
    assert updated_item["Item"]["status"]["S"] == str(Status.FAILED.value)
    assert updated_item["Item"]["last_modified"]["S"] != "2022-01-28 10:28:53"


def test_dynamodb_update_doi_item_status_in_database(mocked_dynamodb, dynamodb_class):
    dynamodb_class.client.put_item(
        TableName="test_dois",
        Item={
            "doi": {"S": "111.1/1111"},
            "status": {"S": str(Status.FAILED.value)},
            "attempts": {"S": "1"},
            "last_modified": {"S": "2022-01-28 10:28:53"},
        },
    )
    existing_item = dynamodb_class.client.get_item(
        TableName="test_dois",
        Key={"doi": {"S": "111.1/1111"}},
    )
    assert existing_item["Item"]["attempts"]["S"] == "1"
    assert existing_item["Item"]["doi"]["S"] == "111.1/1111"
    assert existing_item["Item"]["status"]["S"] == str(Status.FAILED.value)
    assert existing_item["Item"]["last_modified"]["S"] == "2022-01-28 10:28:53"
    update_response = dynamodb_class.update_doi_item_status_in_database(
        "test_dois", "111.1/1111", Status.PROCESSING.value
    )
    assert update_response["ResponseMetadata"]["HTTPStatusCode"] == 200
    updated_item = dynamodb_class.client.get_item(
        TableName="test_dois",
        Key={"doi": {"S": "111.1/1111"}},
    )
    assert updated_item["Item"]["attempts"]["S"] == "1"
    assert updated_item["Item"]["doi"]["S"] == "111.1/1111"
    assert updated_item["Item"]["status"]["S"] == str(Status.PROCESSING.value)
    assert updated_item["Item"]["last_modified"]["S"] != "2022-01-28 10:28:53"
