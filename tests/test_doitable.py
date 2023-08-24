from awd.status import Status


def test_add_item(mocked_dynamodb, sample_doi_table):
    assert sample_doi_table.add_item("222.2/2222") == {
        "ConsumedCapacity": {"CapacityUnits": 1.0, "TableName": "test_dois"}
    }


def test_retrieve_items(mocked_dynamodb, sample_doi_table):
    doi_table_items = sample_doi_table.retrieve_items()
    assert doi_table_items[0].doi == "10.1002/term.3131"
    assert doi_table_items[0].attempts == 0
    assert doi_table_items[0].status == 1


def test_attempts_exceeded_false(mocked_dynamodb, sample_doi_table):
    assert not sample_doi_table.attempts_exceeded("10.1002/term.3131", 10)


def test_attempts_exceeded_true(mocked_dynamodb, sample_doi_table):
    sample_doi_table.increment_attempts("10.1002/term.3131")
    assert sample_doi_table.attempts_exceeded("10.1002/term.3131", 1)


def test_increment_attempts(mocked_dynamodb, sample_doi_table):
    assert sample_doi_table.get("10.1002/term.3131").attempts == 0
    sample_doi_table.increment_attempts("10.1002/term.3131")
    assert sample_doi_table.get("10.1002/term.3131").attempts == 1


def test_update_status(mocked_dynamodb, sample_doi_table):
    sample_doi_table.update_status("10.1002/term.3131", Status.MESSAGE_SENT.value)
    assert sample_doi_table.get("10.1002/term.3131").status == Status.MESSAGE_SENT.value
