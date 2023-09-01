from awd.status import Status


def test_add_item(mocked_dynamodb, sample_doiprocessattempt):
    assert sample_doiprocessattempt.add_item(doi="222.2/2222") == {
        "ConsumedCapacity": {"CapacityUnits": 1.0, "TableName": "wiley-test"}
    }


def test_has_unprocessed_status_true(sample_doiprocessattempt):
    assert sample_doiprocessattempt.has_unprocessed_status() is True


def test_has_unprocessed_status_false(sample_doiprocessattempt):
    sample_doiprocessattempt.update_status(status_code=Status.MESSAGE_SENT.value)
    assert sample_doiprocessattempt.has_unprocessed_status() is False


def test_attempts_exceeded_false(mocked_dynamodb, sample_doiprocessattempt):
    assert not sample_doiprocessattempt.attempts_exceeded(retry_threshold=10)


def test_attempts_exceeded_true(mocked_dynamodb, sample_doiprocessattempt):
    sample_doiprocessattempt.increment_attempts()
    assert sample_doiprocessattempt.attempts_exceeded(retry_threshold=1)


def test_increment_attempts(mocked_dynamodb, sample_doiprocessattempt):
    assert sample_doiprocessattempt.get("10.1002/term.3131").attempts == 0
    sample_doiprocessattempt.increment_attempts()
    assert sample_doiprocessattempt.get("10.1002/term.3131").attempts == 1


def test_update_status(mocked_dynamodb, sample_doiprocessattempt):
    sample_doiprocessattempt.update_status(status_code=Status.MESSAGE_SENT.value)
    assert (
        sample_doiprocessattempt.get("10.1002/term.3131").status
        == Status.MESSAGE_SENT.value
    )
