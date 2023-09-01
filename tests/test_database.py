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


def test_sqs_error_update_status_above_retry_threshold_set_failed_status(
    caplog,
    sample_doiprocessattempt,
):
    sample_doiprocessattempt.doi = "222.2/2222"
    sample_doiprocessattempt.sqs_error_update_status(retry_threshold=0)
    assert sample_doiprocessattempt.get("222.2/2222").status == Status.FAILED.value
    assert (
        "DOI: '222.2/2222' has exceeded the retry threshold and will not be "
        "attempted again" in caplog.text
    )


def test_sqs_error_update_status_below_retry_threshold_set_unprocessed_status(
    caplog,
    sample_doiprocessattempt,
):
    sample_doiprocessattempt.doi = "222.2/2222"
    sample_doiprocessattempt.sqs_error_update_status(retry_threshold=30)
    assert sample_doiprocessattempt.get("222.2/2222").status == Status.UNPROCESSED.value


def test_update_status(mocked_dynamodb, sample_doiprocessattempt):
    sample_doiprocessattempt.update_status(status_code=Status.MESSAGE_SENT.value)
    assert (
        sample_doiprocessattempt.get("10.1002/term.3131").status
        == Status.MESSAGE_SENT.value
    )
