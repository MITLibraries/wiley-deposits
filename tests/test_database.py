import pytest
from pynamodb.exceptions import DoesNotExist

from awd.database import UnprocessedStatusFalseError
from awd.status import Status


def test_add_item(mocked_dynamodb, sample_doiprocessattempt):
    assert sample_doiprocessattempt.add_item("222.2/2222") == {
        "ConsumedCapacity": {"CapacityUnits": 1.0, "TableName": "wiley-test"}
    }


def test_has_unprocessed_status_true(sample_doiprocessattempt):
    assert sample_doiprocessattempt.has_unprocessed_status("10.1002/term.3131") is True


def test_has_unprocessed_status_false(sample_doiprocessattempt):
    sample_doiprocessattempt.update_status("10.1002/term.3131", Status.MESSAGE_SENT.value)
    assert sample_doiprocessattempt.has_unprocessed_status("10.1002/term.3131") is False


def test_check_doi_and_add_to_table_success(
    mocked_dynamodb,
    sample_doiprocessattempt,
):
    with pytest.raises(DoesNotExist):
        sample_doiprocessattempt.get("10.1002/none.0000")
    sample_doiprocessattempt.check_doi_and_add_to_table("10.1002/none.0000")
    assert sample_doiprocessattempt.get("10.1002/none.0000").attempts == 1


def test_add_item_to_doi_table_unprocessed_status_false_raises_exception(
    mocked_dynamodb,
    sample_doiprocessattempt,
):
    sample_doiprocessattempt.update_status("10.1002/term.3131", Status.MESSAGE_SENT.value)
    with pytest.raises(UnprocessedStatusFalseError):
        sample_doiprocessattempt.check_doi_and_add_to_table("10.1002/term.3131")


def test_attempts_exceeded_false(mocked_dynamodb, sample_doiprocessattempt):
    assert not sample_doiprocessattempt.attempts_exceeded("10.1002/term.3131", 10)


def test_attempts_exceeded_true(mocked_dynamodb, sample_doiprocessattempt):
    sample_doiprocessattempt.increment_attempts("10.1002/term.3131")
    assert sample_doiprocessattempt.attempts_exceeded("10.1002/term.3131", 1)


def test_increment_attempts(mocked_dynamodb, sample_doiprocessattempt):
    assert sample_doiprocessattempt.get("10.1002/term.3131").attempts == 0
    sample_doiprocessattempt.increment_attempts("10.1002/term.3131")
    assert sample_doiprocessattempt.get("10.1002/term.3131").attempts == 1


def test_update_status(mocked_dynamodb, sample_doiprocessattempt):
    sample_doiprocessattempt.update_status("10.1002/term.3131", Status.MESSAGE_SENT.value)
    assert (
        sample_doiprocessattempt.get("10.1002/term.3131").status
        == Status.MESSAGE_SENT.value
    )
