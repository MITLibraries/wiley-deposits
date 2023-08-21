from awd.article import Article
from awd.status import Status


def test_exists_in_database_true():
    article = Article("222.2/2222")
    database_items = [{"doi": "111.1/1111"}]
    validation_status = article.exists_in_database(database_items)
    assert validation_status is True


def test_exists_in_database_false(sample_article):
    database_items = [{"doi": "111.1/1111"}]
    validation_status = sample_article.exists_in_database(database_items)
    assert validation_status is False


def test_has_retry_status_true(sample_article):
    database_items = [{"doi": "111.1/1111", "status": str(Status.UNPROCESSED.value)}]
    validation_status = sample_article.has_retry_status(database_items)
    assert validation_status is True


def test_has_retry_status_false(sample_article):
    database_items = [{"doi": "111.1/1111", "status": str(Status.SUCCESS.value)}]
    validation_status = sample_article.has_retry_status(database_items)
    assert validation_status is False
