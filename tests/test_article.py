from awd.article import Article
from awd.status import Status


def test_doi_to_be_added_true():
    article = Article("222.2/2222")
    database_items = [{"doi": "111.1/1111"}]
    validation_status = article.doi_to_be_added(database_items)
    assert validation_status is True


def test_doi_to_be_added_false(article_class):
    database_items = [{"doi": "111.1/1111"}]
    validation_status = article_class.doi_to_be_added(database_items)
    assert validation_status is False


def test_doi_to_be_retried_true(article_class):
    database_items = [{"doi": "111.1/1111", "status": str(Status.UNPROCESSED.value)}]
    validation_status = article_class.doi_to_be_retried(database_items)
    assert validation_status is True


def test_doi_to_be_retried_false(article_class):
    database_items = [{"doi": "111.1/1111", "status": str(Status.SUCCESS.value)}]
    validation_status = article_class.doi_to_be_retried(database_items)
    assert validation_status is False
