# pylint: skip-file
from pgware import QueryLoader


def test_successfull_loading():
    ql = QueryLoader('tests', 'resources')
    query = ql.load('sql_basic_query')
    assert(query == 'SELECT 1')


def test_successful_substitution_stored():
    ql = QueryLoader('tests', 'resources', replace_me='"done"')
    query = ql.load('sql_format_query')
    assert(query == 'SELECT "done"')


def test_successful_substitution_param():
    ql = QueryLoader('tests', 'resources')
    query = ql.load('sql_format_query', replace_me="'ok'")
    assert(query == "SELECT 'ok'")


def test_failing_load():
    ql = QueryLoader('tests', 'resources')
    try:
        ql.load('sql_where_are_you')
    except RuntimeError:
        assert(True)
    else:
        # error has not occured
        assert(False)


def test_failing_substitution():
    ql = QueryLoader('tests', 'resources')
    try:
        ql.load('sql_format_query')
    except KeyError:
        assert(True)
    else:
        # error has not occured
        assert(False)


def test_cache():
    ql = QueryLoader('tests', 'resources')
    assert('sql_basic_query' not in ql._cache)
    ql.load('sql_basic_query')
    assert('sql_basic_query' in ql._cache)
