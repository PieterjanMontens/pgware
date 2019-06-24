import pgware as pgware


def test_psyco2postgre():
    given = ('select %s as one, %s as two', ('1', '2'))
    expected = ('select $1 as one, $2 as two', ('1', '2'))
    assert expected == pgware.ps2pg(*given)

    given = ('select %s as one', ('andalouse'))
    expected = ('select $1 as one', ('andalouse',))
    assert expected == pgware.ps2pg(*given)


def test_psyco2postgre_dict():
    given = (
        'select %(ingredient)s as one, %(sauce)s as two',
        {'ingredient': 'frites', 'sauce': 'andalouse'}
    )
    expected = ('select $1 as one, $2 as two', ('frites', 'andalouse'))
    assert expected == pgware.ps2pg(*given)

    given = (
        'select %(ingredient)s as one, %(sauce)s as two, %(sauce)s as three',
        {'ingredient': 'frites', 'sauce': 'andalouse'}
    )
    expected = ('select $1 as one, $2 as two, $3 as three', ('frites', 'andalouse', 'andalouse'))
    assert expected == pgware.ps2pg(*given)

    given = (
        'select %(sauce)s as one, %(ingredient)s as two',
        {'ingredient': 'frites', 'sauce': 'andalouse'}
    )
    expected = ('select $1 as one, $2 as two', ('andalouse', 'frites'))
    assert expected == pgware.ps2pg(*given)

    given = (
        'select %(sauce)s as one',
        {'sauce': 'andalouse'}
    )
    expected = ('select $1 as one', ('andalouse',))
    assert expected == pgware.ps2pg(*given)


def test_postgre2psyco():
    given = ('select $1 as one, $2 as two', ('1', '2'))
    expected = ('select %s as one, %s as two', ('1', '2'))
    assert expected == pgware.pg2ps(*given)

    given = ('select $2 as one, $2 as two', ('1', '2'))
    expected = ('select %s as one, %s as two', ('2', '2'))
    assert expected == pgware.pg2ps(*given)

    given = ('select $2 as one, $1 as two', ('1', '2'))
    expected = ('select %s as one, %s as two', ('2', '1'))
    assert expected == pgware.pg2ps(*given)

    given = ('select $1 as one', ('boudin'))
    expected = ('select %s as one', ('boudin',))
    assert expected == pgware.pg2ps(*given)


def test_config_mapper():
    given_config = {'a': 1, 'b': 2}
    given_map = {
        'a': ['a', 0],
        'c': ['b', 0],
        'd': ['x', 10]
    }
    expected = {
        'a': 1,
        'c': 2,
        'd': 10
    }
    assert expected == pgware.config_map(given_map, given_config)
