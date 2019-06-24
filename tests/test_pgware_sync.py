# pylint: skip-file
import pytest

import pgware as pgware


def pytest_generate_tests(metafunc):
    if 'db_cfg' in metafunc.fixturenames:
        metafunc.parametrize('db_cfg', ['asyncpg', 'psycopg2'], indirect=True)


@pytest.fixture
def db_cfg(request):
    return {
        'client': request.param,
        'database': '[DB]',
        'user': '[USER]',
        'password': None,
        'host': '[HOST]',
        'port': None,
        'connection_type': 'single',
    }


def test_single_connect(db_cfg):
    pgw = pgware.build(output='dict', **db_cfg)
    assert(pgw.backend == db_cfg['client'])


def test_preheat(db_cfg):
    pgw = pgware.build(output='dict', **db_cfg)
    pgw.preheat()
    assert(pgw.backend == db_cfg['client'])


def test_cursor(db_cfg):
    pgw = pgware.build(output='dict', **db_cfg)
    with pgw.get_connection().cursor():
        pass


def test_close(db_cfg):
    pgw = pgware.build(output='dict', **db_cfg)
    with pgw.get_connection() as conn:
        conn.close()


def test_cursor_query(db_cfg):
    pgw = pgware.build(output='dict', **db_cfg)
    with pgw.get_connection().cursor() as cur:
        cur.execute('select 1')
        result = cur.fetchone()
    assert(1 == result['?column?'])

    with pgw.get_connection().cursor() as cur:
        cur.execute('select 2')
        result = cur.fetchone()
    assert(2 == result['?column?'])


def test_query(db_cfg):
    pgw = pgware.build(output='dict', **db_cfg)
    with pgw.get_connection() as cur:
        result = cur.fetchone('select 1')
    assert(1 == result['?column?'])


def test_psycogp2_syntax(db_cfg):
    pgw = pgware.build(output='dict', param_format='psycopg2', **db_cfg)
    with pgw.get_connection() as pg:
        result = pg.fetchone('select %s as one, %s as two', ('pouly', 'croc'))
    assert('pouly' == result['one'])
    assert('croc' == result['two'])

    with pgw.get_connection() as pg:
        result = pg.fetchone(
            'select %(qui)s as one, %(quoi)s as two',
            {'quoi': 'pouly', 'qui': 'croc'}
        )
    assert('croc' == result['one'])
    assert('pouly' == result['two'])


def test_asyncpg_syntax(db_cfg):
    pgw = pgware.build(output='dict', param_format='postgresql', **db_cfg)
    with pgw.get_connection() as pg:
        result = pg.fetchone('select $1 as one, $2 as two', ('pouly', 'croc'))
        assert('pouly' == result['one'])
        assert('croc' == result['two'])

        result = pg.fetchone('select $2 as one, $1 as two', ('pouly', 'croc'))
        assert('croc' == result['one'])
        assert('pouly' == result['two'])


def test_json_querying(db_cfg):
    pgw = pgware.build(output='dict', param_format='postgresql', **db_cfg)
    with pgw.get_connection() as pg:
        result = pg.fetchone(
            'select $1::jsonb as json',
            ({'sauce': "andalouse"},))
        assert('andalouse' == result['json']['sauce'])
        result = pg.fetchone(
            'select $1::jsonb as json',
            (['andalouse'],))
        assert('andalouse' == result['json'][0])


def test_prepared_cursor_query_params_postgresql(db_cfg):
    pgw = pgware.build(output='dict', auto_json=False, param_format='postgresql', **db_cfg)
    with pgw.get_connection().cursor() as conn:
        stmt = conn.prepare('select $1 as one, $2 as two')
        stmt.execute(('pouly', 'croc'))
        result = stmt.fetchone()
        assert('pouly' == result['one'])
        assert('croc' == result['two'])
        stmt.execute(('mexi', 'canos'))
        result = stmt.fetchone()
        assert('mexi' == result['one'])
        assert('canos' == result['two'])


def test_prepared_query_params_postgresql(db_cfg):
    pgw = pgware.build(output='dict', param_format='postgresql', **db_cfg)
    with pgw.get_connection() as conn:
        stmt = conn.prepare('select $1 as one, $2 as two')
        result = stmt.fetchone(('frites', 'mayo'))
        assert('frites' == result['one'])
        assert('mayo' == result['two'])
        result = stmt.fetchone(('frites', 'ketchup'))
        assert('frites' == result['one'])
        assert('ketchup' == result['two'])


def test_prepared_query_params_psycopg2(db_cfg):
    pgw = pgware.build(output='dict', param_format='psycopg2', **db_cfg)
    with pgw.get_connection() as conn:
        stmt = conn.prepare('select %s as one, %s as two')
        result = stmt.fetchone(('frites', 'mayo'))
        assert('frites' == result['one'])
        assert('mayo' == result['two'])
        result = stmt.fetchone(('frites', 'ketchup'))
        assert('frites' == result['one'])
        assert('ketchup' == result['two'])


def test_public_methods(db_cfg):
    pgw = pgware.build(output='dict', param_format='postgresql', **db_cfg)
    # Test all methods in all possible combinations

    with pgw.get_connection() as cur:
        cur.executemany('SELECT $1 as one, $2 as two', [('frites', 'ketchup'), ('frites', 'moutarde')])

    with pgw.get_connection().cursor() as cur:
        result = cur.fetchval('select \'mayo\' as one')
        assert result == 'mayo'

        cur.execute('select \'mexicanos\' as one')
        result = cur.fetchval()
        assert result == 'mexicanos'

        stmt = cur.prepare('select $1 as one, $2 as two')
        result = stmt.fetchval(('frites', 'mayo'))
        assert result == 'frites'

        result = cur.fetchval('select $1 as one', ('ketchup',))
        assert result == 'ketchup'

        cur.execute('select \'fricadelle\' as one')
        result = cur.fetchval()
        assert result == 'fricadelle'

    with pgw.get_connection() as cur:
        result = cur.fetchval('select \'mayo\' as one')
        assert result == 'mayo'

        cur.execute('select \'mexicanos\' as one')

        stmt = cur.prepare('select $1 as one, $2 as two')
        result = stmt.fetchval(('frites', 'mayo'))
        assert result == 'frites'

        result = cur.fetchval('select $1 as one', ('ketchup',))
        assert result == 'ketchup'

        cur.execute('select \'fricadelle\' as one')

    with pgw.get_connection().cursor() as cur:
        result = cur.fetchone('select \'mayo\' as one')
        assert result['one'] == 'mayo'

        cur.execute('select \'mexicanos\' as one')
        result = cur.fetchone()
        assert result['one'] == 'mexicanos'

        stmt = cur.prepare('select $1 as one, $2 as two')
        result = stmt.fetchone(('frites', 'mayo'))
        assert result['one'] == 'frites'

        result = cur.fetchone('select $1 as one', ('ketchup',))
        assert result['one'] == 'ketchup'

        cur.execute('select \'fricadelle\' as one')
        result = cur.fetchone()
        assert result['one'] == 'fricadelle'

    with pgw.get_connection() as cur:
        result = cur.fetchone('select \'mayo\' as one')
        assert result['one'] == 'mayo'

        stmt = cur.prepare('select $1 as one, $2 as two')
        result = stmt.fetchone(('frites', 'mayo'))
        assert result['one'] == 'frites'

        result = cur.fetchone('select $1 as one', ('ketchup',))
        assert result['one'] == 'ketchup'

    with pgw.get_connection().cursor() as cur:
        result = cur.fetchall('select \'mayo\' as one')
        assert result[0]['one'] == 'mayo'

        cur.execute('select \'mexicanos\' as one')
        result = cur.fetchall()
        assert result[0]['one'] == 'mexicanos'

        stmt = cur.prepare('select $1 as one, $2 as two')
        result = stmt.fetchall(('frites', 'mayo'))
        assert result[0]['one'] == 'frites'

        result = cur.fetchall('select $1 as one', ('ketchup',))
        assert result[0]['one'] == 'ketchup'

        cur.execute('select \'fricadelle\' as one')
        result = cur.fetchall()
        assert result[0]['one'] == 'fricadelle'

    with pgw.get_connection() as cur:
        result = cur.fetchall('select \'mayo\' as one')
        assert result[0]['one'] == 'mayo'

        stmt = cur.prepare('select $1 as one, $2 as two')
        result = stmt.fetchall(('frites', 'mayo'))
        assert result[0]['one'] == 'frites'

        result = cur.fetchall('select $1 as one', ('ketchup',))
        assert result[0]['one'] == 'ketchup'


def test_dict_outputs(db_cfg):
    pgw = pgware.build(output='dict', param_format='postgresql', **db_cfg)

    with pgw.get_connection() as pgw:
        pgw.execute("""
            CREATE TEMPORARY TABLE pgware_test (
            one varchar(50),
            two int,
            three boolean
            )
        """)
        pgw.execute("INSERT INTO pgware_test VALUES ('pgware', 2, TRUE)")
        result = pgw.fetchone("SELECT * FROM pgware_test")
        assert(isinstance(result, dict))
        assert(result['one'] == 'pgware')
        assert(result['two'] == 2)
        assert(result['three'] is True)

        result = pgw.fetchall("SELECT * FROM pgware_test")
        assert(isinstance(result[0], dict))
        assert(result[0]['one'] == 'pgware')
        assert(result[0]['two'] == 2)
        assert(result[0]['three'] is True)

        result = pgw.fetchval("SELECT * FROM pgware_test")
        assert(result == 'pgware')


def test_list_outputs(db_cfg):
    pgw = pgware.build(output='list', param_format='postgresql', **db_cfg)

    with pgw.get_connection() as pgw:
        pgw.execute("""
            CREATE TEMPORARY TABLE pgware_test (
            one varchar(50),
            two int,
            three boolean
            )
        """)
        pgw.execute("INSERT INTO pgware_test VALUES ('pgware', 2, TRUE)")
        result = pgw.fetchone("SELECT * FROM pgware_test")
        assert(isinstance(result, list))
        assert(result[0] == 'pgware')
        assert(result[1] == 2)
        assert(result[2] is True)

        result = pgw.fetchall("SELECT * FROM pgware_test")
        assert(isinstance(result, list))
        assert(isinstance(result[0], list))
        assert(result[0][0] == 'pgware')
        assert(result[0][1] == 2)
        assert(result[0][2] is True)

        result = pgw.fetchval("SELECT * FROM pgware_test")
        assert(result == 'pgware')


def test_native_outputs(db_cfg):
    pgw = pgware.build(output='native', param_format='postgresql', **db_cfg)
    # Both native formats allow access to values by index or by name

    with pgw.get_connection() as pgw:
        pgw.execute("""
            CREATE TEMPORARY TABLE pgware_test (
            one varchar(50),
            two int,
            three boolean
            )
        """)

        pgw.execute("INSERT INTO pgware_test VALUES ('pgware', 2, TRUE)")
        result = pgw.fetchone("SELECT * FROM pgware_test")
        assert(result[0] == 'pgware')
        assert(result[1] == 2)
        assert(result[2] is True)
        assert(result['one'] == 'pgware')
        assert(result['two'] == 2)
        assert(result['three'] is True)

        result = pgw.fetchall("SELECT * FROM pgware_test")
        assert(result[0][0] == 'pgware')
        assert(result[0][1] == 2)
        assert(result[0][2] is True)
        assert(result[0]['one'] == 'pgware')
        assert(result[0]['two'] == 2)
        assert(result[0]['three'] is True)

        result = pgw.fetchval("SELECT * FROM pgware_test")
        assert(result == 'pgware')


def test_iterator(db_cfg):
    pgw = pgware.build(output='dict', param_format='postgresql', **db_cfg)
    # Both native formats allow access to values by index or by name

    with pgw.get_connection().cursor() as cur:
        cur.execute("""
            CREATE TEMPORARY TABLE pgware_test (
            one varchar(50),
            two int,
            three boolean
            )
        """)

        cur.execute("INSERT INTO pgware_test VALUES ('pgware', 2, TRUE)")
        cur.execute("INSERT INTO pgware_test VALUES ('pgloop', 3, FALSE)")
        cur.fetchall("SELECT * FROM pgware_test")
        out = []
        for row in cur:
            out.append(row)
            print(row)
        print(out)
        assert out[0]['one'] == 'pgware'
        assert out[1]['one'] == 'pgloop'
        cur.execute("INSERT INTO pgware_test VALUES ('pglimp', 4, FALSE)")
        cur.execute("SELECT * FROM pgware_test")
        out = []
        for row in cur:
            out.append(row)
            print(row)
        assert out[0]['one'] == 'pgware'
        assert out[1]['one'] == 'pgloop'
        assert out[2]['one'] == 'pglimp'
