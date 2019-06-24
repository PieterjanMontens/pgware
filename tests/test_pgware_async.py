# pylint: skip-file
import pytest

import pgware as pgware

pytestmark = pytest.mark.asyncio


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


async def test_single_connect(db_cfg, event_loop):
    pgw = pgware.build(output='dict', **db_cfg)
    assert(pgw.backend == db_cfg['client'])


async def test_preheat(db_cfg, event_loop):
    pgw = pgware.build(output='dict', **db_cfg)
    await pgw.preheat_async()
    async with pgw.get_connection().cursor():
        pass


async def test_cursor(db_cfg, event_loop):
    pgw = pgware.build(output='dict', **db_cfg)
    async with pgw.get_connection().cursor():
        pass


async def test_close(db_cfg, event_loop):
    pgw = pgware.build(output='dict', **db_cfg)
    async with pgw.get_connection() as conn:
        conn.close()


async def test_close_all(db_cfg, event_loop):
    pgw = pgware.build(output='dict', **db_cfg)
    async with pgw.get_connection():
        pass
    await pgw.close_all()


async def test_cursor_query_async(db_cfg, event_loop):
    pgw = pgware.build(output='dict', **db_cfg)
    async with pgw.get_connection().cursor() as cur:
        await cur.execute('select 1')
        result = await cur.fetchone()
    assert(1 == result['?column?'])

    async with pgw.get_connection().cursor() as cur:
        await cur.execute('select 2')
        result = await cur.fetchone()
    assert(2 == result['?column?'])


async def test_query_async(db_cfg, event_loop):
    pgw = pgware.build(output='dict', **db_cfg)
    async with pgw.get_connection() as cur:
        result = await cur.fetchone('select 1')
    assert(1 == result['?column?'])


async def test_psycogp2_syntax(db_cfg, event_loop):
    pgw = pgware.build(output='dict', param_format='psycopg2', **db_cfg)
    async with pgw.get_connection() as pg:
        result = await pg.fetchone('select %s as one, %s as two', ('pouly', 'croc'))
    assert('pouly' == result['one'])
    assert('croc' == result['two'])

    async with pgw.get_connection() as pg:
        result = await pg.fetchone(
            'select %(qui)s as one, %(quoi)s as two',
            {'quoi': 'pouly', 'qui': 'croc'}
        )
    assert('croc' == result['one'])
    assert('pouly' == result['two'])


async def test_asyncpg_syntax(db_cfg, event_loop):
    pgw = pgware.build(output='dict', param_format='postgresql', **db_cfg)
    async with pgw.get_connection() as pg:
        result = await pg.fetchone('select $1 as one, $2 as two', ('pouly', 'croc'))
        assert('pouly' == result['one'])
        assert('croc' == result['two'])

        result = await pg.fetchone('select $2 as one, $1 as two', ('pouly', 'croc'))
        assert('croc' == result['one'])
        assert('pouly' == result['two'])


async def test_json_querying(db_cfg, event_loop):
    pgw = pgware.build(output='dict', param_format='postgresql', **db_cfg)
    async with pgw.get_connection() as pg:
        result = await pg.fetchone(
            'select $1::jsonb as json',
            ({'sauce': "andalouse"},))
        assert('andalouse' == result['json']['sauce'])
        result = await pg.fetchone(
            'select $1::jsonb as json',
            (['andalouse'],))
        assert('andalouse' == result['json'][0])


async def test_prepared_cursor_query_params_postgresql(db_cfg, event_loop):
    pgw = pgware.build(output='dict', param_format='postgresql', **db_cfg)
    async with pgw.get_connection().cursor() as conn:
        stmt = await conn.prepare('select $1 as one, $2 as two')
        await stmt.execute(('pouly', 'croc'))
        result = await stmt.fetchone()
        assert('pouly' == result['one'])
        assert('croc' == result['two'])
        await stmt.execute(('mexi', 'canos'))
        result = await stmt.fetchone()
        assert('mexi' == result['one'])
        assert('canos' == result['two'])


async def test_prepared_query_params_postgresql(db_cfg, event_loop):
    pgw = pgware.build(output='dict', param_format='postgresql', **db_cfg)
    async with pgw.get_connection() as conn:
        stmt = await conn.prepare('select $1 as one, $2 as two')
        result = await stmt.fetchone(('frites', 'mayo'))
        assert('frites' == result['one'])
        assert('mayo' == result['two'])
        result = await stmt.fetchone(('frites', 'ketchup'))
        assert('frites' == result['one'])
        assert('ketchup' == result['two'])


async def test_prepared_query_params_psycopg2(db_cfg, event_loop):
    pgw = pgware.build(output='dict', param_format='psycopg2', **db_cfg)
    async with pgw.get_connection() as conn:
        stmt = await conn.prepare('select %s as one, %s as two')
        result = await stmt.fetchone(('frites', 'mayo'))
        assert('frites' == result['one'])
        assert('mayo' == result['two'])
        result = await stmt.fetchone(('frites', 'ketchup'))
        assert('frites' == result['one'])
        assert('ketchup' == result['two'])


async def test_public_methods(db_cfg, event_loop):
    pgw = pgware.build(output='dict', param_format='postgresql', **db_cfg)
    # Test all methods in all possible combinations

    async with pgw.get_connection() as cur:
        await cur.executemany('SELECT $1 as one, $2 as two', [('frites', 'ketchup'), ('frites', 'moutarde')])

    async with pgw.get_connection().cursor() as cur:
        result = await cur.fetchval('select \'mayo\' as one')
        assert result == 'mayo'

        await cur.execute('select \'mexicanos\' as one')
        result = await cur.fetchval()
        assert result == 'mexicanos'

        stmt = await cur.prepare('select $1 as one, $2 as two')
        result = await stmt.fetchval(('frites', 'mayo'))
        assert result == 'frites'

        result = await cur.fetchval('select $1 as one', ('ketchup',))
        assert result == 'ketchup'

        await cur.execute('select \'fricadelle\' as one')
        result = await cur.fetchval()
        assert result == 'fricadelle'

    async with pgw.get_connection() as cur:
        result = await cur.fetchval('select \'mayo\' as one')
        assert result == 'mayo'

        await cur.execute('select \'mexicanos\' as one')

        stmt = await cur.prepare('select $1 as one, $2 as two')
        result = await stmt.fetchval(('frites', 'mayo'))
        assert result == 'frites'

        result = await cur.fetchval('select $1 as one', ('ketchup',))
        assert result == 'ketchup'

        await cur.execute('select \'fricadelle\' as one')

    async with pgw.get_connection().cursor() as cur:
        result = await cur.fetchone('select \'mayo\' as one')
        assert result['one'] == 'mayo'

        await cur.execute('select \'mexicanos\' as one')
        result = await cur.fetchone()
        assert result['one'] == 'mexicanos'

        stmt = await cur.prepare('select $1 as one, $2 as two')
        result = await stmt.fetchone(('frites', 'mayo'))
        assert result['one'] == 'frites'

        result = await cur.fetchone('select $1 as one', ('ketchup',))
        assert result['one'] == 'ketchup'

        await cur.execute('select \'fricadelle\' as one')
        result = await cur.fetchone()
        assert result['one'] == 'fricadelle'

    async with pgw.get_connection() as cur:
        result = await cur.fetchone('select \'mayo\' as one')
        assert result['one'] == 'mayo'

        stmt = await cur.prepare('select $1 as one, $2 as two')
        result = await stmt.fetchone(('frites', 'mayo'))
        assert result['one'] == 'frites'

        result = await cur.fetchone('select $1 as one', ('ketchup',))
        assert result['one'] == 'ketchup'

    async with pgw.get_connection().cursor() as cur:
        result = await cur.fetchall('select \'mayo\' as one')
        assert result[0]['one'] == 'mayo'

        await cur.execute('select \'mexicanos\' as one')
        result = await cur.fetchall()
        assert result[0]['one'] == 'mexicanos'

        stmt = await cur.prepare('select $1 as one, $2 as two')
        result = await stmt.fetchall(('frites', 'mayo'))
        assert result[0]['one'] == 'frites'

        result = await cur.fetchall('select $1 as one', ('ketchup',))
        assert result[0]['one'] == 'ketchup'

        await cur.execute('select \'fricadelle\' as one')
        result = await cur.fetchall()
        assert result[0]['one'] == 'fricadelle'

    async with pgw.get_connection() as cur:
        result = await cur.fetchall('select \'mayo\' as one')
        assert result[0]['one'] == 'mayo'

        stmt = await cur.prepare('select $1 as one, $2 as two')
        result = await stmt.fetchall(('frites', 'mayo'))
        assert result[0]['one'] == 'frites'

        result = await cur.fetchall('select $1 as one', ('ketchup',))
        assert result[0]['one'] == 'ketchup'

    await pgw.close_all()


async def test_dict_outputs(db_cfg, event_loop):
    pgw = pgware.build(output='dict', param_format='postgresql', **db_cfg)

    async with pgw.get_connection() as pgw:
        await pgw.execute("""
            CREATE TEMPORARY TABLE pgware_test (
            one varchar(50),
            two int,
            three boolean
            )
        """)
        await pgw.execute("INSERT INTO pgware_test VALUES ('pgware', 2, TRUE)")
        result = await pgw.fetchone("SELECT * FROM pgware_test")
        assert(isinstance(result, dict))
        assert(result['one'] == 'pgware')
        assert(result['two'] == 2)
        assert(result['three'] is True)

        result = await pgw.fetchall("SELECT * FROM pgware_test")
        assert(isinstance(result[0], dict))
        assert(result[0]['one'] == 'pgware')
        assert(result[0]['two'] == 2)
        assert(result[0]['three'] is True)

        result = await pgw.fetchval("SELECT * FROM pgware_test")
        assert(result == 'pgware')


async def test_list_outputs(db_cfg, event_loop):
    pgw = pgware.build(output='list', param_format='postgresql', **db_cfg)

    async with pgw.get_connection() as pgw:
        await pgw.execute("""
            CREATE TEMPORARY TABLE pgware_test (
            one varchar(50),
            two int,
            three boolean
            )
        """)
        await pgw.execute("INSERT INTO pgware_test VALUES ('pgware', 2, TRUE)")
        result = await pgw.fetchone("SELECT * FROM pgware_test")
        assert(isinstance(result, list))
        assert(result[0] == 'pgware')
        assert(result[1] == 2)
        assert(result[2] is True)

        result = await pgw.fetchall("SELECT * FROM pgware_test")
        assert(isinstance(result, list))
        assert(isinstance(result[0], list))
        assert(result[0][0] == 'pgware')
        assert(result[0][1] == 2)
        assert(result[0][2] is True)

        result = await pgw.fetchval("SELECT * FROM pgware_test")
        assert(result == 'pgware')


async def test_native_outputs(db_cfg, event_loop):
    pgw = pgware.build(output='native', param_format='postgresql', **db_cfg)
    # Both native formats allow access to values by index or by name

    async with pgw.get_connection() as pgw:
        await pgw.execute("""
            CREATE TEMPORARY TABLE pgware_test (
            one varchar(50),
            two int,
            three boolean
            )
        """)

        await pgw.execute("INSERT INTO pgware_test VALUES ('pgware', 2, TRUE)")
        result = await pgw.fetchone("SELECT * FROM pgware_test")
        assert(result[0] == 'pgware')
        assert(result[1] == 2)
        assert(result[2] is True)
        assert(result['one'] == 'pgware')
        assert(result['two'] == 2)
        assert(result['three'] is True)

        result = await pgw.fetchall("SELECT * FROM pgware_test")
        assert(result[0][0] == 'pgware')
        assert(result[0][1] == 2)
        assert(result[0][2] is True)
        assert(result[0]['one'] == 'pgware')
        assert(result[0]['two'] == 2)
        assert(result[0]['three'] is True)

        result = await pgw.fetchval("SELECT * FROM pgware_test")
        assert(result == 'pgware')


async def test_iterator(db_cfg, event_loop):
    pgw = pgware.build(output='dict', param_format='postgresql', **db_cfg)
    # Both native formats allow access to values by index or by name

    async with pgw.get_connection().cursor() as cur:
        await cur.execute("""
            CREATE TEMPORARY TABLE pgware_test (
            one varchar(50),
            two int,
            three boolean
            )
        """)

        await cur.execute("INSERT INTO pgware_test VALUES ('pgware', 2, TRUE)")
        await cur.execute("INSERT INTO pgware_test VALUES ('pgloop', 3, FALSE)")
        await cur.fetchall("SELECT * FROM pgware_test")
        out = []
        async for row in cur:
            out.append(row)
            print(row)
        print(out)
        assert out[0]['one'] == 'pgware'
        assert out[1]['one'] == 'pgloop'
        await cur.execute("INSERT INTO pgware_test VALUES ('pglimp', 4, FALSE)")
        await cur.execute("SELECT * FROM pgware_test")
        out = []
        async for row in cur:
            out.append(row)
            print(row)
        assert out[0]['one'] == 'pgware'
        assert out[1]['one'] == 'pgloop'
        assert out[2]['one'] == 'pglimp'
