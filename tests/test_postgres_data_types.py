# pylint: skip-file
from datetime import datetime

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
        'host': '[HOST]',
        'connection_type': 'single',
    }


CREATE_QUERY = """
CREATE TEMPORARY TABLE pgware_test_types_temp (
    -- numeric
    test_int    int,
    test_dec    decimal,
    test_num    numeric,
    test_real   real,
    test_money  money,

    -- characters
    test_vchar  varchar(10),
    test_char   char(10),
    test_txt    text,

    -- misc
    test_bin    bytea,
    test_bool   boolean,
    -- test_enum   enum ('one', 'two', 'three'), (not yet, young padawan)
    test_inet   inet,
    test_bit    bit(3),
    test_uuid   uuid,
    -- test_xml    xmlparse (I don't even..)
    test_oid    oid,

    -- time
    test_tm     timestamp without time zone,
    test_tmtz   timestamp with time zone,
    test_date   date,
    test_time   time,
    -- test_tmint  interval 'YEAR', (not yet, young padawan)

    -- json
    test_json   json,
    test_jsonb  jsonb,

    -- arrays
    test_arrr   text[],
    test_arri   integer[],
    test_arrj   jsonb[]

    -- skipped types:
    -- range
    -- geometric
    -- text search/vectors
    -- LSN (log sequence number)
    -- pseudo
    -- complex/composite

);
"""

INSERT_QUERY_STATIC = """
INSERT INTO pgware_test_types_temp (
    test_int, test_dec, test_num, test_real, test_money,
    test_vchar, test_char, test_txt,
    test_bin, test_bool, test_inet, test_bit, test_uuid, test_oid,
    test_tm, test_tmtz, test_date, test_time,
    test_json, test_jsonb,
    test_arrr, test_arri, test_arrj
)
VALUES (
    12341234, 1234.1234, 1234.1234, 1234, 1234.12,
    'pgware', 'pgware', 'pgware',
    E'\\176', TRUE, '10.10.10.10', B'010', 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11', '1234',
    '2004-10-19 10:23:54', '2004-10-19 10:23:54+02', '2004-10-19', '10:23:54',
    '{"foo": "bar"}', '{"pouly": "croc"}',
    '{"un", "deux", "trois"}', '{1,2,3}', '{"{\\"foo\\":\\"bar\\"}", "{\\"pouly\\":\\"croc\\"}"}'
);
"""

COMPLEX_QUERY_ASYNCPG = """
SELECT
    $1::text as txt,
    $2::text[] as txt_array,
    $3::int as int,
    $4::json as json,
    $5::timestamp as tz
"""

COMPLEX_QUERY_PSYCOPG2 = """
SELECT
    %s::text as txt,
    %s::text[] as txt_array,
    %s::int as int,
    %s::json as json,
    %s::timestamp as tz
"""

COMPLEX_QUERY_VALUES = ("Poulet", ["ketchup", "mayo", "andalouse"], 18, '{"un":1}', datetime.now())

SELECT_QUERY = "SELECT * FROM pgware_test_types_temp"


async def test_simple(db_cfg, event_loop):
    db = pgware.build(output='dict', **db_cfg)
    async with db.get_connection() as pgw:
        await pgw.execute("CREATE TEMPORARY TABLE pgware_test (val varchar(50))")
        await pgw.execute("INSERT INTO pgware_test VALUES ('pgware')")
        result = await pgw.fetchone("SELECT val as val FROM pgware_test")
        assert(result['val'] == 'pgware')


async def test_static(db_cfg, event_loop):
    db = pgware.build(output='dict', **db_cfg)
    async with db.get_connection() as pgw:
        await pgw.execute(CREATE_QUERY)
        await pgw.execute(INSERT_QUERY_STATIC)
        await pgw.fetchone(SELECT_QUERY)


async def test_numeric(db_cfg, event_loop):
    db = pgware.build(output='dict', param_format='psycopg2', **db_cfg)
    from decimal import Decimal
    async with db.get_connection() as pgw:
        await pgw.execute("""
            CREATE TEMPORARY TABLE pgware_test (
                test_int    int,
                test_dec    decimal,
                test_num    numeric,
                test_real   real,
                test_money  money
            )
            """)
        await pgw.execute("""
            INSERT INTO pgware_test VALUES (
                %s,
                %s,
                %s,
                %s,
                %s
            )
            """, (12, 1.2, 1.2, 12, '12'))
        result = await pgw.fetchone("SELECT * FROM pgware_test")
        assert(result['test_int'] == 12)
        assert(float(result['test_dec']) == float(Decimal('1.2')))
        assert(float(result['test_num']) == float(Decimal('1.2')))
        assert(result['test_real'] == 12)
        assert(result['test_money'] == '$12.00')


async def test_char(db_cfg, event_loop):
    db = pgware.build(output='dict', param_format='psycopg2', **db_cfg)
    async with db.get_connection() as pgw:
        await pgw.execute("""
            CREATE TEMPORARY TABLE pgware_test (
                test_vchar  varchar(10),
                test_char   char(4),
                test_txt    text
            )
            """)
        await pgw.execute(
            "INSERT INTO pgware_test VALUES (%s, %s, %s)",
            ("un", "deux", "trois"))
        result = await pgw.fetchone("SELECT * FROM pgware_test")
        assert(result['test_vchar'] == "un")
        assert(result['test_char'] == "deux")
        assert(result['test_txt'] == "trois")


async def test_arrays(db_cfg, event_loop):
    db = pgware.build(output='dict', param_format='psycopg2', auto_json=False, **db_cfg)
    import json
    async with db.get_connection() as pgw:
        await pgw.execute("""
            CREATE TEMPORARY TABLE pgware_test (
                test_arrr   text[],
                test_arri   integer[],
                test_arrj   jsonb[]
            )
            """)
        await pgw.execute(
            "INSERT INTO pgware_test VALUES (%s, %s, %s::jsonb[])",
            (["a", "b", "c"],
             [1, 2, 3],
             [json.dumps({'foo': 'bar'}), json.dumps({'pouly': 'croc'})]))
        result = await pgw.fetchone("SELECT * FROM pgware_test")
        assert(result['test_arrr'] == ["a", "b", "c"])
        assert(result['test_arri'] == [1, 2, 3])
        # Needs more work:
        # assert(result['test_arrj'] == [{'foo':'bar'}, {'pouly':'croc'}])
        await pgw.execute('TRUNCATE TABLE pgware_test')

        await pgw.execute(
            "INSERT INTO pgware_test VALUES (%(one)s::text[], %(two)s, %(three)s::jsonb[])",
            {'one': ["a", "b", "c"],
             'two': [1, 2, 3],
             'three': [json.dumps({'foo': 'bar'}), json.dumps({'pouly': 'croc'})]}
        )
        result = await pgw.fetchone("SELECT * FROM pgware_test")
        assert(result['test_arrr'] == ["a", "b", "c"])
        assert(result['test_arri'] == [1, 2, 3])
        # Needs more work:
        # assert(result['test_arrj'] == [{'foo':'bar'}, {'pouly':'croc'}])


async def test_time(db_cfg, event_loop):
    db = pgware.build(output='dict', param_format='psycopg2', **db_cfg)
    async with db.get_connection() as pgw:
        await pgw.execute("""
            CREATE TEMPORARY TABLE pgware_test (
                test_tm     timestamp without time zone,
                test_tmtz   timestamp with time zone,
                test_date   date,
                test_time   time
            )
            """)
        now = datetime.now()
        await pgw.execute(
            "INSERT INTO pgware_test VALUES (%s, %s, %s, %s)",
            (now, now, now.date(), now.time())
        )
        result = await pgw.fetchone("SELECT * FROM pgware_test")
        assert(result['test_tm'] == now)
        assert(result['test_tmtz'].date() == now.date())
        assert(result['test_date'] == now.date())
        assert(result['test_time'] == now.time())


async def test_json(db_cfg, event_loop):
    db = pgware.build(output='dict', param_format='psycopg2', **db_cfg)
    async with db.get_connection() as pgw:
        await pgw.execute("""
            CREATE TEMPORARY TABLE pgware_test (
                test_json   json,
                test_jsonb  jsonb
            )
            """)
        await pgw.execute(
            "INSERT INTO pgware_test VALUES (%s, %s)",
            ({'foo': 'bar'}, {'pouly': 'croc'})
        )
        result = await pgw.fetchone("SELECT * FROM pgware_test")
        assert(result['test_json'] == {'foo': 'bar'})
        assert(result['test_jsonb'] == {'pouly': 'croc'})


async def test_complex_asyncpg(db_cfg, event_loop):
    db = pgware.build(output='dict', param_format='asyncpg', auto_json=False, **db_cfg)
    async with db.get_connection() as pgw:
        result = await pgw.fetchone(COMPLEX_QUERY_ASYNCPG, COMPLEX_QUERY_VALUES)
        print(result)
        assert(result['txt'] == COMPLEX_QUERY_VALUES[0])


async def test_complex_psycopg2(db_cfg, event_loop):
    db = pgware.build(output='dict', param_format='psycopg2', auto_json=False, **db_cfg)
    async with db.get_connection() as pgw:
        result = await pgw.fetchone(COMPLEX_QUERY_PSYCOPG2, COMPLEX_QUERY_VALUES)
        print(result)
        assert(result['txt'] == COMPLEX_QUERY_VALUES[0])


# async def test_misc(db_cfg, event_loop):
#     db = pgware.build(output='dict', param_format='psycopg2', **db_cfg)
#     # FIXME: to do
