# pylint: skip-file
import pytest

import pgware as pgware
from pgware import (
    PgWareError,
    ProgrammingError,
    PublicError,
    QueryError,
    RetriesExhausted,
)

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


async def test_query_error(db_cfg, event_loop):
    """ Syntax error """
    pgw = pgware.build(output='dict', **db_cfg)
    try:
        async with pgw.get_connection().cursor() as cur:
            await cur.execute('SELRECT 2')
    except PgWareError as ex:
        assert isinstance(ex, QueryError)
        assert isinstance(ex, PublicError)
    else:
        assert False, "Exception failed to be raised"


async def test_query2_error(db_cfg, event_loop):
    """ Undefined table """
    pgw = pgware.build(output='dict', **db_cfg)
    try:
        async with pgw.get_connection().cursor() as cur:
            await cur.execute('INSERT INTO plouplou VALUES (1,2)')
    except PgWareError as ex:
        assert isinstance(ex, QueryError)
        assert isinstance(ex, PublicError)
    else:
        assert False, "Exception failed to be raised"


async def test_retries_exhausted_error(db_cfg, event_loop):
    """ Force error by setting wrong host """
    db_cfg['host'] = '127.0.0.1'
    pgw = pgware.build(output='dict', **db_cfg)
    try:
        async with pgw.get_connection().cursor() as cur:
            await cur.execute('SELECT 2')
    except PgWareError as ex:
        assert isinstance(ex, PublicError)
        assert isinstance(ex, RetriesExhausted)
    else:
        assert False, "Exception failed to be raised"


async def test_programming2_error(db_cfg, event_loop):
    """ Bad config settings"""
    db_cfg['client'] = 'samurai'
    pgw = pgware.build(output='dict', **db_cfg)
    try:
        async with pgw.get_connection().cursor() as cur:
            await cur.execute(('values', 'without', 'query'))
    except PgWareError as ex:
        assert isinstance(ex, PublicError)
        assert isinstance(ex, ProgrammingError)
    else:
        assert False, "Exception failed to be raised"


async def test_programming2_error(db_cfg, event_loop):
    """ Bad method usage """
    pgw = pgware.build(output='dict', **db_cfg)
    try:
        async with pgw.get_connection() as conn:
            await conn.execute(('values', 'without', 'query'))
    except PgWareError as ex:
        assert isinstance(ex, PublicError)
        assert isinstance(ex, ProgrammingError)
    else:
        assert False, "Exception failed to be raised"
