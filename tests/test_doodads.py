# pylint: skip-file
import pytest

import pgware as pgware
from pgware import doodad

pytestmark = pytest.mark.asyncio


config = {
    'client': 'psycopg2',
    'database': '[DB]',
    'user': '[USER]',
    'password': None,
    'host': '[HOST]',
    'port': None,
    'connection_type': 'single'
}


async def test_doodad(event_loop):
    db = pgware.build(output='dict', **config)
    test = [0] * 4

    @doodad
    def test0(state):
        test[0] += 1
        yield state

    @doodad
    def test1(state):
        test[1] += 1
        yield state

    @doodad
    def test2(state):
        test[2] += 1
        yield state

    @doodad
    def test3(state):
        test[3] += 1
        yield state

    db.add_doodad('connection', test0)
    db.add_doodad('parsing', test1)

    async with db.get_connection() as conn:
        conn.add_doodad('execution', test2)
        await conn.fetchone('select 1')
        assert(test[0] == 1)
        assert(test[1] == 1)
        assert(test[2] == 1)
        assert(test[3] == 0)
        conn.add_doodad('execution', test3)
        await conn.fetchone('select 1')
        assert(test[1] == 2)
        assert(test[2] == 2)
        assert(test[3] == 1)

    test[1] = 0
    test[2] = 0
    test[3] = 0
    async with db.get_connection() as conn:
        await conn.fetchone('select 1')
        assert(test[1] == 1)
        assert(test[2] == 0)
        assert(test[3] == 0)
