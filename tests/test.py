#!/usr/bin/env python3
# pylint: skip-file

import asyncio
import logging

import pgware as pgware

logging.basicConfig(level=logging.DEBUG)

config = {
    'client': 'psycopg2',
    'database': '[DB]',
    'user': '[USER]',
    'password': None,
    'host': '[HOST]',
    'port': None,
    'connection_type': 'single'
}

SQL = "[SOME_SQL]"


async def main():
    print('----------------- TEST ASYNC -----------------------')
    print('----------------------------------------------------')
    db = pgware.build(output='dict', **config)
    # Test all methods in all possible combinations
    for i in range(0, 4000):
        async with db.connect() as pgw:
            await pgw.fetchall(SQL)


# db = pgware.build(output='record', **config)
# db.preheat()


# @contextmanager
# def state_status(state):
#     state.status()
#     yield state
#     state.status()
#
#
# db.add_doodad('connection', state_status)
# db.add_doodad('result', state_status)
#
# with db.connect() as conn:
#     for i in range(0, 1):
#         res = conn.execute(f'SELECT {i}')
#         out = res.fetchval()
#         print(f'########### Obtained value is {out}')
#         time.sleep(1)


asyncio.run(main())
