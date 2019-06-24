#!/usr/bin/env python3
# pylint: skip-file

import asyncio
import logging
import timeit

import asyncpg
import numpy as np
import psycopg2
import psycopg2.extras

import pgware as pgware

logging.basicConfig(level=logging.WARNING)

psy_config = {
    'database': '[DB]',
    'user': '[USER]',
    'password': None,
    'host': '[HOST]',
    'port': None
}

asy_config = {
    'database': '[DB]',
    'user': '[USER]',
    'password': None,
    'host': '[HOST]',
    'port': None,
}


SQL = 'select * from user limit {i}'
global QUERIES
QUERIES = 1


def psycopg2_test():
    global QUERIES
    db = psycopg2.connect(**psy_config)
    with db.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        for _i in range(QUERIES):
            cur.execute(SQL.format(i=_i))
            cur.fetchone()


def pgware_psycopg2_test():
    global QUERIES
    db = pgware.build(type='single', output='dict', client='psycopg2', **psy_config)
    with db.connect() as cur:
        for _i in range(QUERIES):
            cur.execute(SQL.format(i=_i))
            cur.fetchone()


async def pgware_psycopg2_ascyn_test():
    global QUERIES
    db = pgware.build(type='single', output='dict', client='psycopg2', **psy_config)
    async with db.connect() as cur:
        for _i in range(QUERIES):
            await cur.execute(SQL.format(i=_i))
            await cur.fetchone()
    await db.close()


async def asyncpg_test():
    global QUERIES
    db = await asyncpg.connect(**asy_config)
    for _i in range(QUERIES):
        await db.fetchrow(SQL.format(i=_i))
    await db.close()


async def pgware_asyncpg_test():
    global QUERIES
    db = pgware.build(
        auto_json=False,
        type='single',
        output='dict',
        client='asyncpg',
        **asy_config)
    async with db.connect() as conn:
        for _i in range(QUERIES):
            await conn.fetchone(SQL.format(i=_i))
    await db.close()


def a2_test():
    asyncio.run(pgware_psycopg2_ascyn_test())


def a_test():
    asyncio.run(asyncpg_test())


def ap_test():
    asyncio.run(pgware_asyncpg_test())


def test(mask, fun, number, repeat):
    raw = timeit.repeat(fun, number=number, repeat=repeat)
    res = reject_outliers(raw)
    score = round(np.mean(res), 4)
    mask += ' ({ln}/{lnf})'
    print(mask.format(score=score, ln=len(res), lnf=len(raw)))
    # print(f'raw: {raw} filt: {res}')
    return len(res), round(np.mean(res), 4)


def reject_outliers(data, m=2.):
    d = np.abs(data - np.median(data))
    mdev = np.median(d)
    s = d / mdev if mdev else 0.
    # print(f'{data} s:{s} m:{m} d:{d} mdev:{mdev}')
    out = []
    for i, sx in enumerate(s):
        if sx < m:
            out.append(data[i])
    return out


QUERIES = 5
print(f'## Pre-heating runs..')
psycopg2_test()
pgware_psycopg2_test()
a2_test()
a_test()
ap_test()

QUERIES = 1
number = 10
repeat = 10
print(f'## Test score: average of {number} * ("{SQL}" * {QUERIES}), {repeat} repeats, outliers excluded')
test('Vanilla Psycopg2:\t{score}s', psycopg2_test, number, repeat)
test('PGWare Psycopg2:\t{score}s', pgware_psycopg2_test, number, repeat)
test('PGWare Psycopg2 Async:\t{score}s', a2_test, number, repeat)
test('Vanilla Asyncpg:\t{score}s', a_test, number, repeat)
test('PGWare Asyncpg:\t\t{score}s', ap_test, number, repeat)
QUERIES = 10
number = 1
repeat = 10
print(f'## Test score: average of {number} * ("{SQL}" * {QUERIES}), {repeat} repeats, outliers excluded')
test('Vanilla Psycopg2:\t{score}s', psycopg2_test, number, repeat)
test('PGWare Psycopg2:\t{score}s', pgware_psycopg2_test, number, repeat)
test('PGWare Psycopg2 Async:\t{score}s', a2_test, number, repeat)
test('Vanilla Asyncpg:\t{score}s', a_test, number, repeat)
test('PGWare Asyncpg:\t\t{score}s', ap_test, number, repeat)
