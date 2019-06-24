import asyncio
import select
from collections import namedtuple
import psycopg2
import psycopg2.extensions
import psycopg2.extras
from psycopg2.extras import Json as pgJson
from pgware import (
    provider,
    UnrecoverableError,
    logger,
    Context as C,
    pg2ps,
    DD,
)

"""
PGWare asynchronous psycopg2 client definition file

WORK IN PROGRESS
"""

# ## API for integration with PGWare
__backend__ = 'asyncpsycopg2'
__supports__ = C.CURSOR | C.SINGLE | C.OUTPUT_DICT | C.QUERY_ARGS_POSTGRESQL | C.QUERY_ARGS_PSYCOPG2 | C.JSON | C.POOLED


# Map format: {ADAPTER_KEY: [PGWARE_KEY, DEFAULT_VALUE] | ...}
def __config_map__(_context):
    return {
        'dbname': ['database', None],
        'user': ['user', None],
        'password': ['password', None],
        'host': ['host', None],
        'port': ['port', None],
        'connect_timeout': ['timeout', 3],
        'application_name': ['app_name', 'pgware'],
        'max_size': ['max_size', 5],
        # 'XXXX' to force default value in config dict
        'async': ['XXXX', True]
    }
# ##


PoolCon = namedtuple(
    'PoolCon',
    'free, conn'
)


class Extensions():
    @staticmethod
    def apply(name, state):
        logger.debug('Extension "%s" enabled', name)
        if name == 'dec2float':
            Extensions.dec2float(state)

    @staticmethod
    def dec2float(_s):
        dec2float = psycopg2.extensions.new_type(
            psycopg2.extensions.DECIMAL.values,
            'DEC2FLOAT',
            lambda value, curs: float(value) if value is not None else None)
        psycopg2.extensions.register_type(dec2float)


def default_error_handler(ex, state):
    """
    Handle adapter-specific exceptions and tell pgware
    return pgware-specific exceptions if applicable

    e: the exception
    state: current state of pgware execution loop

    return: exception | None
    """
    if isinstance(ex, psycopg2.ProgrammingError):
        logger.warning('Programming error not recoverable')
        return UnrecoverableError(ex)
    return None


async def close_context(state):
    pass


async def close_connection(state):
    if state.connection:
        logger.debug('Closing psycopg2 connection')
        state.connection.close()


async def wait_async(status=None, *, connection=None):
    conn = status.connection if status else connection
    loop = asyncio.get_event_loop()
    while 1:
        state = conn.poll()
        if state == psycopg2.extensions.POLL_OK:
            break
        elif state == psycopg2.extensions.POLL_READ:
            await loop.run_in_executor(None, lambda: select.select([conn.fileno()], [], []))
        elif state == psycopg2.extensions.POLL_WRITE:
            await loop.run_in_executor(None, lambda: select.select([], [conn.fileno()], []))
        else:
            raise psycopg2.OperationalError("bad state from poll: %s" % state)


@provider(reuse=True)
def single_connect():
    def job(state):
        state.connection = psycopg2.connect(**state.store['setup'])
        state.connection.autocommit = True
        for ext in state.store.get('extensions', []):
            Extensions.apply(ext, state)
        yield state

    return job, default_error_handler


@provider(reuse=True)
def pool_connect():
    async def job(state):
        logger.debug('psycopg2 connection pool initiating')

        size = state.store['max_size']
        del state.store['max_size']
        for _ in range(0, size):
            newc = psycopg2.connect(**state.store['setup'])
            await wait_async(connection=newc)
            state.pool.append(PoolCon(free=True, conn=newc))

        yield state

    return job, default_error_handler


@provider()
def acquire():
    async def job(state):
        logger.debug('psycogp2 acquiring connection')
        pooll = len(state.pool)
        for i in range(0, pooll):
            conn = state.pool[i]
            state = conn.poll()
            if state == psycopg2.extensions.POLL_OK:
                state.connection = conn
            await asyncio.sleep(.001)

    return job, default_error_handler


@provider(reuse=True)
def cursor():
    async def job(state):
        state.cursor = state.connection.cursor(
            cursor_factory=psycopg2.extras.RealDictCursor
        )
        await wait_async(state)
        yield state

    return job, default_error_handler


@provider()
def execute():
    async def job(state):
        state.cursor.execute(state.query, state.values)
        await wait_async(state)
        yield state

    return job, default_error_handler


@provider()
def fetchval():
    async def job(state):
        row = state.cursor.fetchone()
        await wait_async(state)
        if isinstance(row, dict):
            row = list(row.values())
        state.result = row[0]
        yield state

    return job, default_error_handler


@provider()
def fetchone():
    async def job(state):
        if state.query is not None:
            state.cursor.execute(state.query, state.values)
            await wait_async(state)
        state.result = state.cursor.fetchone()
        yield state

    return job, default_error_handler


@provider()
def fetchall():
    async def job(state):
        state.result = state.cursor.fetchall()
        yield state

    return job, default_error_handler


@provider()
def convert_input():
    def job(state):
        if state.values is not None:
            if DD.DEEP ^ DD.ADAPTERS:
                print(f'$$ Input conversion BEFORE: query "{state.query}" values "{state.values}"')
            if state.context & state.context.QUERY_ARGS_POSTGRESQL:
                if DD.DEEP ^ DD.ADAPTERS:
                    print('$$ PG=>PS syntax conversion')
                state.query, state.values = pg2ps(state.query, state.values)
            if state.context & state.context.JSON:
                if DD.DEEP ^ DD.ADAPTERS:
                    print('$$ JSON value conversion')
                if isinstance(state.values, dict):
                    state.values = {k: pgJson(v) if isinstance(v, (dict, list)) else v for k, v in state.values.items()}
                if isinstance(state.values, (list, tuple)):
                    state.values = [pgJson(v) if isinstance(v, (dict, list)) else v for v in state.values]
            if DD.DEEP ^ DD.ADAPTERS:
                print(f'$$ Input conversion AFTER: query "{state.query}" values "{state.values}"')
        yield state

    return job, default_error_handler
