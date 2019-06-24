import random
import string

import psycopg2
import psycopg2.extensions
import psycopg2.extras
from psycopg2.extras import Json as pgJson

from pgware import (
    DD,
    Context as C,
    ProgrammingError,
    QueryError,
    logger,
    pg2ps,
    provider,
    ps2pg,
)

"""
PGWare psycopg2 client definition file
"""

# ## API for integration with PGWare
__backend__ = 'psycopg2'
__supports__ = C.CURSOR | C.SINGLE | C.PREPARED | C.OUTPUT_DICT | C.QUERY_ARGS_POSTGRESQL | C.QUERY_ARGS_PSYCOPG2 | C.JSON | C.OUTPUT_LIST | C.OUTPUT_NATIVE


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
    }
# ##


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


async def default_error_handler(ex, state):
    """
    Handle adapter-specific exceptions and tell pgware
    return pgware-specific exceptions if applicable

    e: the exception
    state: current state of pgware execution loop

    return: exception | None
    """
    if not isinstance(ex, psycopg2.Error):
        # Unhandled exception, raise it
        logger.exception(ex)
        return ProgrammingError(ex)
    if isinstance(ex, psycopg2.ProgrammingError):
        logger.warning('psycopg2: ProgrammingError not recoverable')
        logger.exception(ex)
        return QueryError(str(ex))
    if isinstance(ex, psycopg2.DataError):
        logger.warning('psycopg2: DataError not recoverable')
        return QueryError(str(ex))
    if isinstance(ex, psycopg2.OperationalError):
        logger.info('psycopg2: OperationalError occured, recovering')
    if isinstance(ex, psycopg2.InterfaceError):
        logger.info('psycopg2: InterfaceError occured, recovering')
    return None


async def close_context(state):
    if 'temp_exec' in state.store:
        del state.store['temp_exec']


async def close_connection(state):
    if state.connection:
        logger.debug('Closing psycopg2 connection')
        state.connection.close()


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
def cursor():
    def job(state):
        state.cursor = state.connection.cursor(
            cursor_factory=psycopg2.extras.DictCursor
        )
        yield state

    return job, default_error_handler


@provider(reuse=True)
def prepare():
    async def job(state):
        state.prepared = ''.join(random.choice(string.ascii_letters) for _ in range(10))
        prep_sql = f'PREPARE {state.prepared} AS {state.query}'
        state.cursor.execute(prep_sql)

        yield state

    return job, default_error_handler


@provider()
def execute():
    def job(state):
        _execute(state)
        state.store['temp_exec'] = True
        yield state

    return job, default_error_handler


@provider()
def executemany():

    async def job(state):
        i = 0
        for values in state.valuelist:
            state.cursor.execute(state.query, values)
            i += 1
        state.valuelist = None
        state.result = i

        yield state

    return job, default_error_handler


@provider()
def fetchval():
    def job(state):
        _execute(state)
        row = state.cursor.fetchone()
        # if isinstance(row, dict):
        #     row = list(row.values())
        state.result = row[0]
        yield state

    return job, default_error_handler


@provider()
def fetchone():
    def job(state):
        _execute(state)
        state.result = state.cursor.fetchone()
        yield state

    return job, default_error_handler


@provider()
def fetchall():
    def job(state):
        _execute(state)
        state.result = state.cursor.fetchall()
        yield state

    return job, default_error_handler


@provider()
def convert_result():
    """
    Author: Raphaël Dehousse
    """
    def job(state):
        sco = state.context
        if DD.DEEP ^ DD.ADAPTERS:
            print(f'$$ Output conversion, context is {sco}')
        if state.result:
            if state.context & state.context.OUTPUT_DICT:
                if isinstance(state.result, psycopg2.extras.DictRow):
                    state.result = dict(state.result)
                elif isinstance(state.result, list):
                    if isinstance(state.result[0], psycopg2.extras.DictRow):
                        for i, row in enumerate(state.result):
                            state.result[i] = dict(row)
            if state.context & state.context.OUTPUT_LIST:
                if DD.DEEP ^ DD.ADAPTERS:
                    print(f'$$ Output conversion, converting to list')
                if isinstance(state.result, psycopg2.extras.DictRow):
                    state.result = list(state.result)
                elif isinstance(state.result, list):
                    if isinstance(state.result[0], psycopg2.extras.DictRow):
                        for i, row in enumerate(state.result):
                            state.result[i] = list(row)

        yield state

    return job, default_error_handler


@provider()
def convert_input():
    def job(state):
        sco = state.context
        if DD.DEEP ^ DD.ADAPTERS:
            print(f'$$ Input conversion, context is {sco}')
        if state.values is not None:
            if DD.DEEP ^ DD.ADAPTERS:
                print(f'$$ Input conversion BEFORE: query "{state.query}" values "{state.values}":{type(state.values)}')
            if sco & sco.PREPARED and not sco & sco.QUERY_ARGS_POSTGRESQL and '$' not in state.query:
                if DD.DEEP ^ DD.ADAPTERS:
                    print('$$ PS=>PG syntax conversion')
                state.query, state.values = ps2pg(state.query, state.values)
            if sco & sco.QUERY_ARGS_POSTGRESQL and not sco & sco.PREPARED:
                if DD.DEEP ^ DD.ADAPTERS:
                    print('$$ PG=>PS syntax conversion')
                state.query, state.values = pg2ps(state.query, state.values)
            if sco & sco.JSON:
                if DD.DEEP ^ DD.ADAPTERS:
                    print('$$ JSON value conversion')
                if isinstance(state.values, dict):
                    state.values = {k: pgJson(v) if isinstance(v, (dict, list)) else v for k, v in state.values.items()}
                if isinstance(state.values, (list, tuple)):
                    state.values = [pgJson(v) if isinstance(v, (dict, list)) else v for v in state.values]
            if DD.DEEP ^ DD.ADAPTERS:
                print(f'$$ Input conversion AFTER: query "{state.query}" values "{state.values}"')
        elif state.valuelist is not None:
            if sco & sco.QUERY_ARGS_POSTGRESQL:
                state.query, _ = pg2ps(state.query, state.valuelist[0])

        yield state

    return job, default_error_handler


def _execute(state):
    if 'temp_exec' in state.store and state.store['temp_exec']:
        del state.store['temp_exec']
    if state.query is not None:
        if state.context & state.context.PREPARED:
            if state.values is not None:
                mask = ", ".join(['%s'] * len(state.values))
                exec_sql = f'EXECUTE {state.prepared} ({mask})'
                state.cursor.execute(exec_sql, state.values)
        else:
            state.cursor.execute(state.query, state.values)
