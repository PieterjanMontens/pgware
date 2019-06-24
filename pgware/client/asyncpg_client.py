import json

import asyncpg
# from psycopg2.extras import Json as pgJson
import dateutil.parser

from pgware import (
    DD,
    Context as C,
    ProgrammingError,
    QueryError,
    logger,
    provider,
    ps2pg,
)

"""
PGWare ascyncpg client definition file
"""
# ## API for integration with PGWare
__backend__ = 'asyncpg'
__supports__ = C.CURSOR | C.SINGLE | C.QUERY_ARGS_POSTGRESQL | C.OUTPUT_DICT | C.PREPARED | C.QUERY_ARGS_PSYCOPG2 | C.JSON | C.POOLED | C.OUTPUT_LIST | C.OUTPUT_NATIVE


# Map format: {ADAPTER_KEY: [PGWARE_KEY, DEFAULT_VALUE] | ...}
def __config_map__(context):
    if context & context.POOLED:
        return {
            'database': ['database', None],
            'user': ['user', None],
            'password': ['password', None],
            'host': ['host', None],
            'port': ['port', None],
            'timeout': ['timeout', 3],
            'command_timeout': ['command_timeout', 5],
            'min_size': ['min_size', 1],
            'max_size': ['max_size', 5]
        }
    else:
        return {
            'database': ['database', None],
            'user': ['user', None],
            'password': ['password', None],
            'host': ['host', None],
            'port': ['port', None],
            'timeout': ['timeout', 3],
            'command_timeout': ['command_timeout', 5]
        }
# ##


class Extensions():
    @staticmethod
    async def apply(name, connection):
        logger.debug('Extension "%s" enabled', name)
        if name == 'json':
            await Extensions.json(connection)
        if name == 'json_out':
            await Extensions.json_out(connection)
        if name == 'dec2float':
            await Extensions.dec2float(connection)
        if name == 'str2dt':
            await Extensions.str2dt(connection)
        if name == 'str2str':
            await Extensions.str2str(connection)

    @staticmethod
    async def json(connection):
        await connection.set_type_codec(
            'json', encoder=json.dumps, decoder=json.loads, schema='pg_catalog'
        )
        await connection.set_type_codec(
            'jsonb', encoder=json.dumps, decoder=json.loads, schema='pg_catalog'
        )

    @staticmethod
    async def json_out(connection):
        await connection.set_type_codec(
            'json', encoder=lambda x: x, decoder=json.loads, schema='pg_catalog'
        )
        await connection.set_type_codec(
            'jsonb', encoder=lambda x: x, decoder=json.loads, schema='pg_catalog'
        )

    @staticmethod
    async def dec2float(connection):
        await connection.set_type_codec(
            'numeric', encoder=lambda value: float(value) if value is not None else None, decoder=lambda value: float(value) if value is not None else None, schema='pg_catalog'
        )

    @staticmethod
    async def str2dt(connection):
        await connection.set_type_codec(
            'timestamp', encoder=lambda x: x.isoformat(), decoder=dateutil.parser.parse, schema='pg_catalog'
        )
        await connection.set_type_codec(
            'timestamptz', encoder=lambda x: x.isoformat(), decoder=dateutil.parser.parse, schema='pg_catalog'
        )

    @staticmethod
    async def str2str(connection):
        await connection.set_type_codec(
            'varchar', encoder=str, decoder=str, schema='pg_catalog'
        )
        # await state.connection.set_type_codec(
        #     'int2', encoder=lambda x: str(x), decoder=lambda x: x, schema='pg_catalog'
        # )
        # await state.connection.set_type_codec(
        #     'int4', encoder=lambda x: str(x), decoder=lambda x: x, schema='pg_catalog'
        # )
        # await state.connection.set_type_codec(
        #     'int8', encoder=lambda x: str(x), decoder=lambda x: x, schema='pg_catalog'
        # )


async def default_error_handler(ex, state):
    """
    Handle adapter-specific exceptions and tell pgware
    return pgware-specific exceptions if applicable

    e: the exception
    state: current state of pgware execution loop

    return: exception | None
    """
    if state.transaction:
        await state.transaction.rollback()
        logger.warning('Asyncpg transaction rolled back')
    if (isinstance(ex, (
            asyncpg.PostgresSyntaxError,
            asyncpg.exceptions.SyntaxOrAccessError))):
        logger.exception(ex)
        return QueryError(str(ex))
    if (isinstance(ex, (
            TypeError))):
        logger.exception(ex)
        return ProgrammingError(ex)
    if (not isinstance(ex, (
            ConnectionRefusedError,
            ConnectionResetError,
            asyncpg.PostgresError,
            asyncpg.PostgresConnectionError,
            asyncpg.InternalClientError))):
        # Unhandled exception, raise it
        logger.exception(ex)
        return ProgrammingError(str(ex))
    return None


async def close_context(state):
    if state.transaction:
        await state.transaction.commit()
    if state.pool:
        await state.pool.release(state.connection)


async def close_connection(state):
    if state.pool:
        logger.debug('Closing asyncpg pooled connections')
        await state.pool.close()
    elif state.connection:
        logger.debug('Closing asyncpg connection')
        await state.connection.close()


@provider(reuse=True)
def single_connect():
    async def job(state):
        logger.debug('asyncpg single connect initiating')
        s_settings = {'application_name': state.store['app_name']}
        state.connection = await asyncpg.connect(
            server_settings=s_settings,
            **state.store['setup']
        )
        for ext in state.store.get('extensions', []):
            await Extensions.apply(ext, state.connection)
        state.store['temp_exec'] = False
        yield state

    return job, default_error_handler


@provider(reuse=True)
def pool_connect():
    async def job(state):
        logger.debug('asyncpg connection pool initiating')

        async def con_setup(connection):
            for ext in state.store.get('extensions', []):
                await Extensions.apply(ext, connection)

        s_settings = {'application_name': state.store['app_name']}
        state.pool = await asyncpg.create_pool(
            server_settings=s_settings,
            init=con_setup,
            **state.store['setup']
        )
        state.store['temp_exec'] = False
        yield state

    return job, default_error_handler


@provider()
def acquire():
    async def job(state):
        logger.debug('asyncpg acquiring connection')
        state.connection = await state.pool.acquire()
        yield state

    return job, default_error_handler


@provider(reuse=True)
def cursor():
    async def job(state):
        state.context |= state.context.TRANSACTION
        state.transaction = state.connection.transaction()
        await state.transaction.start()
        yield state

    return job, default_error_handler


async def _cursor(state):
    args = [] if state.values is None else state.values
    if state.context & state.context.PREPARED:
        if state.query is not None:
            await _prepare(state)
        state.cursor = await state.prepared.cursor(*args)
    else:
        if state.query is not None:
            state.cursor = await state.connection.cursor(state.query, *args)


@provider(reuse=True)
def prepare():
    async def job(state):
        await _prepare(state)
        yield state

    return job, default_error_handler


async def _prepare(state):
    if state.query is not None:
        state.prepared = await state.connection.prepare(state.query)
        state.store['prepared_query'] = state.query
        logger.debug('Storing prepared query %s', state.query)
        state.query = None


@provider()
def executemany():

    async def job(state):
        state.result = await state.connection.executemany(
            state.query,
            state.valuelist
        )
        state.valuelist = None

        yield state

    return job, default_error_handler


@provider()
def execute():

    async def job(state):
        args = [] if state.values is None else state.values
        ctxt = state.context
        if ctxt & ctxt.CURSOR:
            await _cursor(state)
            state.store['temp_exec'] = True
            results = 1000 if 'n' not in state.store['special'] else state.store['special']['n']
            logger.warning('Cursor implementation limited: only %s rows retrieved', results)
            state.result = await state.cursor.fetch(n=10)
        elif ctxt & ctxt.PREPARED:
            await _prepare(state)
            state.store['temp_exec'] = True
            state.result = await state.prepared.fetch(*args)
        else:
            if state.query is None:
                raise ProgrammingError('Impossible method call: missing query')
            state.result = await state.connection.execute(state.query, *args)

        yield state

    return job, default_error_handler


@provider()
def fetchval():
    async def job(state):
        if state.store['temp_exec'] and state.values is None and state.query is None:
            state.result = state.result[0][0]
        else:
            args = [] if state.values is None else state.values
            ctxt = state.context
            if ctxt & ctxt.CURSOR:
                await _cursor(state)
                buff = await state.cursor.fetchrow()
                state.result = buff[0]
            elif ctxt & ctxt.PREPARED:
                await _prepare(state)
                state.result = await state.prepared.fetchval(*args)
            else:
                if state.query is None:
                    raise ProgrammingError('Impossible method call: missing cursor or query')
                state.result = await state.connection.fetchval(state.query, *args)
        state.store['temp_exec'] = False

        yield state

    return job, default_error_handler


@provider()
def fetchone():
    async def job(state):
        args = [] if state.values is None else state.values
        if state.store['temp_exec'] and state.values is None and state.query is None:
            state.result = state.result[0]
        else:
            state.status()
            ctxt = state.context
            if ctxt & ctxt.CURSOR:
                await _cursor(state)
                state.result = await state.cursor.fetchrow()
            elif ctxt & ctxt.PREPARED:
                await _prepare(state)
                state.result = await state.prepared.fetchrow(*args)
            else:
                if state.query is None:
                    raise ProgrammingError('Impossible method call: missing cursor or query')
                state.result = await state.connection.fetchrow(state.query, *args)
        state.store['temp_exec'] = False

        yield state

    return job, default_error_handler


@provider()
def fetchall():
    async def job(state):
        args = [] if state.values is None else state.values
        if state.store['temp_exec'] and state.values is None and state.query is None:
            state.result = state.result
        else:
            ctxt = state.context
            if ctxt & ctxt.CURSOR:
                await _cursor(state)
                results = 1000 if 'n' not in state.store['special'] else state.store['special']['n']
                logger.warning('Cursor implementation limited: only %s rows retrieved', results)
                state.result = await state.cursor.fetch(n=results)
            elif ctxt & ctxt.PREPARED:
                await _prepare(state)
                state.result = await state.prepared.fetch(*args)
            else:
                if state.query is None:
                    raise ProgrammingError('Impossible method call: missing cursor or query')
                state.result = await state.connection.fetch(state.query, *args)
        state.store['temp_exec'] = False

        yield state

    return job, default_error_handler


@provider()
def convert_result():
    def job(state):
        sco = state.context
        if DD.DEEP ^ DD.ADAPTERS:
            print(f'$$ Output conversion, context is {sco}')
        if state.result and isinstance(state.result, asyncpg.Record):
            if sco & sco.OUTPUT_DICT:
                if DD.DEEP ^ DD.ADAPTERS:
                    print(f'$$ Output conversion, converting to dict')
                state.result = dict(state.result)
            if sco & sco.OUTPUT_LIST:
                if DD.DEEP ^ DD.ADAPTERS:
                    print(f'$$ Output conversion, converting to list')
                state.result = list(state.result)
        elif state.result and isinstance(state.result, list):
            if sco & sco.OUTPUT_DICT:
                if DD.DEEP ^ DD.ADAPTERS:
                    print(f'$$ Output conversion, converting to dict')
                for i, row in enumerate(state.result):
                    state.result[i] = dict(row)
            if sco & sco.OUTPUT_LIST:
                if DD.DEEP ^ DD.ADAPTERS:
                    print(f'$$ Output conversion, converting to list')
                for i, row in enumerate(state.result):
                    print('Changed !')
                    state.result[i] = list(row)
        yield state

    return job, default_error_handler


@provider()
def convert_input():
    def job(state):
        if state.values is not None:
            if DD.DEEP ^ DD.ADAPTERS:
                print(f'$$ Input conversion BEFORE: query "{state.query}" values "{state.values}"')
            if state.context & state.context.QUERY_ARGS_PSYCOPG2:
                if state.query and '$' not in state.query:
                    if DD.DEEP ^ DD.ADAPTERS:
                        print('$$ PS=>PG syntax conversion')
                    state.query, state.values = ps2pg(state.query, state.values)
                if 'prepared_query' in state.store and '$' not in state.store['prepared_query']:
                    if DD.DEEP ^ DD.ADAPTERS:
                        print('$$ PS=>PG syntax conversion')
                    state.store['prepared_query'], state.values = ps2pg(state.store['prepared_query'], state.values)
            if DD.DEEP ^ DD.ADAPTERS:
                print(f'$$ Input conversion AFTER: query "{state.query}" values "{state.values}"')

        yield state

    return job, default_error_handler
