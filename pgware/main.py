import inspect
import logging
import time
import uuid
from collections import defaultdict, namedtuple
from enum import Flag, auto

from .exceptions import (
    ProgrammingError,
)
from .utils import (
    config_map,
    raise_,
)


# ################################################################### Internals
# #############################################################################
class DD(Flag):
    """
    Debug pinpointing using bit flags.
    Generic & specific flags are checked by XOR,
    so one can:
    - include single components
    - select all and exclude components
    """
    # Generic
    DEEP = 0
    # Component Specific
    BUILDER = 0
    CONTEXTMANAGER = 0
    STATE = 0  # execution pipeline state
    OPS = 0  # execution pipeline
    HELPERS = 0  # activity from helpers (providers, doodads, ..)
    ADAPTERS = 0  # activity from adapters


MAX_STAGE_RETRIES = 1
MAX_TOTAL_RETRIES = 3
CONTEXT_STAT_INTERVAL_SECONDS = 60


def logger_setup():
    """
    Creates a logger object and returns it
    """
    logging.basicConfig(level=logging.DEBUG)
    return logging.getLogger(__name__)


LOGGER = logger_setup()


# ############################################################# Data Structures
# #############################################################################
Setup = namedtuple(
    'setup',
    'client, op_list, cmd_dict'
)


class Context(Flag):
    """
    Flag Enum (bitmask) for query execution context flags
    """
    CURSOR = auto()
    PREPARED = auto()
    SINGLE = auto()
    TRANSACTION = auto()
    POOLED = auto()
    QUERY_ARGS_POSTGRESQL = auto()
    QUERY_ARGS_PSYCOPG2 = auto()
    OUTPUT_DICT = auto()
    OUTPUT_LIST = auto()
    OUTPUT_NATIVE = auto()
    JSON = auto()


class State():
    """
    State object that get's passed between the functions and represents
    the state of the execution pipeline.
    It has the following public attributes:

    - connection: db connection
    - transaction: transaction id, if used
    - cursor: db cursor, if used
    - result: raw result from backend
    - prepared: preparad statement pointer
    - store: a key-value store (dict)
    - query: sql query to be parsed/executed
    - values: values to be integrated into the query
    - valuelist: list of values, when using `executemany`
    - context: the context which the pipeline is executed in
    - retries: the number of times the execution pipeline has been retried
    - done: interal list of executed steps
    - pool: pool of connections to use
    - loop: current event loop (for sync operation)

    """
    __slots__ = ['connection', 'transaction', 'cursor', 'result', 'prepared', 'done',
                 'pool', 'store', 'query', 'values', 'context', 'retries', 'valuelist',
                 'loop', '_me', '_v', '_context']

    def __init__(self, connection=None, cursor=None, result=None,  # pylint: disable=too-many-arguments
                 store=None, query=None, values=None, done=None, pool=None, valuelist=None,
                 transaction=None, prepared=None, context=Context(False), loop=None):
        # _me and _v serve for debugging purpouses
        object.__setattr__(self, '_me', uuid.uuid4())
        object.__setattr__(self, '_v', 0)
        object.__setattr__(self, '_context', context)
        self._me = uuid.uuid4()
        self.pool = pool
        self.connection = connection
        self.cursor = cursor
        self.result = result
        self.store = store if store is not None else {}
        self.done = done if done is not None else []
        self.query = query
        self.values = values
        self.valuelist = valuelist
        self.context = context
        self.transaction = transaction
        self.prepared = prepared
        self.loop = loop
        self.retries = {'total': 0, 'stage': 0}

    def status(self):
        print(f'#### {self._me} status dump:')
        for k in self.__slots__:
            val = object.__getattribute__(self, k)
            print(f"\t{k} => {val}")

    def __getattr__(self, _attr):
        return None

    def __getattribute__(self, attr):
        if DD.STATE ^ DD.DEEP:
            _v = object.__getattribute__(self, '_v')
            _me = object.__getattribute__(self, '_me')
            print(f"\tS => {attr} ({_me}:{_v})")
        return object.__getattribute__(self, attr)

    def __setattr__(self, attr, value):
        if DD.STATE ^ DD.DEEP:
            _v = object.__getattribute__(self, '_v')
            object.__setattr__(self, '_v', _v + 1)
            _me = object.__getattribute__(self, '_me')
            print(f"\tS <= {attr} ({_me}: {_v} => {_v+1})")
        object.__setattr__(self, attr, value)

    def clean(self, to_clean=['transaction', 'cursor', 'result', 'prepared', 'query', 'values']):
        LOGGER.debug('Cleansing state')
        for key in to_clean:
            object.__setattr__(self, key, None)
        keep_done = ['single_connect']
        self.done = list(set(keep_done) & set(self.done))
        self.retries = {'total': 0, 'stage': 0}
        self.context = object.__getattribute__(self, '_context')


# ################################################################# Exposed API
# #############################################################################
def build(client='psycopg2', *, connection_type='single', output='list',  # pylint: disable=too-many-statements
          param_format='native', auto_json=True, extensions=None,
          dbname=None, **kwargs):
    """
    Initialize config and context and return a pgware builder instance

    **Parameters:**

    client: str [psycopg2, asyncpg, ...]
        The client you wish to use
    connection_type: str [single, pooled]
        Single connection or a pooled one
    output: str [list, dict, native]
        Output results as a list or a dict, or as the native format
    param_format: str [native, psycopg2, asyncpg/postgresql]
        Query parameter format (%s is psycopg2, $1 is asyncpg/postgresql)
        native depends on the chosen client
    auto_json: bool
        Whether to try to parse given lists and dicts as json
        (mostly works, but can cause trouble when working with postgresql arrays)
    extensions: list
        List of extensions to enable (see client spec for list)
    special: dict
        Special values used by clients for specific/custom behaviour and settings
    kwargs:
        Postgresql connection settings

    **Returns:**

    _PgwareBuilder object
        Returns instance of the pgware builder class. The builder allows
        for some operations to happen, and exposes the connect/cursor methods
        to enter PgWare's contextmanager
    """
    LOGGER.info('Building pgware for %s:%s', client, connection_type)
    from .client import psycopg2_client as pg2
    from .client import asyncpg_client as apg
    op_list = defaultdict(list)

    if client not in ['psycopg2', 'asyncpg']:
        msg = f"Backend '{client}' not known / unsupported"
        raise ProgrammingError(msg)

    if kwargs.get('database') is None and dbname is not None:
        # Recover if config is still psycopg2 style
        kwargs['database'] = dbname

    extensions = [] if extensions is None else extensions

    # Build context flags
    context = Context(False)
    if connection_type == 'single':
        context |= context.SINGLE
    elif connection_type == 'pooled':
        context |= context.POOLED
    if param_format == 'native':
        if client == 'psycopg':
            context |= context.QUERY_ARGS_POSTGRESQL
        if client == 'psycogp2':
            context |= context.QUERY_ARGS_PSYCOPG2
    elif param_format in ['postgresql', 'asyncpg']:
        context |= context.QUERY_ARGS_POSTGRESQL
    elif param_format == 'psycopg2':
        context |= context.QUERY_ARGS_PSYCOPG2
    if output == 'dict':
        context |= context.OUTPUT_DICT
    elif output == 'list':
        context |= context.OUTPUT_LIST
    else:
        context |= context.OUTPUT_NATIVE
    if auto_json:
        context |= context.JSON

    # Preload specific client pipelines according to context flags
    if client == 'psycopg2':
        backend = pg2
        if context & context.SINGLE:
            op_list['connection'] = [pg2.single_connect(), pg2.cursor()]
        if context & context.OUTPUT_DICT or context & context.OUTPUT_LIST:
            op_list['result'] = [pg2.convert_result()]
        op_list['parsing'] = [pg2.convert_input()]
    elif client == 'asyncpg':
        backend = apg
        if context & context.SINGLE:
            op_list['connection'] = [apg.single_connect()]
        if context & context.POOLED:
            op_list['connection'] = [apg.pool_connect(), apg.acquire()]
        if context & context.OUTPUT_DICT or context & context.OUTPUT_LIST:
            op_list['result'] = [apg.convert_result()]
        if context & context.QUERY_ARGS_PSYCOPG2:
            op_list['parsing'] = [apg.convert_input()]
            extensions += ['str2dt', 'str2str']
        if context & context.JSON:
            extensions.append('json')

    # Test context flags against chosen backend support
    if (context & backend.__supports__) != context:
        missing = (backend.__supports__ ^ context) & context
        missing_str = ', '.join([n for n, x in Context.__members__.items() if x & missing])
        msg = f'Selected adapter (for {client}) does not support desired context ({missing_str})'
        LOGGER.warning(msg)
        raise ProgrammingError(msg)
    else:
        supported_str = ', '.join([n for n, x in Context.__members__.items() if x & context])
        LOGGER.info('Client supports desired context (%s)', supported_str)

    cfg_map = backend.__config_map__(context)
    return _PgwareBuilder(
        setup=Setup(
            client=backend,
            op_list=op_list,
            cmd_dict={}
        ),
        state=State(
            store={
                'setup': config_map(cfg_map, kwargs),
                'app_name': kwargs.get('app_name', 'pgware'),
                'extensions': extensions,
                'special': kwargs.get('special', {})
            },
            context=context
        )
    )


def connect(*args, **kwargs):
    """
    Alias for build
    """
    return build(*args, **kwargs)


# ##################################################################### CLASSES
# #############################################################################

# ##################################################################### BUILDER
class _PgwareBuilder():
    """
    Root context manager, providing the pgware context
    """

    def __init__(self, *, setup, state):
        # Initialize pgware object with the provided data
        if DD.DEEP ^ DD.BUILDER:
            print('$$ Pgware Building with state:')
            state.status()
        self._setup = setup
        self._state = state
        self._meta = {
            'timer': time.time(),
            'operation_cntr': 0,
            'context_cntr': 0,
            'stage_retries_cntr': 0,
            'total_retries_cntr': 0,
        }

    def get_connection(self, cursor=False):
        # Return pgware object context manager
        self._stats()
        return _PgwareContext(
            setup=self._setup,
            state=self._state,
            cursor=False,
            meta=self._meta,
        )

    def raw_connection(self, cursor=False):
        # Return raw, uncontextualised pgw object
        self._stats()
        return _PgwareContext(
            setup=self._setup,
            state=self._state,
            cursor=False,
            meta=self._meta,
        ).raw()

    async def close_all(self):
        LOGGER.debug('PGWare closing connection')
        await self._setup.client.close_connection(self._state)

    def preheat(self):
        """
        Preheat lazy connection acquisition, avoid doing
        it later on and incuring timeout penalty
        """
        if DD.DEEP ^ DD.BUILDER:
            print(f'-- Preheating pgware')
        with self.get_connection() as pgw:
            pgw.preheat()
            # hacky but the job get's done
            pgw.execute('SELECT 1')
            LOGGER.info('Preheated')

    async def preheat_async(self):
        """
        Preheat lazy connection acquisition in async mode, avoid doing
        it later on and incuring timeout penalty
        """
        if DD.DEEP ^ DD.BUILDER:
            print(f'-- Preheating pgware in async')
        async with self.get_connection() as pgw:
            await pgw.preheat()
            # hacky but the job get's done
            await pgw.execute('SELECT 1')
            LOGGER.info('Preheated (async)')

    def _stats(self, force=False):
        obj = self._meta
        obj['context_cntr'] += 1
        now = time.time()
        if force or now - obj['timer'] > CONTEXT_STAT_INTERVAL_SECONDS:
            delta = now - obj['timer']
            LOGGER.info(
                'stats: contexts @ %s (%s/sec) - ops @ %s (%s/sec) - total/stage retries @ %s/%s (%s/sec)',
                obj['context_cntr'],
                round(self._stats_cntr_delta('context_cntr') / delta, 2),
                obj['operation_cntr'],
                round(self._stats_cntr_delta('operation_cntr') / delta, 2),
                obj['total_retries_cntr'],
                obj['stage_retries_cntr'],
                round((self._stats_cntr_delta('total_retries_cntr') + self._stats_cntr_delta('stage_retries_cntr')) / delta, 2),
            )
            obj['timer'] = now
            if 'prev_cntr' not in obj:
                obj['prev_cntr'] = {}
            for cntr in ['context_cntr', 'operation_cntr', 'total_retries_cntr', 'stage_retries_cntr']:
                obj['prev_cntr'][cntr] = obj[cntr]

    def _stats_cntr_delta(self, name):
        curr = self._meta
        prev = curr.get('prev_cntr', {})
        return curr.get(name, 0) - prev.get(name, 0)

    def __getattr__(self, name):
        if name == 'backend':
            return self._setup.client.__backend__

        raise AttributeError(f'pgware builder has no attribute {name}')

    def add_doodad(self, stage, job, err_handler=lambda e, i: raise_(e)):
        """
        Add a doodad (small decorator) to one of the base providers.
        A doodad must is a generator that receive the provider's input,
        yield it to the following operation (another doodad or ultimately
        the provider itself), and clean up when they regain control

        At this step, doodads are "general": they will act on all queries,
        regardles of context.

        @step: {'connection', 'parsing', 'execution', 'result', 'errors'}
        @job: Function that takes input

        """
        if inspect.isasyncgenfunction(job):
            doodad = ('doodad', job, err_handler, False, True)
        else:
            doodad = ('doodad', job, err_handler, False, False)
        if stage == 'connection':
            self._setup.op_list['connection'].insert(0, doodad)
        elif stage == 'parsing':
            self._setup.op_list['parsing'].insert(0, doodad)
        elif stage == 'execution':
            self._setup.op_list['execution'].insert(0, doodad)
        elif stage == 'result':
            self._setup.op_list['result'].insert(0, doodad)
        elif stage == 'errors':
            self._setup.op_list['errors'].insert(0, doodad)
        else:
            raise AttributeError(f'Doodad for stage {stage} not supported')


# ############################################################## CONTEXTMANAGER
class _PgwareContext():
    """
    Context manager, responsible for async/sync detection,
    instantiation and cleanup
    """

    def __init__(self, **kwargs):
        self._params = kwargs
        self._pgware = None
        # print(f'Inited with {self._params}')

    def cursor(self):
        """
        Return a pgware context manager with the cursor property activated.
        """
        self._params['cursor'] = True
        return self

    def raw(self):
        from .pgw import _Pgware
        self._pgware = _Pgware(sync=True, **self._params)
        return self._pgware

    def __enter__(self):
        from .pgw import _Pgware
        if DD.CONTEXTMANAGER:
            print(f'Entered with {self._params}')
        self._pgware = _Pgware(sync=True, **self._params)
        return self._pgware

    def __exit__(self, ex_type, value, traceback):
        if not self._pgware.closed:
            self._pgware.close_context_sync()
        if ex_type is None:
            if DD.CONTEXTMANAGER:
                print('-----------------------')
                LOGGER.info('Pgware exited cleanly')
        else:
            if DD.CONTEXTMANAGER:
                LOGGER.warning('type: %s, value: %s, traceback: %s',
                               ex_type, value, traceback)

    async def __aenter__(self):
        from .pgw import _Pgware
        self._pgware = _Pgware(sync=False, **self._params)
        if DD.CONTEXTMANAGER:
            print(f'Entered with {self._params}')
        return self._pgware

    async def __aexit__(self, ex_type, value, traceback):
        if not self._pgware.closed:
            await self._pgware.close_context()
        if ex_type is None:
            if DD.CONTEXTMANAGER:
                print('-----------------------')
                LOGGER.info('Pgware exited cleanly')
        else:
            if DD.CONTEXTMANAGER:
                LOGGER.warning('type: %s, value: %s, traceback: %s',
                               ex_type, value, traceback)
