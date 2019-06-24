import asyncio
import copy
import inspect
from .main import (
    DD,
    LOGGER,
    Context,
    Setup,
    MAX_STAGE_RETRIES,
    MAX_TOTAL_RETRIES,
)
from .utils import (
    retuple,
    raise_,
    supports,
)
from .exceptions import (
    PrivateError,
    ProgrammingError,
    PublicError,
    RetriesExhausted,
    UnrecoverableError,
)


class _Pgware():
    """
    PGWare, advanced posgresql adaptor wrapper
    """

    # Internals
    # ########################################################################
    def __init__(self, setup, state, sync, cursor, meta):  # pylint: disable=too-many-arguments
        self._setup = Setup(
            client=setup.client,
            op_list=copy.deepcopy(setup.op_list),
            cmd_dict=setup.cmd_dict
        )
        self._meta = meta
        self._itercursor = 0
        self.closed = False

        if sync:
            LOGGER.debug('Sync pgware launched')
            self.execute = self.execute_sync
            self.fetchval = self.fetchval_sync
            self.fetchone = self.fetchone_sync
            self.fetchall = self.fetchall_sync
            self.executemany = self.executemany_sync
            self.prepare = self.prepare_sync
            self.close = self.close_context_sync
            self.preheat = self.preheat_sync
        else:
            self.close = self.close_context
        if cursor:
            # Cursor is redundant for psycopg2
            if not supports(self._setup.client, Context.CURSOR):
                raise ProgrammingError('Selected client does not support cursors')
            if DD.OPS ^ DD.DEEP:
                LOGGER.debug('Using cursor')
                print(f'=>  Cursor Enabled')
            if self._setup.client.__backend__ == 'asyncpg':
                LOGGER.debug('Adding cursor to connection pipeline')
                self._setup.op_list['connection'] += [(self._setup.client.cursor())]
            state.context |= state.context.CURSOR

        self._state = state

    def __getattr__(self, name):
        def wrap(*_args, **_kwargs):
            if 'fetch' in name:
                gname = name[:5] + '_' + name[5:]
                msg = f"Method {name}() not supported, try \"{gname}()\""
            else:
                msg = f"Called not existing method {name}"
            raise ProgrammingError(msg)
        return wrap

    def _incr(self, cntr):
        self._meta[cntr] += 1

    def _exec_ops_sync(self, pipeline):
        """
        Execute the pipeline in an event loop (pgware has been called
        in a synchronous context, we try to run it in an eventloop
        and return the results anyway)
        """
        state = self._state
        if DD.OPS ^ DD.DEEP:
            print(f'received pipeline {pipeline} (sync)')
        try:
            if state.loop is None:
                state.loop = asyncio.new_event_loop()
            # return asyncio.run(self._exec_opline(pipeline))
            return state.loop.run_until_complete(self._exec_opline(pipeline))
        except RuntimeError:
            LOGGER.critical('''Running sync PGWare within eventloop;
            please refactor to async/await use''')
            raise ProgrammingError(''''Running sync PGWare within eventloop;
            please refactor to async/await use''')

    async def _exec_ops(self, pipeline):
        if DD.OPS ^ DD.DEEP:
            print(f'received pipeline {pipeline} (async)')
        return await self._exec_opline(pipeline)

    async def _exec_opline(self, opline):
        """
        Execution of task pipeline.
        Tasks are ordered left to right in execution pipeline
        ex: [connect, acquire, cursor, execute, fetchval]
        Each operation executes itself and passes its result
        to the next in line, until it's done, basically a
        functional reduce operation, like a haskell foldl.
        """
        state = self._state
        if 'temp_exec' not in state.store or not state.store['temp_exec']:
            LOGGER.debug('Resetting state result')
            self._state.result = []
        stages = ['connection', 'parsing', 'execution', 'result', 'errors']

        while True:
            try:
                for stage in stages:
                    stage_ops = opline[stage]
                    if DD.OPS ^ DD.DEEP:
                        print(f'-- Launching stage {stage} {len(stage_ops)} context:{state.context}')
                    if not stage_ops:
                        continue
                    state.retries['stage'] = 0
                    state = await self._exec_stage(stage, stage_ops, state)
                break
            except (RetriesExhausted, PrivateError) as ex:
                if DD.OPS ^ DD.DEEP:
                    print(f'!! {stage} exception, retrying pipeline')
                if state.retries['total'] > MAX_TOTAL_RETRIES:
                    LOGGER.debug('Total retries (%s) exhausted allowable amount (%s)', state.retries['total'], MAX_TOTAL_RETRIES)
                    raise RetriesExhausted(
                        f'Exhaused total retries, abandoning'
                    )
                LOGGER.warning('Pipeline Stage exception: %s', ex)
                state.retries['total'] += 1
                await asyncio.sleep(state.retries['total'] * .5)
                self._incr('total_retries_cntr')
                state.done = []
            except Exception as ex:  # pylint: disable=broad-except
                LOGGER.debug('unrecoverable op exec exception (%s) - query: %s\tValues: %s', type(ex), self._state.query, self._state.values)
                if isinstance(ex, PublicError):
                    raise ex
                else:
                    LOGGER.exception(ex)
                    raise UnrecoverableError(f'unrecognized exception occured')

        self._state = state
        return self._state.result

    async def _exec_stage(self, name, ops, state):
        (jobname, job, err_handler, reuse, coroutine) = ops[0]
        if DD.OPS ^ DD.DEEP:
            print(f'#  {name}:{jobname}, reuse:{reuse}, coroutine:{coroutine}')
        while True:
            try:
                if reuse and (jobname in state.done):
                    if DD.OPS ^ DD.DEEP:
                        print(f'#  {name}:{jobname} already done')
                    out_state = await self._exec_stage(
                        name,
                        ops[1:],
                        state
                    ) if len(ops) > 1 else state
                else:
                    if DD.OPS ^ DD.DEEP:
                        print(f'#  {name}:{jobname} must be (re)done')
                    if coroutine:
                        async with job(state) as new_state:
                            out_state = await self._exec_stage(
                                name,
                                ops[1:],
                                new_state
                            ) if len(ops) > 1 else new_state
                        if DD.OPS ^ DD.DEEP:
                            print(f'#  {name}:{jobname} yielded async!')
                    else:
                        with job(state) as new_state:
                            out_state = await self._exec_stage(
                                name,
                                ops[1:],
                                new_state
                            ) if len(ops) > 1 else new_state
                        if DD.OPS ^ DD.DEEP:
                            print(f'#  {name}:{jobname} yielded sync!')
                    if reuse:
                        out_state.done.append(jobname)
                return out_state
            except asyncio.TimeoutError:
                if state.retries['stage'] > MAX_STAGE_RETRIES:
                    raise RetriesExhausted(name)
                LOGGER.warning('Asyncio timeout error, recovering')
                state.retries['stage'] += 1
                self._incr('stage_retries_cntr')
                await asyncio.sleep(state.retries['stage'] * .2)
            except Exception as ex:  # pylint: disable=broad-except
                if state.retries['stage'] > MAX_STAGE_RETRIES:
                    LOGGER.exception(ex)
                    raise RetriesExhausted(name)
                state.retries['stage'] += 1
                reraise = await err_handler(ex, state)
                LOGGER.warning('Stage exception: %s', ex)
                if DD.OPS ^ DD.DEEP:
                    print(f'!! {name}:{jobname} FAILED! retrying')
                if isinstance(reraise, Exception):
                    raise reraise

    # Iteration functionality
    # ########################################################################
    def __iter__(self):
        if not self._state.result:
            self.fetchall()
        self._itercursor = 0
        return self

    def __next__(self):
        if len(self._state.result) > self._itercursor:
            self._itercursor += 1
            return self._state.result[self._itercursor - 1]
        else:
            raise StopIteration

    def __aiter__(self):
        self._itercursor = 0
        return self

    async def __anext__(self):
        if not self._state.result:
            await self.fetchall()
        if len(self._state.result) > self._itercursor:
            self._itercursor += 1
            return self._state.result[self._itercursor - 1]
        else:
            raise StopAsyncIteration

    # Exposed API
    # ########################################################################
    def status(self):
        print(f'PGWare setup is : {self._params}')

    async def preheat(self):
        """
        Force open connections, instead of doing it lazily
        """
        oplist = copy.deepcopy(self._setup.op_list)
        await self._exec_ops(oplist)
        return self

    def preheat_sync(self):
        """
        Force open connections, instead of doing it lazily
        """
        oplist = copy.deepcopy(self._setup.op_list)
        self._exec_ops_sync(oplist)
        return self

    def close_context_sync(self):
        """
        Clean pgware's state and close all open connections.
        """
        try:
            self.closed = True
            return asyncio.run(self.close_context())
        except RuntimeError:
            LOGGER.critical('''Running sync PGWare within eventloop;
            please refactor to async/await use''')
            raise ProgrammingError(''''Running sync PGWare within eventloop;
            please refactor to async/await use''')

    async def close_context(self):
        """
        Clean pgware's state and close all open connections.
        """
        LOGGER.debug('Closing pgware context')
        client = self._setup.client
        self._state.clean()
        self.closed = True
        await client.close_context(self._state)

    def add_doodad(self, stage, job, err_handler=lambda e, i: raise_(e)):
        """
        Add a doodad (small decorator) to one of the base providers.
        A doodad must is a generator that receive the provider's input,
        yield it to the following operation (another doodad or ultimately
        the provider itself), and clean up when they regain control

        At this step, doodads are "local": they exist only in the current
        context

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

    # Interface
    # ########################################################################
    async def execute(self, q_p, par=None):
        """
        Execute a statement, with optional values

            [await] conn.execute('SELECT 1, 2');
            [await] conn.execute('SELECT $1, $2', (1, 2));
            [await] conn.execute('SELECT %s, %s', (1, 2));

        async or sync
        """
        LOGGER.debug('execute statement %s | %s', q_p, par)
        self._incr('operation_cntr')
        if isinstance(q_p, str) and self._state.context & Context.PREPARED:
            self._state.context = self._state.context ^ Context.PREPARED
        if self._state.context & Context.PREPARED:
            self._state.values = retuple(q_p)
        else:
            self._state.query, self._state.values = q_p, retuple(par)
        client = self._setup.client
        oplist = copy.deepcopy(self._setup.op_list)
        # FIXME: One option to do it, maybe better to let the client
        # patch the execution pipeline
        # oplist = cl.execute(oplist, self._state.context)
        oplist['execution'] += [client.execute()]
        oplist['result'] = []  # Execute statement ignores result parsing
        await self._exec_ops(oplist)
        return self

    def execute_sync(self, q_p, par=None):
        LOGGER.debug('execute statement %s | %s', q_p, par)
        self._incr('operation_cntr')
        if isinstance(q_p, str) and self._state.context & Context.PREPARED:
            self._state.context = self._state.context ^ Context.PREPARED
        if self._state.context & Context.PREPARED:
            self._state.values = retuple(q_p)
        else:
            self._state.query, self._state.values = q_p, retuple(par)
        client = self._setup.client
        oplist = copy.deepcopy(self._setup.op_list)
        # FIXME: One option to do it, maybe better to let the client
        # patch the execution pipeline
        # oplist = cl.execute(oplist, self._state.context)
        oplist['execution'] += [client.execute()]
        oplist['result'] = []  # Execute statement ignores result parsing
        self._exec_ops_sync(oplist)
        return self

    async def executemany(self, query, params):
        """
        Execute a statement for each set of parameters

            [await] conn.executemany('SELECT $1, $2', [(1, 2), (2, 3), (3, 4)]);
            [await] conn.executemany('SELECT %s, %s', [(1, 2), (2, 3), (3, 4)]);

        async or sync
        """
        self._incr('operation_cntr')
        self._state.query, self._state.valuelist = query, params
        client = self._setup.client
        oplist = copy.deepcopy(self._setup.op_list)
        oplist['execution'] += [client.executemany()]
        await self._exec_ops(oplist)
        return self

    def executemany_sync(self, query, params):
        self._incr('operation_cntr')
        self._state.query, self._state.valuelist = query, params
        client = self._setup.client
        oplist = copy.deepcopy(self._setup.op_list)
        oplist['execution'] += [client.executemany()]
        self._exec_ops_sync(oplist)
        return self

    async def prepare(self, query):
        """
        Prepare a statement for use later while supplying values

            [await] cur.prepare('SELECT $1, $2');

            [await] cur.execute((1, 2));
            [await] cur.fetchone((3, 4));

        async or sync
        """
        # prepare is already sync (lazy): just forward to it
        self.prepare_sync(query)
        return self

    def prepare_sync(self, query):
        LOGGER.debug('Preparing query %s', query)
        self._incr('operation_cntr')
        self._state.query = query
        client = self._setup.client
        if not supports(client, Context.PREPARED):
            raise ProgrammingError('Selected client does not support prepared statements (yet)')
        self._setup.op_list['execution'] += [client.prepare()]
        self._state.context |= self._state.context.PREPARED
        return self

    async def fetchall(self, q_p=None, par=None):
        """
        Fetch all results for a SQL query

            [await] conn.fetchall('SELECT $1, $2', ('foo', 'bar'));
            [await] conn.fetchall('SELECT %s, %s', ('foo', 'bar'));

        async or sync
        """
        LOGGER.debug('fetchall statement %s | %s', q_p, par)
        self._incr('operation_cntr')
        if isinstance(q_p, str) and self._state.context & Context.PREPARED:
            self._state.context = self._state.context ^ Context.PREPARED
        if self._state.context & Context.PREPARED:
            self._state.values = retuple(q_p)
        else:
            self._state.query, self._state.values = q_p, retuple(par)
        client = self._setup.client
        oplist = copy.deepcopy(self._setup.op_list)
        oplist['execution'] += [client.fetchall()]
        return await self._exec_ops(oplist)

    def fetchall_sync(self, q_p=None, par=None):
        LOGGER.debug('fetchall statement %s | %s', q_p, par)
        self._incr('operation_cntr')
        if isinstance(q_p, str) and self._state.context & Context.PREPARED:
            self._state.context = self._state.context ^ Context.PREPARED
        if self._state.context & Context.PREPARED:
            self._state.values = retuple(q_p)
        else:
            self._state.query, self._state.values = q_p, retuple(par)
        client = self._setup.client
        oplist = copy.deepcopy(self._setup.op_list)
        oplist['execution'] += [client.fetchall()]
        return self._exec_ops_sync(oplist)

    async def fetchone(self, q_p=None, par=None):
        """
        Fetch the first result row for a SQL query

            [await] conn.fetchone('SELECT $1, $2', ('foo', 'bar'));
            [await] conn.fetchone('SELECT %s, %s', ('foo', 'bar'));

        async or sync
        """
        LOGGER.debug('fetchone statement %s | %s', q_p, par)
        self._incr('operation_cntr')
        if isinstance(q_p, str) and self._state.context & Context.PREPARED:
            self._state.context = self._state.context ^ Context.PREPARED
        if self._state.context & Context.PREPARED:
            self._state.values = retuple(q_p)
        else:
            self._state.query, self._state.values = q_p, retuple(par)
        client = self._setup.client
        oplist = copy.deepcopy(self._setup.op_list)
        oplist['execution'] += [client.fetchone()]
        return await self._exec_ops(oplist)

    def fetchone_sync(self, q_p=None, par=None):
        LOGGER.debug('fetchone statement %s | %s', q_p, par)
        self._incr('operation_cntr')
        if isinstance(q_p, str) and self._state.context & Context.PREPARED:
            self._state.context = self._state.context ^ Context.PREPARED
        if self._state.context & Context.PREPARED:
            self._state.values = retuple(q_p)
        else:
            self._state.query, self._state.values = q_p, retuple(par)
        client = self._setup.client
        oplist = copy.deepcopy(self._setup.op_list)
        oplist['execution'] += [client.fetchone()]
        return self._exec_ops_sync(oplist)

    async def fetchval(self, q_p=None, par=None):
        """
        Fetch the first result value of the first result row for a SQL query

            [await] conn.fetchval('SELECT $1, $2', ('foo', 'bar'));
            [await] conn.fetchval('SELECT %s, %s', ('foo', 'bar'));

        async or sync
        """
        LOGGER.debug('fetchval statement %s | %s', q_p, par)
        self._incr('operation_cntr')
        if isinstance(q_p, str) and self._state.context & Context.PREPARED:
            self._state.context = self._state.context ^ Context.PREPARED
        if self._state.context & Context.PREPARED:
            self._state.values = retuple(q_p)
        else:
            self._state.query, self._state.values = q_p, retuple(par)
        client = self._setup.client
        oplist = copy.deepcopy(self._setup.op_list)
        oplist['execution'] += [client.fetchval()]
        return await self._exec_ops(oplist)

    def fetchval_sync(self, q_p=None, par=None):
        LOGGER.debug('fetchval statement %s | %s', q_p, par)
        self._incr('operation_cntr')
        if isinstance(q_p, str) and self._state.context & Context.PREPARED:
            self._state.context = self._state.context ^ Context.PREPARED
        if self._state.context & Context.PREPARED:
            self._state.values = retuple(q_p)
        else:
            self._state.query, self._state.values = q_p, retuple(par)
        client = self._setup.client
        oplist = copy.deepcopy(self._setup.op_list)
        oplist['execution'] += [client.fetchval()]
        return self._exec_ops_sync(oplist)
