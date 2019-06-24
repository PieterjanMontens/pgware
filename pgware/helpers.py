import inspect
import string
from contextlib import asynccontextmanager, contextmanager
from importlib import import_module
from typing import Generator

from .main import DD


"""
Library of internal and external helper functions.
"""


def provider(reuse=False):
    """
    A provider is much like the doodad helper, and is used internally to ease the implementation of
    the PostgreSQL clients. It's a decorater who adds technical information for the PGWare execution queue to use,
    and is parametrizable.

    A provider must output two functions, respecting this order:

    1. The job which will accept the state and output it after modification (ie: like a doodad)
    2. The error handler, which can be specific to each provider, who is expected to handle errors generated
    by the job


    **Decorator input & output:**

    Input:
    - reuse: State output of function can be stored for further use (example: connections, prepared statements, ...)

    Output:
    - jobname: the function's name (for logging & internal purposes)
    - the (async) context manager wrapping the final job code to be executed
    - the error/exception manager
    - "reuse" flag
    - "coroutine" flag (whether the job is a coroutine or not)

    See client implementations for examples
    """
    def decorator(fun):
        jobname = fun.__name__

        def wrap(*args, **kwargs):
            job, ex_manager = fun(*args, **kwargs)
            if inspect.isasyncgenfunction(job):
                if DD.HELPERS ^ DD.DEEP:
                    print(f'## {jobname} is coroutine')
                return jobname, asynccontextmanager(job), ex_manager, reuse, True
            else:
                if DD.HELPERS ^ DD.DEEP:
                    print(f'## {jobname} is not coroutine')
                return jobname, contextmanager(job), ex_manager, reuse, False
        return wrap
    return decorator


def doodad(fun: Generator):
    """
    A doodad is a function that can be inserted in PGWare's asynchronous execution pipeline.
    It receives the pipeline's state as parameter, and **must** `yield` it at some point.

    The available fields of the State object that can be accessed are documented in the State's definition.

    **Usage:**

    A doodad can be defined by employing the `doodad` decorator. Example:

        import pgware as pgware

        @pgware.doodad
        def log_query(state):
            print(f"raw query: {state.query}")
            print(f"raw values: {state.values}")
            yield state

    Doodads need to be assigned to the PGWare object or connection context, by specifying at which stage you wish
    to insert the doodad:

        import pgware as pgware

        pgw = pgware.build(**config)
        # For all PGWare queries:
        pgw.add_doodad('execution', log_query)

        # Just for a single context:
        with pgw.get_connection() as conn:
            conn.add_doodad('execution', log_query)

    Available execution stages are (see README for more details):

    - connection
    - parsing
    - execution
    - result
    - errors

    **Definition:**

    **parameters**, **types**, **return** and **return types**::

    - :param arg1: the function which will become the doodad
    - :type arg1: a generator function, optionnaly async
    - :return: the function which can be used as dooded
    - :rtype: a context manager, optionnaly async

    """
    if inspect.isasyncgenfunction(fun):
        return asynccontextmanager(fun)
    else:
        return contextmanager(fun)


class QueryLoader():
    """
    Query loader class: tell where it can find the files, and it will load, format and cache them.
    The python file containing the query just has to export a "QUERY" variable containing the query,
    with optionnal fields to be formatted (**Python formatted**, like for table or schema names)'.

    Usage:

        ql = QueryLoader("MY_PACKAGE", some_schema="MY_SCHEMA"})
        query = ql.load("MY_QUERY", some_table="MY_TABLE")

        ql.debug("MY_QUERY")

    """

    def __init__(self, package, directory, **kwargs):
        self._cache = {}
        self._data = {
            'package': package,
            'dir': directory,
            'formattings': kwargs
        }

    def load(self, query_name, **kwargs):
        """
        Load query, formatted with stored and/or provided format definition(s)
        """
        query = self._get_query(query_name)
        kwargs = {**self._data['formattings'], **kwargs}
        return query.format(**kwargs)

    def debug(self, query_name, **kwargs):
        """
        Display how query loading would happen, and what formattings will be applied
        """
        in_cache = 'Found in cache' if query_name in self._cache else 'Not found in cache'
        q_1 = self._get_query(query_name)
        values_provided = []

        for key, value in kwargs.items():
            values_provided.append(f'\t{key}: "{value}"')
        values_stored = []
        for key, value in self._data['formattings'].items():
            values_stored.append(f'\t{key}: "{value}"')

        fmt = string.Formatter()
        values_expected = []
        key_list = []
        for _str, key, _spec, _conv in fmt.parse(q_1):
            if key and key not in key_list:
                values_expected.append(f'\t{key}')
                key_list.append(key)

        kwargs = {**self._data['formattings'], **kwargs}

        try:
            q_2 = q_1.format(**kwargs)
        except KeyError:
            missing = ', '.join([f'{x}' if x not in kwargs else '' for x in values_expected])
            q_2 = f"\tError in creating string, missing key(s): {missing}"

        print(f"""########### QUERYLOADER DEBUG - {query_name} - {in_cache}
SQL:
{q_1}
Expected values:
{', '.join(values_expected)}

Stored Values:
{', '.join(values_stored)}

Provided Values:
{', '.join(values_provided)}

Resulting SQL:
{q_2}
########### END OF QUERY DEBUG
""")

    def _get_query(self, query_name):
        if query_name in self._cache:
            return self._cache[query_name]
        try:
            self._cache[query_name] = import_module(f".{self._data['dir']}.{query_name}", package=self._data['package']).QUERY
        except ImportError:
            raise RuntimeError(f"QueryLoader: query {query_name} not found in package {self._data['package']}")
        return self._get_query(query_name)
