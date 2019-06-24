# pgware

Advanced PostGreSQL Adapter Wrapper, beta (unsupported functions are ~~striked~~).

- Abstracts different PostgreSQL clients and offers a common asynchronous interface, by using native methods or by
emulating them;
- Offers broad error recovery: cursors can die, connections can get interrupted, it can recover until the
world burns, the parent process is interrupted or a provided timeout deadline is reached;
- Add Doodads to insert logging, tracing, parsing, converting or any other operation on each and/or every
step executed;
- ~~Responds to system signals (SIGUSR1, SIGUSR2) to for maintenance / recovery~~

## Documentation
(WORK IN PROGRESS)

## Usage
```python
pgw = pgware.build(client='asyncpg', type='single', output='record', **config)

# Simple execution
with pgw.get_connection() as conn:
    conn.execute('SELECT 1')
    conn.fetchval() # => 1
    conn.fetchval('SELECT 2') # => 2

# Prepared statement
with pgw.get_connection() as conn:
    conn.prepare('SELECT 2 * $1')
    conn.execute(2)
    conn.fetchval() # => 4
    conn.fetchval(5) # => 10

# Cursor
with pgw.get_connection().cursor() as cur:
    execute('SELECT 2 * %(number)i', number=3)
    cur.fetchval() # => 6

# Query parameters
with pgw.get_connection().cursor() as cur:
    execute('SELECT $1', ('value'))
    cur.fetchval() # => 'value'

with pgw.get_connection().cursor() as cur:
    execute('SELECT %s', ('value'))
    cur.fetchval() # => 'value'

with pgw.get_connection().cursor() as cur:
    execute('SELECT %(val)s', {'val': 'value'})
    cur.fetchval() # => 'value'

# Optionnal closing of connections
pgw.close_all()
    
```

## Parameters and extensions
The pgware.build() has the following parameters and defaults:
- client (str:`psycopg2`) : `asyncpg` or `psycopg2` are supported
- type (str:`single`) : `single` or ~~`pooled`~~
- output (str:`list`) : output rows as `list` or `dict`
- param_format (str:`postgresql`) : query parameter syntax, `postgresql` for asyncpg or `psycopg2`
- auto_json (bool:`True`) : auto-convert json data
- extensions (list:`[]`): extensions to be used

The `get_connection()` can be chained with the `cursor()` method to obtain a cursor.

Adapters who don't fully support pgware's interface will fail at the build stage.

### Available extensions:
- `dec2float`: convert decimal numbers to floats


## Query parameters:
Query parameters, be they a tuple, list or a dict, are an optional second argument
to the executing and fetching methods. Whether they use the PostgreSQL/Asyncpg
syntax or the psycopg2 one, both are supported and available for both clients.

## Helpers:
### QueryLoader
The QueryLoader acts as an import and cache interface for queries stored in python files:

```python
# Use it:
ql = QueryLoader("MY_PACKAGE", some_schema="MY_SCHEMA"})
query = ql.load("MY_QUERY", some_table="MY_TABLE")

# Debug a query:
ql.debug("MY_QUERY")
```


## API:

#### Prepared:
- prepare(query): Prepare a statement
- fetchall(values): Get all results from query
- fetchone(values): Get first result from query
- fetchval(values): Get first value from first result from query

#### Cursor:
- execute(query, [values]): Execute a statement
- fetchall(): Get all results from query
- fetchone(): Get first result from query
- fetchval(): Get first value from first result from query

#### Default:
- execute(query, [values]): Execute a statement
- executemany(query, [list of values]): Execute a statement
- fetchall(query, [values]): Get all results from query
- fetchone(query, [values]): Get first result from query
- fetchval(query, [values]): Get first value from first result from query

# Internals

## Terminology
<dl>
    <dt>Adapter</dt>
    <dd>A PostgreSQL python client, currently supported: psycopg2 and asyncpg</dd>
    <dt>Stage</dt>
    <dd>PGWare splits the operation to be executed in different stages</dd>
    <dt>Provider</dt>
    <dd>Each stage accepts interchangeable providers, who add the desired functionality
    from the selected adapter</dd>
    <dt>Doodad</dt>
    <dd>Small decorators that can be added to each stage</dd>
</dl>


## Stages and Providers:
- connection: 
    - pooled: use a pool of connections
    - single: use a single connection
- parsing:
    - prepared: use prepared queries
    - placeholder: use queries with placeholders (psycopg2 style)
    - raw\_text: just plain & simple SQL
- execution:
    - prepared
    - cursor
    - default
- result:
    - record: output results as rows of records (asyncpg style)
    - list: output a list of results
- errors:
    - handles adapter-specific exceptions and translates them to pgware ones
