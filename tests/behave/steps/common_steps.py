# pylint: skip-file
from behave import given, when, then
import pgware as pgware


@given('a {client} db config')
def given_pspg2(context, client):
    context.cfg = {
        'client': client,
        'database': '[DB]',
        'user': '[USER]',
        'password': None,
        'host': '[HOST]',
        'port': None,
        'connection_type': 'single',
    }


@given('a pgware instance')
def given_pgw(context):
    context.pgw = pgware.build(**context.cfg)


@when('we open a connection')
def when_open(context):
    context.conn = context.pgw.raw_connection()


@when('we {operation} query "{query}"')
def when_op_query(context, operation, query):
    try:
        context.result = getattr(context.conn, operation)(query)
    except RuntimeError:
        raise
    except Exception as exc:
        context.error = exc


@when('we sabotage the psycopg2 connection')
def when_sabotage(context):
    context.conn._state.cursor.close()


@then('no error is raised')
def then_noerr(context):
    assert context.error is False


@then('we obtain "{result}"')
def then_result(context, result):
    # print(f'Expected: {result}, Returned: {context.result}')
    assert str(context.result) == str(result)
