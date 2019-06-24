def before_all(context):
    pass


def after_all(context):
    pass


def before_scenario(context, scenario):
    context.pgw = False
    context.cfg = False
    context.conn = False
    context.error = False
    context.result = False


def after_scenario(context, scenario):
    if context.conn is not False:
        context.conn.close_context_sync()
