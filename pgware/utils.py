from .exceptions import QueryError

# Utility functions


def ps2pg(q_in, v_in):
    """
    Convert psycopg2 query argument syntax to postgresql syntax
    - supports named arguments
    - keeps order of values
    - multiple reference will result in multiple values, cost of uniqueness check not worth it
    """
    if isinstance(v_in, dict):
        return ps2pg_dict(q_in, v_in)
    if not isinstance(v_in, tuple):
        v_in = [v_in]
    q_out = []
    v_out = []
    arg_count = 0
    skip = False
    for i, elm in enumerate(q_in):
        if skip and elm == 's':
            skip = not skip
        elif elm == '%' and q_in[i + 1] == 's':
            arg_count += 1
            skip = not skip
            q_out.append(f'${arg_count}')
            v_out.append(v_in[arg_count - 1])
        else:
            q_out.append(elm)
    if skip:
        raise QueryError('Query argument converter failed: check query')
    return ''.join(q_out), tuple(v_out)


def ps2pg_dict(q_in, v_in):
    q_out = []
    v_out = []
    skip = False
    buff = []
    for i, elm in enumerate(q_in):
        if skip and elm in ['(', ')']:
            pass
        elif skip and q_in[i - 1:i + 1] == ')s':
            v_out.append(v_in[''.join(buff)])
            q_out.append(str(len(v_out)))
            buff = []
            skip = not skip
        elif skip:
            buff.append(elm)
        elif elm == '%' and q_in[i + 1] == '(':
            q_out.append('$')
            skip = not skip
        else:
            q_out.append(elm)
    if skip:
        raise QueryError('Query argument converter failed: check query')
    return ''.join(q_out), tuple(v_out)


def pg2ps(q_in, v_in):
    """
    Convert postgresql query argument syntax to psycopg2 syntax
    - keeps order of values
    - fails if using double-dollar quotes with a number as first character (don't ;p)
    """
    q_out = []
    v_out = []
    skip = False
    buff = []
    if not isinstance(v_in, tuple):
        v_in = [v_in]
    for i, elm in enumerate(q_in):
        if skip and ord(elm) in range(48, 58):
            buff.append(elm)
        elif skip:
            skip = not skip
            v_out.append(v_in[int(''.join(buff)) - 1])
            q_out.append(elm)
            buff = []
        elif elm == '$' and ord(q_in[i + 1]) in range(48, 58):
            q_out.append('%s')
            skip = not skip
        else:
            q_out.append(elm)
    if skip:
        raise QueryError('Query argument converter failed: check query')
    return ''.join(q_out), tuple(v_out)


def config_map(configmap, config):
    """
    Map the config provided to PGWare to the client format

    Map format: {ADAPTER_KEY: [PGWARE_KEY, DEFAULT_VALUE] | ...}
    """
    out = dict(zip(configmap.keys(), [None] * len(configmap)))
    for key, definition in configmap.items():
        out[key] = config.get(*definition)
    return out


def retuple(inp):
    if inp is None:
        return None
    if isinstance(inp, tuple):
        return inp
    if isinstance(inp, dict):
        return inp
    return [inp]


def raise_(ex):
    """
    Default Doodad exception handler helper
    """
    raise ex


def supports(client, flag):
    return client.__supports__ & flag
