"""Miscellaneous doodads for PGWare

This module is a generic place used to hold little doodad functions
which can help in a variety of cases>

To use them, import the desired functions from the pgware.doodads module.

    import pgware.doodads as doodads

    app.dbconn.add_doodad('parsing', doodads.remove_overflow_args)
    app.dbconn.add_doodad('parsing', doodads.dict_2_list)

"""
from .helpers import doodad
from .main import LOGGER


@doodad
def dict_2_list(state):
    """
    To be used at the **parsing** stage.

    When using asyncpg parameter syntax but the input values are stored in a dict,
    transform the dict into a list (insert this doodad at the _parsing_ stage).

    **Warning:**
    Attributes have to be in the same order as needed in the query
    """
    if isinstance(state.values, dict):
        LOGGER.debug("doodad:dict_2_list Values before conversion: %s", state.values)
        state.values = tuple(state.values.values())
        LOGGER.debug("doodad:dict_2_list Values after conversion: %s", state.values)
    yield state


@doodad
def remove_overflow_args(state):
    """
    To be used at the **parsing** stage.

    When using asyncpg parameter syntax but the amount of values too high, take the
    corresponding subset of those values to execute the query.
    """
    count = state.query.count('$')
    if count < len(state.values):
        state.values = state.values[0:count]
        LOGGER.debug("doodad:remove_overflow_args Values after reduction: %s", state.values)
    yield state
