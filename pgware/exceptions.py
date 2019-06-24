"""
PGWare Exceptions

Inheritance layout:

PgWareError
|__PrivateError
|__PublicError
   |__ ProgrammingError
   |__ RetriesExhausted
   |__ QueryError
   |__ UnrecoverableError

"""


class PgWareError(Exception):
    pass


class PrivateError(PgWareError):
    pass


class PublicError(PgWareError):
    pass


class ProgrammingError(PublicError):
    pass


class RetriesExhausted(PublicError):
    pass


class QueryError(PublicError):
    pass


class UnrecoverableError(PublicError):
    pass
