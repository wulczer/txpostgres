"""
Choose the implementation of psycopg2.
"""
import os


def _try_psycopg2_impl():
    # check for explicit request to use a specific implementation
    _library = os.getenv("TXPOSTGRES_PSYCOPG_IMPL")

    if _library == "psycopg2":
        import psycopg2
        return psycopg2

    if _library == "psycopg2cffi":
        import psycopg2cffi as psycopg2
        return psycopg2

    if _library == "psycopg2ct":
        import psycopg2ct as psycopg2
        return psycopg2

    # try various implementations until one works
    try:
        import psycopg2
        return psycopg2
    except ImportError:
        pass

    try:
        import psycopg2cffi as psycopg2
        return psycopg2
    except ImportError:
        pass

    try:
        import psycopg2ct as psycopg2
        return psycopg2
    except ImportError:
        pass

    raise ImportError('no module named psycopg2, psycopg2cffi or psycopg2ct')


psycopg2 = _try_psycopg2_impl()

del _try_psycopg2_impl
