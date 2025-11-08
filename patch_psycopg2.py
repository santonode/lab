# patch_psycopg2.py
import sys
from psycopg import extras  # ← Import real extras from psycopg v3

class FakePsycopg2:
    # === REQUIRED BY SQLAlchemy ===
    paramstyle = 'pyformat'
    threadsafety = 2
    apilevel = '2.0'

    # === CONNECTION ===
    @staticmethod
    def connect(*args, **kwargs):
        from psycopg import connect
        return connect(*args, **kwargs)

    # === EXTRAS (from psycopg v3) ===
    extras = extras

    # === EXCEPTIONS ===
    class OperationalError(Exception): pass
    class IntegrityError(Exception): pass
    class InterfaceError(Exception): pass
    class DatabaseError(Exception): pass
    class ProgrammingError(Exception): pass
    class DataError(Exception): pass
    class NotSupportedError(Exception): pass

# INJECT IMMEDIATELY
sys.modules['psycopg2'] = FakePsycopg2
