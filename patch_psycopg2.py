# patch_psycopg2.py
import sys

class FakePsycopg2:
    # === REQUIRED BY SQLAlchemy ===
    paramstyle = 'pyformat'           # <-- THIS WAS MISSING
    threadsafety = 2
    apilevel = '2.0'

    # === CONNECTION ===
    @staticmethod
    def connect(*args, **kwargs):
        from psycopg import connect
        return connect(*args, **kwargs)

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
