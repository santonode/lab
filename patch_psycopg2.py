# patch_psycopg2.py
import sys

# Fake psycopg2 that uses psycopg v3
class FakePsycopg2:
    @staticmethod
    def connect(*args, **kwargs):
        from psycopg import connect
        return connect(*args, **kwargs)

    # Fake exceptions
    class OperationalError(Exception): pass
    class IntegrityError(Exception): pass
    class InterfaceError(Exception): pass
    class DatabaseError(Exception): pass
    class ProgrammingError(Exception): pass

# INJECT IMMEDIATELY
sys.modules['psycopg2'] = FakePsycopg2
