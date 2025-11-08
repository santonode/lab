# sitecustomize.py
import sys
import os

# === PATCH BEFORE ANYTHING ELSE ===
class FakePsycopg2:
    @staticmethod
    def connect(*args, **kwargs):
        from psycopg import connect
        return connect(*args, **kwargs)

    # Fake exceptions
    class OperationalError(Exception): pass
    class IntegrityError(Exception): pass

# Inject fake psycopg2
sys.modules['psycopg2'] = FakePsycopg2
