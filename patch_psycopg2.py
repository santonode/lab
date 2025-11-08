# patch_psycopg2.py
import importlib
import sys

# === PATCH BEFORE ANYTHING ELSE ===
def patch_psycopg2():
    from psycopg import Connection

    # Fake psycopg2 module
    class FakePsycopg2:
        @staticmethod
        def connect(*args, **kwargs):
            from psycopg import connect
            return connect(*args, **kwargs)

        # Add any other needed attributes
        OperationalError = type('OperationalError', (Exception,), {})
        IntegrityError = type('IntegrityError', (Exception,), {})

    # Inject fake psycopg2 into sys.modules
    sys.modules['psycopg2'] = FakePsycopg2

# Run immediately
patch_psycopg2()
