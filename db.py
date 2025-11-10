# db.py
from flask import g
import psycopg
from psycopg_pool import ConnectionPool
import os
import logging
import ssl

# === LOGGING ===
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# === DATABASE URL ===
DATABASE_URL = os.getenv('DATABASE_URL')
if DATABASE_URL and DATABASE_URL.startswith("postgres://"):
    DATABASE_URL = DATABASE_URL.replace("postgres://", "postgresql://", 1)

# === GLOBAL CONNECTION POOL ===
pool = None

# === INITIALIZE CONNECTION POOL WITH TLS 1.2 ===
def init_db_pool(app):
    global pool
    if pool is None:
        logger.info("Initializing database connection pool...")
        
        # Get connection string
        conninfo = app.config.get('DATABASE_URL', DATABASE_URL)
        
        # Ensure SSL is enforced
        if 'sslmode' not in conninfo.lower():
            conninfo += ' sslmode=require'
        if 'sslrootcert' not in conninfo.lower():
            conninfo += ' sslrootcert=auto'

        # Create TLS 1.2 context
        ssl_context = ssl.create_default_context()
        ssl_context.minimum_version = ssl.TLSVersion.TLSv1_2
        ssl_context.maximum_version = ssl.TLSVersion.TLSv1_2

        pool = ConnectionPool(
            conninfo=conninfo,
            min_size=1,
            max_size=10,
            timeout=30.0,
            open=True,
            context=ssl_context  # Force TLS 1.2
        )
        logger.info("Connection pool initialized with TLS 1.2 (max_size=10)")

# === GET CONNECTION FROM POOL ===
def get_conn():
    """Get a connection from the pool (thread-safe)"""
    if 'db' not in g:
        if pool is None:
            raise RuntimeError("Connection pool not initialized. Call init_db_pool(app) first.")
        try:
            g.db = pool.getconn(timeout=10.0)
            # Re-apply SSL context per connection
            g.db.sslcontext = ssl.create_default_context()
            g.db.sslcontext.minimum_version = ssl.TLSVersion.TLSv1_2
        except Exception as e:
            logger.error(f"Failed to get connection from pool: {e}")
            raise
    return g.db

# === RETURN CONNECTION TO POOL ===
def close_conn(e=None):
    """Return connection to pool on Flask request teardown"""
    db = g.pop('db', None)
    if db is not None:
        try:
            pool.putconn(db)
        except Exception as e:
            logger.warning(f"Error returning connection to pool: {e}")

# === INIT DB SCHEMA (IDEMPOTENT) ===
def init_db():
    """Initialize database tables — safe, idempotent"""
    try:
        conn = get_conn()
        with conn.cursor() as cur:
            logger.info("Initializing database schema...")

            # === VOTES TABLE: Recreate safely with FK ===
            cur.execute('DROP TABLE IF EXISTS votes')
            logger.info("Dropped existing votes table")
            cur.execute('''
                CREATE TABLE votes (
                    id SERIAL PRIMARY KEY,
                    meme_id INTEGER NOT NULL,
                    voter_ip TEXT NOT NULL,
                    vote_type TEXT CHECK (vote_type IN ('up', 'down')) NOT NULL,
                    created_at TIMESTAMP DEFAULT NOW(),
                    UNIQUE(meme_id, voter_ip),
                    FOREIGN KEY (meme_id) REFERENCES memes(meme_id) ON DELETE CASCADE
                )
            ''')
            logger.info("Created votes table with constraints")

            conn.commit()
            logger.info("Database initialization complete")
    except Exception as e:
        logger.error(f"Database initialization failed: {e}")
        if 'conn' in locals():
            conn.rollback()
        raise

# === INIT FLASK APP (CALL FROM app.py) ===
def init_app(app):
    """Register teardown and initialize pool + schema"""
    app.teardown_appcontext(close_conn)
    with app.app_context():
        init_db_pool(app)
        init_db()
