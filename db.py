# db.py
from flask import g
import psycopg
from psycopg_pool import ConnectionPool
import os
import logging

# === LOGGING ===
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# === DATABASE URL ===
DATABASE_URL = os.getenv('DATABASE_URL')
if DATABASE_URL and DATABASE_URL.startswith("postgres://"):
    DATABASE_URL = DATABASE_URL.replace("postgres://", "postgresql://", 1)

# === GLOBAL CONNECTION POOL ===
pool = None

# === INITIALIZE CONNECTION POOL ===
def init_db_pool(app):
    global pool
    if pool is None:
        logger.info("Initializing database connection pool...")
        pool = ConnectionPool(
            conninfo=app.config.get('DATABASE_URL', DATABASE_URL),
            min_size=1,
            max_size=10,
            timeout=30.0,
            open=True
        )
        logger.info("Connection pool initialized (max_size=10)")

# === GET CONNECTION FROM POOL ===
def get_conn():
    """Get a connection from the pool (thread-safe)"""
    if 'db' not in g:
        if pool is None:
            raise RuntimeError("Connection pool not initialized. Call init_db_pool(app) first.")
        try:
            g.db = pool.get_conn(timeout=10.0)
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
            pool.put_conn(db)
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
        if conn:
            conn.rollback()
        raise

# === INIT FLASK APP (CALL FROM app.py) ===
def init_app(app):
    """Register teardown and pool init"""
    app.teardown_appcontext(close_conn)
    with app.app_context():
        init_db_pool(app)
        init_db()
