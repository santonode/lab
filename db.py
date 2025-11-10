# db.py
from flask import g
import psycopg
import os
import logging

# === LOGGING ===
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# === DATABASE URL ===
DATABASE_URL = os.getenv('DATABASE_URL')
if DATABASE_URL and DATABASE_URL.startswith("postgres://"):
    DATABASE_URL = DATABASE_URL.replace("postgres://", "postgresql://", 1)

# === GET FRESH CONNECTION (NO POOL) ===
def get_conn():
    """Get a fresh connection every time — avoids SSL reuse & corruption"""
    if 'db' not in g:
        try:
            # Build connection string with sslmode=require as query param
            conninfo = DATABASE_URL
            if 'sslmode' not in conninfo.lower():
                if '/' in conninfo:
                    base, dbname = conninfo.rsplit('/', 1)
                    conninfo = f"{base}/{dbname}?sslmode=require"
                else:
                    conninfo += '?sslmode=require'

            g.db = psycopg.connect(conninfo)
            g.db.autocommit = False
            logger.debug("New DB connection established")
        except Exception as e:
            logger.error(f"Failed to connect to database: {e}")
            raise
    return g.db

# === CLOSE CONNECTION ===
def close_conn(e=None):
    """Close and discard connection on request teardown"""
    db = g.pop('db', None)
    if db is not None:
        try:
            db.close()
            logger.debug("DB connection closed")
        except Exception as e:
            logger.warning(f"Error closing connection: {e}")

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
            try:
                conn.rollback()
            except:
                pass
        raise

# === INIT FLASK APP ===
def init_app(app):
    """Register teardown and initialize schema"""
    app.teardown_appcontext(close_conn)
    with app.app_context():
        init_db()
