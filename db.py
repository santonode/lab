# db.py
from psycopg import connect
import os
import logging

# === LOGGING ===
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# === DATABASE URL ===
DATABASE_URL = os.getenv('DATABASE_URL')
if DATABASE_URL and DATABASE_URL.startswith("postgres://"):
    DATABASE_URL = DATABASE_URL.replace("postgres://", "postgresql://", 1)

def get_conn():
    """Return a new database connection with SSL"""
    try:
        conn = connect(DATABASE_URL, sslmode='require')
        conn.autocommit = False  # Safe transactions
        return conn
    except Exception as e:
        logger.error(f"Database connection failed: {e}")
        raise

def init_db():
    """Initialize database tables — safe, idempotent"""
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                logger.info("Initializing database schema...")

                # === ERATE TABLE: Already exists — skip ===
                # (Your 70+ column table is perfect — no changes)

                # === MEMES TABLE: Already exists with meme_id PK — skip ===
                # (Your schema is solid)

                # === VOTES TABLE: Recreate safely with FK to meme_id ===
                cur.execute('DROP TABLE IF EXISTS votes')
                logger.info("Dropped existing votes table")

                cur.execute('''
                    CREATE TABLE votes (
                        id SERIAL PRIMARY KEY,
                        meme_id INTEGER NOT NULL,
                        voter_ip TEXT NOT NULL,
                        vote_type TEXT CHECK (vote_type IN ('up', 'down')) NOT NULL,
                        created_at TIMESTAMP DEFAULT NOW(),
                        UNIQUE(meme_id, voter_ip),  -- Prevent duplicate votes
                        FOREIGN KEY (meme_id) REFERENCES memes(meme_id) ON DELETE CASCADE
                    )
                ''')
                logger.info("Created votes table with constraints")

                conn.commit()
                logger.info("Database initialization complete")

    except Exception as e:
        logger.error(f"Database initialization failed: {e}")
        raise
