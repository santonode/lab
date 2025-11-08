# db.py
from psycopg import connect
import os

DATABASE_URL = os.getenv('DATABASE_URL')
if DATABASE_URL and DATABASE_URL.startswith("postgres://"):
    DATABASE_URL = DATABASE_URL.replace("postgres://", "postgresql://", 1)

def get_conn():
    return connect(DATABASE_URL, sslmode='require')

def init_db():
    with get_conn() as conn:
        with conn.cursor() as cur:
            # === ERATE TABLE: Already exists — do nothing ===
            # (Your real schema is perfect — no changes needed)

            # === MEMES TABLE: Already exists with meme_id PK — do nothing ===
            # (Your real schema is perfect — no changes needed)

            # === VOTES TABLE: Recreate to use meme_id (existing PK) ===
            cur.execute('DROP TABLE IF EXISTS votes')
            cur.execute('''
                CREATE TABLE votes (
                    id SERIAL PRIMARY KEY,
                    meme_id INTEGER NOT NULL,
                    voter_ip TEXT NOT NULL,
                    vote_type TEXT NOT NULL,
                    FOREIGN KEY (meme_id) REFERENCES memes(meme_id) ON DELETE CASCADE
                )
            ''')

            conn.commit()
