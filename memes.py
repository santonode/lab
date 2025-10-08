from flask import Blueprint, render_template, request, jsonify, redirect, url_for
import os
import hashlib
import psycopg
import re

memes_bp = Blueprint('memes', __name__)

DATABASE_URL = os.environ.get('DATABASE_URL')
ADMIN_PASS = os.environ.get('ADMIN_PASS')
if not DATABASE_URL or not ADMIN_PASS:
    raise ValueError("DATABASE_URL and ADMIN_PASS environment variables must be set")

# Custom filter to transform Google Drive URL to download link
def get_download_url(url):
    if url and 'drive.google.com/file/d/' in url:
        match = re.search(r'https://drive.google.com/file/d/([^/]+)/view\?usp=drive_link', url)
        if match:
            file_id = match.group(1)
            return f"https://drive.google.com/uc?export=download&id={file_id}"
    return url

# Register the custom filter
memes_bp.jinja_env.filters['get_download_url'] = get_download_url

# Initialize Postgres database
def init_db():
    try:
        with psycopg.connect(DATABASE_URL) as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT EXISTS (
                        SELECT FROM information_schema.tables 
                        WHERE table_name = 'memes'
                    )
                """)
                table_exists = cur.fetchone()[0]
                
                if table_exists:
                    cur.execute("SELECT COUNT(*) FROM memes")
                    meme_count = cur.fetchone()[0]
                    if meme_count > 0:
                        print(f"Memes table already contains {meme_count} records, skipping full reinitialization.")
                    else:
                        print("Memes table exists but is empty, initializing with default data.")
                        cur.execute('DROP TABLE IF EXISTS memes')
                        cur.execute('''
                            CREATE TABLE IF NOT EXISTS memes (
                                meme_id INTEGER PRIMARY KEY,
                                meme_url TEXT NOT NULL,
                                meme_description TEXT NOT NULL,
                                meme_download_counts INTEGER DEFAULT 0,
                                type TEXT DEFAULT 'Other' CHECK (type IN ('Other', 'GM', 'GN', 'Crypto', 'Grawk')),
                                owner INTEGER DEFAULT 3
                            )
                        ''')
                        cur.execute('''
                            INSERT INTO memes (meme_id, meme_url, meme_description, meme_download_counts, type, owner)
                            VALUES (%s, %s, %s, %s, %s, %s)
                            ON CONFLICT (meme_id) DO NOTHING
                        ''', (1, 'https://drive.google.com/file/d/1rKLbOKw88TKBLKhxnrAVEqxy4ZTB0gLv/view?usp=drive_link', 'Good Morning Good Morning 3', 0, 'GM', 3))
                        conn.commit()
                else:
                    print("Memes table does not exist, creating and initializing.")
                    cur.execute('''
                        CREATE TABLE IF NOT EXISTS memes (
                            meme_id INTEGER PRIMARY KEY,
                            meme_url TEXT NOT NULL,
                            meme_description TEXT NOT NULL,
                            meme_download_counts INTEGER DEFAULT 0,
                            type TEXT DEFAULT 'Other' CHECK (type IN ('Other', 'GM', 'GN', 'Crypto', 'Grawk')),
                            owner INTEGER DEFAULT 3
                        )
                    ''')
                    cur.execute('''
                        INSERT INTO memes (meme_id, meme_url, meme_description, meme_download_counts, type, owner)
                        VALUES (%s, %s, %s, %s, %s, %s)
                        ON CONFLICT (meme_id) DO NOTHING
                    ''', (1, 'https://drive.google.com/file/d/1rKLbOKw88TKBLKhxnrAVEqxy4ZTB0gLv/view?usp=drive_link', 'Good Morning Good Morning 3', 0, 'GM', 3))
                    conn.commit()
        print(f"Database initialized successfully with URL: {DATABASE_URL}")
    except psycopg.Error as e:
        print(f"Database initialization error: {str(e)}")
        raise
    except Exception as e:
        print(f"Unexpected error during initialization: {str(e)}")
        raise

init_db()

# Get next available ID for a table
def get_next_id(table_name):
    try:
        with psycopg.connect(DATABASE_URL) as conn:
            with conn.cursor() as cur:
                column_name = {'memes': 'meme_id', 'users':
