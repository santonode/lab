from flask import Blueprint, render_template, request, session, redirect, url_for, current_app, jsonify
import psycopg
import os
import re
from datetime import datetime

# Define the Blueprint
memes_bp = Blueprint('memes', __name__)

# Database URL from environment variable
DATABASE_URL = os.environ.get('DATABASE_URL')
if not DATABASE_URL:
    raise ValueError("DATABASE_URL environment variable must be set")

# Admin password (should be moved to environment variable in production)
ADMIN_PASS = "admin123"  # Replace with os.environ.get('ADMIN_PASS', 'default') for security

# Function to get the next ID for a table
def get_next_id(table_name):
    try:
        with psycopg.connect(DATABASE_URL) as conn:
            with conn.cursor() as cur:
                cur.execute(f"SELECT COALESCE(MAX({table_name}_id), 0) + 1 FROM {table_name}")
                return cur.fetchone()[0]
    except psycopg.Error as e:
        current_app.logger.error(f"Database error in get_next_id: {str(e)}")
        return 1

# Memes route
@memes_bp.route('/memes')
def memes():
    try:
        with psycopg.connect(DATABASE_URL) as conn:
            with conn.cursor() as cur:
                cur.execute('SELECT meme_id, meme_url, meme_description, meme_download_counts, type, owner FROM memes ORDER BY meme_id')
                rows = cur.fetchall()
                current_app.logger.debug(f"Raw query results: {rows}")
                memes = []
                for row in rows:
                    if not isinstance(row, tuple) or len(row) != 6:
                        current_app.logger.error(f"Invalid row format: {row}, expected 6 columns")
                        continue
                    memes.append({
                        'meme_id': row[0],
                        'meme_url': row[1],
                        'meme_description': row[2],
                        'meme_download_counts': row[3],
                        'type': row[4],
                        'owner': row[5]
                    })
                cur.execute('SELECT id, username FROM users')
                users = [{'id': row[0], 'username': row[1]} for row in cur.fetchall()]
                cur.execute('SELECT COUNT(*) FROM memes')
                meme_count = cur.fetchone()[0]
                verified_count = len(memes)
                if meme_count != verified_count:
                    current_app.logger.warning(f"Warning: Meme count mismatch - SQL COUNT: {meme_count}, Fetched rows: {verified_count}")
                    meme_count = verified_count
                cur.execute('SELECT SUM(meme_download_counts) FROM memes')
                total_downloads = cur.fetchone()[0] or 0
        
        username = session.get('username')
        user_type = session.get('user_type', 'Guest')
        points = 0
        if username:
            try:
                with psycopg.connect(DATABASE_URL) as conn:
                    with conn.cursor() as cur:
                        cur.execute('SELECT user_type, points FROM users WHERE username = %s', (username,))
                        result = cur.fetchone()
                        if result:
                            user_type, points = result
                            session['user_type'] = user_type
            except psycopg.Error as e:
                current_app.logger.error(f"Database error fetching user_type for memes: {str(e)}")

        current_app.logger.debug(f"Memes fetched: {memes}, users: {users}, meme_count: {meme_count}, total_downloads: {total_downloads}")
        return render_template('memes.html', memes=memes, users=users, meme_count=meme_count, total_downloads=total_downloads, username=username, user_type=user_type, points=points if user_type != 'Guest' else None, message=None)
    except psycopg.Error as e:
        current_app.logger.error(f"Database error in memes: {str(e)}")
        return render_template('memes.html', memes=[], users=[], message="Error fetching meme data.", meme_count=0, total_downloads=0, username=None, user_type='Guest', points=0)
    except Exception as e:
        current_app.logger.error(f"Unexpected error in memes: {str(e)}", exc_info=True)
        return render_template('memes.html', memes=[], users=[], message="Error fetching meme data.", meme_count=0, total_downloads=0, username=None, user_type='Guest', points=0)

# Admin route
@memes_bp.route('/admin', methods=['GET', 'POST'])
def admin():
    message = None
    authenticated = session.get('admin_authenticated', False)

    next_meme_id = get_next_id('memes')

    if request.method == 'POST':
        if 'admin_pass' in request.form:
            admin_pass = request.form.get('admin_pass', '')
            if admin_pass == ADMIN_PASS:
                session['admin_authenticated'] = True
                authenticated = True
                message = "Admin authentication successful!"
            else:
                message = "Incorrect admin password."
        elif authenticated and 'add_meme' in request.form:
            meme_url = request.form.get('meme_url', '')
            meme_description = request.form.get('meme_description', '')
            meme_type = request.form.get('meme_type', 'image')
            owner = request.form.get('owner', 'admin')
            if meme_url and meme_description:
                try:
                    with psycopg.connect(DATABASE_URL) as conn:
                        with conn.cursor() as cur:
                            cur.execute('INSERT INTO memes (meme_id, meme_url, meme_description, meme_download_counts, type, owner) VALUES (%s, %s, %s, %s, %s, %s)',
                                        (next_meme_id, meme_url, meme_description, 0, meme_type, owner))
                            conn.commit()
                            message = f"Meme {next_meme_id} added successfully!"
                            next_meme_id = get_next_id('memes')  # Update for next insertion
                except psycopg.Error as e:
                    message = f"Database error adding meme: {str(e)}"
        elif authenticated and 'delete_meme' in request.form:
            meme_id = request.form.get('meme_id', '')
            if meme_id.isdigit():
                try:
                    with psycopg.connect(DATABASE_URL) as conn:
                        with conn.cursor() as cur:
                            cur.execute('DELETE FROM memes WHERE meme_id = %s', (int(meme_id),))
                            conn.commit()
                            message = f"Meme {meme_id} deleted successfully!"
                except psycopg.Error as e:
                    message = f"Database error deleting meme: {str(e)}"

    if not authenticated:
        return render_template('admin.html', message=message, authenticated=authenticated, next_meme_id=next_meme_id)

    try:
        with psycopg.connect(DATABASE_URL) as conn:
            with conn.cursor() as cur:
                cur.execute('SELECT meme_id, meme_url, meme_description, meme_download_counts, type, owner FROM memes ORDER BY meme_id')
                rows = cur.fetchall()
                current_app.logger.debug(f"Raw query results for admin: {rows}")
                memes = []
                for row in rows:
                    if not isinstance(row, tuple) or len(row) != 6:
                        current_app.logger.error(f"Invalid row format in admin: {row}, expected 6 columns")
                        continue
                    memes.append({
                        'meme_id': row[0],
                        'meme_url': row[1],
                        'meme_description': row[2],
                        'meme_download_counts': row[3],
                        'type': row[4],
                        'owner': row[5]
                    })
                cur.execute('SELECT id, username FROM users')
                users = [{'id': row[0], 'username': row[1]} for row in cur.fetchall()]
                cur.execute('SELECT COUNT(*) FROM memes')
                meme_count = cur.fetchone()[0]
        return render_template('admin.html', memes=memes, users=users, meme_count=meme_count, message=message, authenticated=authenticated, next_meme_id=next_meme_id)
    except psycopg.Error as e:
        current_app.logger.error(f"Database error in admin: {str(e)}")
        return render_template('admin.html', memes=[], users=[], meme_count=0, message="Error fetching meme data.", authenticated=authenticated, next_meme_id=next_meme_id)
    except Exception as e:
        current_app.logger.error(f"Unexpected error in admin: {str(e)}", exc_info=True)
        return render_template('admin.html', memes=[], users=[], meme_count=0, message="Error fetching meme data.", authenticated=authenticated, next_meme_id=next_meme_id)

# Register route for guest users
@memes_bp.route('/register', methods=['POST'])
def register():
    username = request.form.get('register_username', '').strip()
    password = request.form.get('register_password', '')
    ip_address = request.remote_addr

    if username and password and all(c.isalnum() for c in username) and 1 <= len(username) <= 12:
        try:
            with psycopg.connect(DATABASE_URL) as conn:
                with conn.cursor() as cur:
                    cur.execute('SELECT 1 FROM users WHERE username = %s', (username,))
                    if cur.fetchone():
                        return jsonify({'message': 'Username already taken.', 'success': False})
                    cur.execute('INSERT INTO users (ip_address, username, password, user_type, points, word_list) VALUES (%s, %s, %s, %s, %s, %s)',
                              (ip_address, username, hash_password(password), 'Member', 0, 'words.txt'))
                    cur.execute('INSERT INTO user_stats (user_id) VALUES (currval(\'users_id_seq\'))')
                    conn.commit()
                    session.clear()
                    session['username'] = username
                    session['user_type'] = 'Member'
                    session['word_list'] = 'words.txt'
                    return jsonify({'message': 'Registration successful! You are now a Member.', 'success': True})
        except psycopg.Error as e:
            current_app.logger.error(f"Database error during registration: {str(e)}")
            return jsonify({'message': 'Error during registration.', 'success': False})
    else:
        return jsonify({'message': 'Invalid username or password (1-12 alphanumeric characters required).', 'success': False})

# Helper function to hash password
def hash_password(password):
    import hashlib
    return hashlib.sha256(password.encode()).hexdigest()

# Increment download count route
@memes_bp.route('/increment_download/<int:meme_id>', methods=['POST'])
def increment_download(meme_id):
    try:
        with psycopg.connect(DATABASE_URL) as conn:
            with conn.cursor() as cur:
                cur.execute('UPDATE memes SET meme_download_counts = meme_download_counts + 1 WHERE meme_id = %s', (meme_id,))
                if cur.rowcount == 0:
                    return jsonify({'success': False, 'error': 'Meme not found.'})
                conn.commit()
                return jsonify({'success': True})
    except psycopg.Error as e:
        current_app.logger.error(f"Database error incrementing download count: {str(e)}")
        return jsonify({'success': False, 'error': 'Database error.'})
    except Exception as e:
        current_app.logger.error(f"Unexpected error incrementing download count: {str(e)}")
        return jsonify({'success': False, 'error': 'Unexpected error.'})

# Custom filter for download URL
def get_download_url(meme):
    # Extract Google Drive asset ID from meme_url
    meme_url = meme.get('meme_url', '')
    if 'drive.google.com' in meme_url:
        # Match common Google Drive URL formats
        match = re.search(r'/d/([a-zA-Z0-9-_]+)', meme_url) or re.search(r'id=([a-zA-Z0-9-_]+)', meme_url)
        if match:
            asset_id = match.group(1)
            return f"https://drive.google.com/uc?export=download&id={asset_id}"
    return meme_url  # Fallback to original URL if not a Google Drive link

# Database initialization function (to be called in app context)
def init_db():
    try:
        with psycopg.connect(DATABASE_URL) as conn:
            with conn.cursor() as cur:
                cur.execute('''
                    CREATE TABLE IF NOT EXISTS memes (
                        meme_id SERIAL PRIMARY KEY,
                        meme_url TEXT NOT NULL,
                        meme_description TEXT NOT NULL,
                        meme_download_counts INTEGER DEFAULT 0,
                        type TEXT DEFAULT 'image',
                        owner TEXT DEFAULT 'admin',
                        UNIQUE (meme_url)
                    )
                ''')
                cur.execute('SELECT COUNT(*) FROM memes')
                count = cur.fetchone()[0]
                if count == 0:
                    cur.execute('INSERT INTO memes (meme_url, meme_description, type, owner) VALUES (%s, %s, %s, %s)',
                                ('https://drive.google.com/file/d/1abc123XYZ/view', 'Funny Cat', 'image', 'admin'))
                    conn.commit()
                    current_app.logger.info(f"Initialized memes table with {count + 1} records")
                else:
                    current_app.logger.info(f"Memes table already contains {count} records, skipping full reinitialization.")
                conn.commit()
    except psycopg.Error as e:
        current_app.logger.error(f"Database initialization error: {str(e)}")
        raise
