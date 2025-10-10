from flask import Blueprint, render_template, request, session, redirect, url_for, current_app, jsonify
import psycopg
import os
import re
from datetime import datetime
import hashlib

# Define the Blueprint
memes_bp = Blueprint('memes', __name__)

# Database URL from environment variable
DATABASE_URL = os.environ.get('DATABASE_URL')
if not DATABASE_URL:
    raise ValueError("DATABASE_URL environment variable must be set")

# Admin password from environment variable, with fallback for local testing
ADMIN_PASS = os.environ.get('ADMIN_PASS', 'admin123')

# Function to get the next ID for a table
def get_next_id(table_name):
    try:
        with psycopg.connect(DATABASE_URL) as conn:
            with conn.cursor() as cur:
                cur.execute(f"SELECT COALESCE(MAX(meme_id), 0) + 1 FROM {table_name}")
                return cur.fetchone()[0]
    except psycopg.Error as e:
        current_app.logger.error(f"Database error in get_next_id: {str(e)}")
        return 1

# Custom Jinja2 filter to check if a file exists in the static folder
def file_exists_filter(filename):
    file_path = os.path.join(current_app.static_folder, filename)
    return os.path.isfile(file_path)

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
                    if not isinstance(row, tuple) or len(row) != 6:  # Adjusted to 6 columns (removed thumbnail_url)
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
            current_app.logger.debug(f"Attempted admin password: '{admin_pass}', Expected from env: '{ADMIN_PASS}'")
            if admin_pass == ADMIN_PASS:
                session['admin_authenticated'] = True
                authenticated = True
                message = "Admin authentication successful!"
            else:
                message = "Incorrect admin password."
        elif authenticated:
            if 'delete_username' in request.form:
                username = request.form.get('delete_username')
                try:
                    with psycopg.connect(DATABASE_URL) as conn:
                        with conn.cursor() as cur:
                            cur.execute('DELETE FROM users WHERE username = %s', (username,))
                            conn.commit()
                            message = f"User {username} deleted successfully!"
                except psycopg.Error as e:
                    message = f"Database error deleting user: {str(e)}"
            elif 'edit_username' in request.form:
                username = request.form.get('edit_username')
                new_username = request.form.get('new_username')
                new_password = request.form.get('new_password')
                new_points = request.form.get('new_points')
                try:
                    with psycopg.connect(DATABASE_URL) as conn:
                        with conn.cursor() as cur:
                            cur.execute('UPDATE users SET username = %s, password = %s, points = %s WHERE username = %s',
                                        (new_username, hash_password(new_password), new_points, username))
                            conn.commit()
                            message = f"User {username} updated successfully!"
                except psycopg.Error as e:
                    message = f"Database error updating user: {str(e)}"
            elif 'add_user' in request.form:
                new_username = request.form.get('new_username')
                new_password = request.form.get('new_password')
                new_points = request.form.get('new_points', 0)
                if new_username and new_password and all(c.isalnum() for c in new_username) and 1 <= len(new_username) <= 12:
                    try:
                        with psycopg.connect(DATABASE_URL) as conn:
                            with conn.cursor() as cur:
                                cur.execute('INSERT INTO users (ip_address, username, password, user_type, points, word_list) VALUES (%s, %s, %s, %s, %s, %s)',
                                          ('0.0.0.0', new_username, hash_password(new_password), 'Member', new_points, 'words.txt'))
                                cur.execute('INSERT INTO user_stats (user_id) VALUES (currval(\'users_id_seq\'))')
                                conn.commit()
                                message = f"User {new_username} added successfully!"
                    except psycopg.Error as e:
                        message = f"Database error adding user: {str(e)}"
            elif 'delete_meme_id' in request.form:
                meme_id = request.form.get('delete_meme_id')
                if meme_id.isdigit():
                    try:
                        with psycopg.connect(DATABASE_URL) as conn:
                            with conn.cursor() as cur:
                                cur.execute('DELETE FROM memes WHERE meme_id = %s', (int(meme_id),))
                                conn.commit()
                                message = f"Meme {meme_id} deleted successfully!"
                    except psycopg.Error as e:
                        message = f"Database error deleting meme: {str(e)}"
            elif 'edit_meme_id' in request.form:
                meme_id = request.form.get('edit_meme_id')
                new_type = request.form.get('new_type')
                new_description = request.form.get('new_description')
                new_meme_url = request.form.get('new_meme_url')
                new_owner = request.form.get('new_owner')
                new_download_counts = request.form.get('new_download_counts')
                if meme_id.isdigit() and new_owner.isdigit():
                    try:
                        with psycopg.connect(DATABASE_URL) as conn:
                            with conn.cursor() as cur:
                                cur.execute('UPDATE memes SET type = %s, meme_description = %s, meme_url = %s, owner = %s, meme_download_counts = %s WHERE meme_id = %s',
                                          (new_type, new_description, new_meme_url, int(new_owner), int(new_download_counts), int(meme_id)))
                                conn.commit()
                                message = f"Meme {meme_id} updated successfully!"
                    except psycopg.Error as e:
                        message = f"Database error updating meme: {str(e)}"
            elif 'add_meme' in request.form:
                new_meme_id = request.form.get('new_meme_id')
                new_type = request.form.get('new_type')
                new_description = request.form.get('new_description')
                new_meme_url = request.form.get('new_meme_url')
                new_owner = request.form.get('new_owner')
                new_download_counts = request.form.get('new_download_counts', 0)
                if new_meme_id.isdigit() and new_owner.isdigit():
                    try:
                        with psycopg.connect(DATABASE_URL) as conn:
                            with conn.cursor() as cur:
                                cur.execute('INSERT INTO memes (meme_id, meme_url, meme_description, meme_download_counts, type, owner) VALUES (%s, %s, %s, %s, %s, %s)',
                                          (int(new_meme_id), new_meme_url, new_description, int(new_download_counts), new_type, int(new_owner)))
                                conn.commit()
                                message = f"Meme {new_meme_id} added successfully!"
                                next_meme_id = get_next_id('memes')  # Update for next insertion
                    except psycopg.Error as e:
                        message = f"Database error adding meme: {str(e)}"
            elif 'upload_thumbnail' in request.form:
                meme_id = request.form.get('meme_id')
                thumbnail = request.files.get('thumbnail')
                if meme_id.isdigit() and thumbnail and thumbnail.filename.lower().endswith(('.jpg', '.jpeg')):
                    try:
                        # Create thumbnails directory if it doesn't exist
                        thumbnail_dir = os.path.join(os.path.dirname(__file__), 'static', 'thumbnails')
                        os.makedirs(thumbnail_dir, exist_ok=True)
                        
                        # Base filename using meme_id
                        filename = f"{meme_id}.jpg"
                        thumbnail_path = os.path.join(thumbnail_dir, filename)
                        thumbnail.save(thumbnail_path)
                        
                        message = f"Thumbnail uploaded successfully for meme {meme_id} at /static/thumbnails/{filename}"
                    except Exception as e:
                        message = f"Error uploading thumbnail: {str(e)}"

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
                    if not isinstance(row, tuple) or len(row) != 6:  # Adjusted to 6 columns
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
                cur.execute('SELECT id, username, password, points FROM users')
                users = [{'id': row[0], 'username': row[1], 'password': row[2], 'points': row[3]} for row in cur.fetchall()]
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
    return hashlib.sha256(password.encode()).hexdigest()

# Increment download count and return download URL
@memes_bp.route('/add_point_and_redirect/<int:meme_id>', methods=['POST'])
def add_point_and_redirect(meme_id):
    url = request.json.get('url')  # Expect URL in request body
    if not url:
        current_app.logger.error("No URL provided in request body")
        return jsonify({'success': False, 'error': 'No URL provided'}), 400

    try:
        with psycopg.connect(DATABASE_URL) as conn:
            with conn.cursor() as cur:
                cur.execute('UPDATE memes SET meme_download_counts = meme_download_counts + 1 WHERE meme_id = %s', (meme_id,))
                if cur.rowcount == 0:
                    current_app.logger.error(f"Meme ID {meme_id} not found")
                    return jsonify({'success': False, 'error': 'Meme not found'}), 404
                conn.commit()
                current_app.logger.debug(f"Incremented download count for meme_id {meme_id}")
        # Transform URL if it's a Google Drive link
        transformed_url = get_download_url(url)
        current_app.logger.debug(f"Returning download URL: {transformed_url}")
        return jsonify({'success': True, 'download_url': transformed_url})
    except psycopg.Error as e:
        current_app.logger.error(f"Database error in add_point_and_redirect: {str(e)}")
        return jsonify({'success': False, 'error': 'Database error updating download count'}), 500
    except Exception as e:
        current_app.logger.error(f"Unexpected error in add_point_and_redirect: {str(e)}")
        return jsonify({'success': False, 'error': 'Unexpected error updating download count'}), 500

# Increment download count (for AJAX calls)
@memes_bp.route('/increment_download/<int:meme_id>', methods=['POST'])
def increment_download(meme_id):
    try:
        with psycopg.connect(DATABASE_URL) as conn:
            with conn.cursor() as cur:
                cur.execute('UPDATE memes SET meme_download_counts = meme_download_counts + 1 WHERE meme_id = %s', (meme_id,))
                if cur.rowcount == 0:
                    return jsonify({'success': False, 'error': 'Meme not found.'})
                conn.commit()
                current_app.logger.debug(f"Incremented download count for meme_id {meme_id}")
                return jsonify({'success': True})
    except psycopg.Error as e:
        current_app.logger.error(f"Database error incrementing download count: {str(e)}")
        return jsonify({'success': False, 'error': 'Database error.'}), 500
    except Exception as e:
        current_app.logger.error(f"Unexpected error incrementing download count: {str(e)}")
        return jsonify({'success': False, 'error': 'Unexpected error.'}), 500

# Check if a file exists in the static folder
@memes_bp.route('/check_file/<path:filename>')
def check_file(filename):
    file_path = os.path.join(current_app.static_folder, filename)
    return jsonify({'exists': os.path.isfile(file_path)})

# Custom filter for download URL
def get_download_url(url):
    if url and 'drive.google.com/file/d/' in url:
        match = re.search(r'https://drive.google.com/file/d/([^/]+)/view\?usp=drive_link', url)
        if match:
            file_id = match.group(1)
            return f"https://drive.google.com/uc?export=download&id={file_id}"
    return url

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
                        owner INTEGER DEFAULT 1,
                        UNIQUE (meme_url)
                    )
                ''')
                cur.execute('SELECT COUNT(*) FROM memes')
                count = cur.fetchone()[0]
                if count == 0:
                    cur.execute('INSERT INTO memes (meme_url, meme_description, type, owner) VALUES (%s, %s, %s, %s)',
                                ('https://drive.google.com/file/d/1abc123XYZ/view', 'Funny Cat', 'image', 1))
                    conn.commit()
                    current_app.logger.info(f"Initialized memes table with {count + 1} records")
                else:
                    current_app.logger.info(f"Memes table already contains {count} records, skipping full reinitialization.")
                cur.execute('''
                    CREATE TABLE IF NOT EXISTS users (
                        id SERIAL PRIMARY KEY,
                        ip_address TEXT NOT NULL,
                        username TEXT NOT NULL UNIQUE,
                        password TEXT NOT NULL,
                        user_type TEXT DEFAULT 'Guest',
                        points INTEGER DEFAULT 0,
                        word_list TEXT DEFAULT 'words.txt'
                    )
                ''')
                cur.execute('SELECT COUNT(*) FROM users')
                count = cur.fetchone()[0]
                if count == 0:
                    cur.execute('INSERT INTO users (ip_address, username, password, user_type, points, word_list) VALUES (%s, %s, %s, %s, %s, %s)',
                                ('0.0.0.0', 'admin', hash_password('admin123'), 'Admin', 100, 'words.txt'))
                    conn.commit()
                    current_app.logger.info(f"Initialized users table with {count + 1} records")
                else:
                    current_app.logger.info(f"Users table already contains {count} records, skipping full reinitialization.")
                cur.execute('''
                    CREATE TABLE IF NOT EXISTS user_stats (
                        user_id INTEGER PRIMARY KEY,
                        FOREIGN KEY (user_id) REFERENCES users(id)
                    )
                ''')
                conn.commit()
    except psycopg.Error as e:
        current_app.logger.error(f"Database initialization error: {str(e)}")
        raise
