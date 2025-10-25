from flask import Blueprint, render_template, request, session, redirect, url_for, current_app, jsonify
import psycopg
import os
import re
from datetime import datetime
import hashlib
import cv2

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

# Function to safely delete existing files (NEW PATHS)
def delete_existing_files(meme_id):
    """Delete existing video and thumbnail files for a given meme_id"""
    try:
        video_dir = os.path.join(os.path.dirname(__file__), 'static', 'vids')
        thumbnail_dir = os.path.join(os.path.dirname(__file__), 'static', 'thumbs')
        
        video_path = os.path.join(video_dir, f"{meme_id}.mp4")
        thumbnail_path = os.path.join(thumbnail_dir, f"{meme_id}.jpg")
        
        deleted_files = []
        if os.path.exists(video_path):
            os.remove(video_path)
            deleted_files.append(f"vids/{meme_id}.mp4")
        if os.path.exists(thumbnail_path):
            os.remove(thumbnail_path)
            deleted_files.append(f"thumbs/{meme_id}.jpg")
        
        current_app.logger.info(f"Deleted files for meme_id {meme_id}: {deleted_files}")
        return deleted_files
    except Exception as e:
        current_app.logger.error(f"Error deleting files for meme_id {meme_id}: {str(e)}")
        return []

# Helper function to hash password
def hash_password(password):
    return hashlib.sha256(password.encode()).hexdigest()

# Memes route
@memes_bp.route('/memes')
def memes():
    try:
        with psycopg.connect(DATABASE_URL) as conn:
            with conn.cursor() as cur:
                cur.execute('SELECT meme_id, meme_url, meme_description, meme_download_counts, type, owner, thumbnail_url FROM memes ORDER BY meme_id')
                rows = cur.fetchall()
                memes = []
                for row in rows:
                    if not isinstance(row, tuple) or len(row) != 7:
                        continue
                    memes.append({
                        'meme_id': row[0],
                        'meme_url': row[1],
                        'meme_description': row[2],
                        'meme_download_counts': row[3],
                        'type': row[4],
                        'owner': row[5],
                        'thumbnail_url': row[6]
                    })
                cur.execute('SELECT id, username FROM users')
                users = [{'id': row[0], 'username': row[1]} for row in cur.fetchall()]
                cur.execute('SELECT COUNT(*) FROM memes')
                meme_count = cur.fetchone()[0]
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

        return render_template('memes.html', memes=memes, users=users, meme_count=meme_count, total_downloads=total_downloads, username=username, user_type=user_type, points=points if user_type != 'Guest' else None, message=None)
    except Exception as e:
        current_app.logger.error(f"Error in memes: {str(e)}")
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
                    delete_existing_files(int(meme_id))
                    try:
                        with psycopg.connect(DATABASE_URL) as conn:
                            with conn.cursor() as cur:
                                cur.execute('DELETE FROM memes WHERE meme_id = %s', (int(meme_id),))
                                conn.commit()
                                message = f"Meme {meme_id} deleted successfully (including files)!"
                    except psycopg.Error as e:
                        message = f"Database error deleting meme: {str(e)}"
            elif 'edit_meme_id' in request.form:
                meme_id = request.form.get('edit_meme_id')
                new_type = request.form.get('new_type')
                new_description = request.form.get('new_description')
                new_meme_url = request.form.get('new_meme_url')
                new_owner = request.form.get('new_owner')
                new_download_counts = request.form.get('new_download_counts')
                if not meme_id.isdigit() or not new_owner.isdigit() or not new_meme_url or not new_meme_url.strip():
                    message = "Invalid input data."
                else:
                    try:
                        with psycopg.connect(DATABASE_URL) as conn:
                            with conn.cursor() as cur:
                                cur.execute('UPDATE memes SET type = %s, meme_description = %s, meme_url = %s, owner = %s, meme_download_counts = %s WHERE meme_id = %s',
                                          (new_type, new_description, new_meme_url, int(new_owner), int(new_download_counts), int(meme_id)))
                                if cur.rowcount == 0:
                                    message = f"No meme found with ID {meme_id} to update."
                                else:
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
                if new_meme_id.isdigit() and new_owner.isdigit() and new_meme_url and new_meme_url.strip():
                    try:
                        with psycopg.connect(DATABASE_URL) as conn:
                            with conn.cursor() as cur:
                                cur.execute('INSERT INTO memes (meme_id, meme_url, meme_description, meme_download_counts, type, owner, thumbnail_url) VALUES (%s, %s, %s, %s, %s, %s, %s)',
                                          (int(new_meme_id), new_meme_url, new_description, int(new_download_counts), new_type, int(new_owner), ''))
                                conn.commit()
                                message = f"Meme {new_meme_id} added manually successfully!"
                                next_meme_id = get_next_id('memes')
                    except psycopg.Error as e:
                        message = f"Database error adding meme: {str(e)}"
            elif 'upload_video' in request.form:
                video = request.files.get('video')
                upload_meme_id = request.form.get('upload_meme_id')
                meme_type = request.form.get('meme_type')
                overwrite_files = request.form.get('overwrite_files') == 'on'
                
                if not video or not video.filename.lower().endswith('.mp4'):
                    message = "Please select a valid MP4 video file."
                elif not upload_meme_id.isdigit():
                    message = "Invalid MEME ID selected."
                elif meme_type not in ['GM', 'GN', 'OTHER', 'CRYPTO', 'GRAWK']:
                    message = "Invalid meme type selected."
                else:
                    meme_id = int(upload_meme_id)
                    max_size = 25 * 1024 * 1024
                    if video.content_length and video.content_length > max_size:
                        message = "Video upload failed: File size exceeds 25MB limit."
                    else:
                        try:
                            is_new = meme_id == next_meme_id
                            is_overwrite = not is_new and overwrite_files
                            
                            deleted_files = []
                            if is_overwrite:
                                deleted_files = delete_existing_files(meme_id)
                            
                            video_dir = os.path.join(os.path.dirname(__file__), 'static', 'vids')
                            thumbnail_dir = os.path.join(os.path.dirname(__file__), 'static', 'thumbs')
                            os.makedirs(video_dir, exist_ok=True)
                            os.makedirs(thumbnail_dir, exist_ok=True)
                            
                            video_path = os.path.join(video_dir, f"{meme_id}.mp4")
                            video.save(video_path)
                            
                            base_description = os.path.splitext(video.filename)[0]
                            
                            with psycopg.connect(DATABASE_URL) as conn:
                                with conn.cursor() as cur:
                                    if is_new:
                                        cur.execute('''
                                            INSERT INTO memes (meme_id, meme_url, meme_description, meme_download_counts, type, owner, thumbnail_url) 
                                            VALUES (%s, %s, %s, %s, %s, %s, %s)
                                        ''', (meme_id, '', base_description, 0, meme_type, 3, ''))
                                    else:
                                        cur.execute('UPDATE memes SET type = %s, meme_description = %s WHERE meme_id = %s',
                                                  (meme_type, base_description, meme_id))
                                        if cur.rowcount == 0:
                                            cur.execute('''
                                                INSERT INTO memes (meme_id, meme_url, meme_description, meme_download_counts, type, owner, thumbnail_url) 
                                                VALUES (%s, %s, %s, %s, %s, %s, %s)
                                            ''', (meme_id, '', base_description, 0, meme_type, 3, ''))
                                    conn.commit()
                            
                            thumbnail_path = os.path.join(thumbnail_dir, f"{meme_id}.jpg")
                            cap = cv2.VideoCapture(video_path)
                            if cap.isOpened():
                                ret, frame = cap.read()
                                if ret:
                                    frame = cv2.resize(frame, (200, 200), interpolation=cv2.INTER_AREA)
                                    cv2.imwrite(thumbnail_path, frame)
                                    with psycopg.connect(DATABASE_URL) as conn:
                                        with conn.cursor() as cur:
                                            cur.execute('UPDATE memes SET thumbnail_url = %s WHERE meme_id = %s',
                                                      (f"/static/thumbs/{meme_id}.jpg", meme_id))
                                            conn.commit()
                                cap.release()
                            
                            file_action = "replaced" if is_overwrite else "created"
                            deleted_msg = f" (deleted: {', '.join(deleted_files)})" if deleted_files else ""
                            message = f"Video {file_action} successfully for MEME ID {meme_id}!{deleted_msg}"
                            if is_new:
                                next_meme_id = get_next_id('memes')
                                
                        except Exception as e:
                            message = f"Error uploading video or generating thumbnail: {str(e)}"

    if not authenticated:
        return render_template('admin.html', message=message, authenticated=authenticated, next_meme_id=next_meme_id)

    try:
        with psycopg.connect(DATABASE_URL) as conn:
            with conn.cursor() as cur:
                cur.execute('SELECT meme_id, meme_url, meme_description, meme_download_counts, type, owner, thumbnail_url FROM memes ORDER BY meme_id')
                rows = cur.fetchall()
                memes = []
                for row in rows:
                    if not isinstance(row, tuple) or len(row) != 7:
                        continue
                    memes.append({
                        'meme_id': row[0],
                        'meme_url': row[1],
                        'meme_description': row[2],
                        'meme_download_counts': row[3],
                        'type': row[4],
                        'owner': row[5],
                        'thumbnail_url': row[6]
                    })
                cur.execute('SELECT id, username, password, points FROM users')
                users = [{'id': row[0], 'username': row[1], 'password': row[2], 'points': row[3]} for row in cur.fetchall()]
                cur.execute('SELECT COUNT(*) FROM memes')
                meme_count = cur.fetchone()[0]
        return render_template('admin.html', memes=memes, users=users, meme_count=meme_count, message=message, authenticated=authenticated, next_meme_id=next_meme_id)
    except Exception as e:
        current_app.logger.error(f"Error in admin: {str(e)}")
        return render_template('admin.html', memes=[], users=[], meme_count=0, message="Error fetching data.", authenticated=authenticated, next_meme_id=next_meme_id)

# Register route (AJAX)
@memes_bp.route('/memes/register', methods=['POST'])
def register():
    username = request.form.get('register_username', '').strip()
    password = request.form.get('register_password', '')

    if not username or not password:
        return jsonify({'message': 'Username and password are required.', 'success': False})
    if not (1 <= len(username) <= 12 and username.isalnum()):
        return jsonify({'message': 'Username must be 1-12 alphanumeric characters.', 'success': False})

    try:
        with psycopg.connect(DATABASE_URL) as conn:
            with conn.cursor() as cur:
                cur.execute('SELECT 1 FROM users WHERE username = %s', (username,))
                if cur.fetchone():
                    return jsonify({'message': 'Username already taken.', 'success': False})
                cur.execute('INSERT INTO users (ip_address, username, password, user_type, points, word_list) VALUES (%s, %s, %s, %s, %s, %s)',
                          (request.remote_addr, username, hash_password(password), 'Member', 0, 'words.txt'))
                cur.execute('INSERT INTO user_stats (user_id) VALUES (currval(\'users_id_seq\'))')
                conn.commit()
                session.clear()
                session['username'] = username
                session['user_type'] = 'Member'
                session['word_list'] = 'words.txt'
                return jsonify({'message': 'Registration successful! Welcome, Member!', 'success': True})
    except psycopg.Error as e:
        current_app.logger.error(f"Database error during registration: {str(e)}")
        return jsonify({'message': 'Registration failed. Try again.', 'success': False})

# Login route (AJAX)
@memes_bp.route('/memes/login', methods=['POST'])
def login():
    username = request.form.get('login_username', '').strip()
    password = request.form.get('login_password', '')

    if not username or not password:
        return jsonify({'message': 'Username and password required.', 'success': False})

    try:
        with psycopg.connect(DATABASE_URL) as conn:
            with conn.cursor() as cur:
                cur.execute('SELECT password, user_type, points FROM users WHERE username = %s', (username,))
                result = cur.fetchone()
                if result and result[0] == hash_password(password):
                    session.clear()
                    session['username'] = username
                    session['user_type'] = result[1]
                    session['word_list'] = 'words.txt'
                    points = result[2]
                    return jsonify({'message': f'Welcome back, {username}!', 'success': True})
                else:
                    return jsonify({'message': 'Invalid username or password.', 'success': False})
    except psycopg.Error as e:
        current_app.logger.error(f"Database error during login: {str(e)}")
        return jsonify({'message': 'Login failed. Try again.', 'success': False})

# Increment download count routes
@memes_bp.route('/add_point_and_redirect/<int:meme_id>', methods=['POST'])
def add_point_and_redirect(meme_id):
    url = request.json.get('url')
    if not url:
        return jsonify({'success': False, 'error': 'No URL provided'}), 400

    try:
        with psycopg.connect(DATABASE_URL) as conn:
            with conn.cursor() as cur:
                cur.execute('UPDATE memes SET meme_download_counts = meme_download_counts + 1 WHERE meme_id = %s', (meme_id,))
                if cur.rowcount == 0:
                    return jsonify({'success': False, 'error': 'Meme not found'}), 404
                conn.commit()
        transformed_url = get_download_url(url)
        return jsonify({'success': True, 'download_url': transformed_url})
    except Exception as e:
        current_app.logger.error(f"Error in add_point_and_redirect: {str(e)}")
        return jsonify({'success': False, 'error': 'Database error'}), 500

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
    except Exception as e:
        current_app.logger.error(f"Error incrementing download: {str(e)}")
        return jsonify({'success': False, 'error': 'Database error.'}), 500

# Check file route
@memes_bp.route('/check_file/<path:filename>')
def check_file(filename):
    file_path = os.path.join(current_app.static_folder, filename)
    return jsonify({'exists': os.path.isfile(file_path)})

# Download URL helper
def get_download_url(url):
    if url and 'drive.google.com/file/d/' in url:
        match = re.search(r'https://drive.google.com/file/d/([^/]+)/view\?usp=drive_link', url)
        if match:
            file_id = match.group(1)
            return f"https://drive.google.com/uc?export=download&id={file_id}"
    return url

# Database initialization function
def init_db():
    try:
        with psycopg.connect(DATABASE_URL) as conn:
            with conn.cursor() as cur:
                # Create memes table
                cur.execute('''
                    CREATE TABLE IF NOT EXISTS memes (
                        meme_id SERIAL PRIMARY KEY,
                        meme_url TEXT NOT NULL,
                        meme_description TEXT NOT NULL,
                        meme_download_counts INTEGER DEFAULT 0,
                        type TEXT DEFAULT 'OTHER',
                        owner INTEGER DEFAULT 3,
                        thumbnail_url TEXT,
                        CONSTRAINT memes_type_check CHECK (type IN ('GM', 'GN', 'OTHER', 'CRYPTO', 'GRAWK'))
                    )
                ''')
                
                # Create users table
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
                
                # Create user_stats table
                cur.execute('''
                    CREATE TABLE IF NOT EXISTS user_stats (
                        user_id INTEGER PRIMARY KEY,
                        FOREIGN KEY (user_id) REFERENCES users(id)
                    )
                ''')
                
                conn.commit()
                current_app.logger.info("Database tables verified/created successfully")
    except psycopg.Error as e:
        current_app.logger.error(f"Database initialization error: {str(e)}")
        raise
