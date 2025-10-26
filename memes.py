from flask import Blueprint, render_template, request, session, redirect, url_for, current_app, jsonify
import psycopg
import os
import re
import hashlib
import cv2

# Blueprint
memes_bp = Blueprint('memes', __name__)

# === CONFIG ===
DATABASE_URL = os.environ.get('DATABASE_URL')
if not DATABASE_URL:
    raise ValueError("DATABASE_URL environment variable must be set")

# Superuser
SANTO_USERNAME = "santo"
ADMIN_PASS = os.environ.get('ADMIN_PASS')  # REQUIRED
if not ADMIN_PASS:
    raise ValueError("ADMIN_PASS environment variable must be set")

# Optional: member login pass (not used in current flow)
MEMBER_PASS = os.environ.get('MEMBER_PASS', 'member123')

# === HELPERS ===
def get_next_id(table_name):
    try:
        with psycopg.connect(DATABASE_URL) as conn:
            with conn.cursor() as cur:
                cur.execute(f"SELECT COALESCE(MAX(meme_id), 0) + 1 FROM {table_name}")
                return cur.fetchone()[0]
    except psycopg.Error as e:
        current_app.logger.error(f"get_next_id error: {e}")
        return 1

def delete_existing_files(meme_id):
    try:
        vdir = os.path.join(os.path.dirname(__file__), 'static', 'vids')
        tdir = os.path.join(os.path.dirname(__file__), 'static', 'thumbs')
        vpath = os.path.join(vdir, f"{meme_id}.mp4")
        tpath = os.path.join(tdir, f"{meme_id}.jpg")
        deleted = []
        if os.path.exists(vpath):
            os.remove(vpath)
            deleted.append(f"vids/{meme_id}.mp4")
        if os.path.exists(tpath):
            os.remove(tpath)
            deleted.append(f"thumbs/{meme_id}.jpg")
        current_app.logger.info(f"Deleted files for meme_id {meme_id}: {deleted}")
        return deleted
    except Exception as e:
        current_app.logger.error(f"delete_files error: {e}")
        return []

def hash_password(pw):
    return hashlib.sha256(pw.encode()).hexdigest()

# === ROUTES ===
@memes_bp.route('/memes')
def memes():
    try:
        with psycopg.connect(DATABASE_URL) as conn:
            with conn.cursor() as cur:
                cur.execute('SELECT meme_id, meme_url, meme_description, meme_download_counts, type, owner, thumbnail_url FROM memes ORDER BY meme_id')
                rows = cur.fetchall()
                memes = []
                for row in rows:
                    if len(row) != 7:
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
                users = [{'id': r[0], 'username': r[1]} for r in cur.fetchall()]
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
                current_app.logger.error(f"User fetch error: {e}")

        return render_template('memes.html',
                               memes=memes,
                               users=users,
                               meme_count=meme_count,
                               total_downloads=total_downloads,
                               username=username,
                               user_type=user_type,
                               points=points if user_type != 'Guest' else None,
                               message=None)
    except Exception as e:
        current_app.logger.error(f"Memes route error: {e}")
        return render_template('memes.html', memes=[], users=[], message="Error loading memes.", meme_count=0, total_downloads=0, username=None, user_type='Guest', points=0)

@memes_bp.route('/admin', methods=['GET', 'POST'])
def admin():
    message = None
    username = session.get('username')
    user_type = session.get('user_type', 'Guest')
    authenticated = bool(username and user_type in ['Member', 'Admin'])
    is_santo = (username == SANTO_USERNAME and user_type == 'Admin')
    next_meme_id = get_next_id('memes')

    # === LOGOUT ===
    if request.args.get('logout'):
        session.clear()
        return redirect('/admin')

    # === NOT AUTHENTICATED → SHOW LOGIN ===
    if not authenticated:
        if request.method == 'POST':
            password = request.form.get('admin_pass', '').strip()
            if password == ADMIN_PASS:
                session['username'] = SANTO_USERNAME
                session['user_type'] = 'Admin'
                return redirect('/admin')
            else:
                message = "Incorrect password."
        return render_template('admin.html', message=message, authenticated=False)

    # === GET CURRENT USER ID (for members) ===
    user_id = None
    if not is_santo:
        try:
            with psycopg.connect(DATABASE_URL) as conn:
                with conn.cursor() as cur:
                    cur.execute('SELECT id FROM users WHERE username = %s', (username,))
                    result = cur.fetchone()
                    if result:
                        user_id = result[0]
        except psycopg.Error as e:
            current_app.logger.error(f"User ID fetch error: {e}")
            message = "Database error."
            return render_template('admin.html', **locals())

        if not user_id:
            session.clear()
            return redirect('/admin')

    # === POST ACTIONS ===
    if request.method == 'POST':
        # === UPLOAD VIDEO ===
        if 'upload_video' in request.form:
            video = request.files.get('video')
            upload_meme_id = request.form.get('upload_meme_id')
            meme_type = request.form.get('meme_type')
            overwrite = 'overwrite_files' in request.form

            if not video or not video.filename.lower().endswith('.mp4'):
                message = "Please select a valid MP4 file."
            elif not upload_meme_id.isdigit():
                message = "Invalid MEME ID."
            elif meme_type not in ['GM', 'GN', 'OTHER', 'CRYPTO', 'GRAWK']:
                message = "Invalid meme type."
            else:
                meme_id = int(upload_meme_id)
                is_new = meme_id == next_meme_id

                # Permission: non-santo can only replace own
                if not is_new and not is_santo:
                    try:
                        with psycopg.connect(DATABASE_URL) as conn:
                            with conn.cursor() as cur:
                                cur.execute('SELECT owner FROM memes WHERE meme_id = %s', (meme_id,))
                                row = cur.fetchone()
                                if row and row[0] != user_id:
                                    message = "You can only replace your own videos."
                                    return render_template('admin.html', **locals())
                    except psycopg.Error as e:
                        message = "Database error."
                        return render_template('admin.html', **locals())

                if video.content_length and video.content_length > 25 * 1024 * 1024:
                    message = "File exceeds 25MB limit."
                else:
                    try:
                        deleted = []
                        if not is_new and overwrite:
                            deleted = delete_existing_files(meme_id)

                        vdir = os.path.join(os.path.dirname(__file__), 'static', 'vids')
                        tdir = os.path.join(os.path.dirname(__file__), 'static', 'thumbs')
                        os.makedirs(vdir, exist_ok=True)
                        os.makedirs(tdir, exist_ok=True)

                        video_path = os.path.join(vdir, f"{meme_id}.mp4")
                        video.save(video_path)
                        desc = os.path.splitext(video.filename)[0]

                        owner_id = 3 if is_santo else user_id  # fallback 3 for santo

                        with psycopg.connect(DATABASE_URL) as conn:
                            with conn.cursor() as cur:
                                if is_new:
                                    cur.execute('''
                                        INSERT INTO memes (meme_id, meme_url, meme_description, meme_download_counts, type, owner, thumbnail_url)
                                        VALUES (%s, %s, %s, %s, %s, %s, %s)
                                    ''', (meme_id, '', desc, 0, meme_type, owner_id, ''))
                                else:
                                    cur.execute('UPDATE memes SET type = %s, meme_description = %s WHERE meme_id = %s',
                                              (meme_type, desc, meme_id))
                                    if cur.rowcount == 0:
                                        cur.execute('''
                                            INSERT INTO memes (meme_id, meme_url, meme_description, meme_download_counts, type, owner, thumbnail_url)
                                            VALUES (%s, %s, %s, %s, %s, %s, %s)
                                        ''', (meme_id, '', desc, 0, meme_type, owner_id, ''))
                                conn.commit()

                        # Generate thumbnail
                        cap = cv2.VideoCapture(video_path)
                        if cap.isOpened():
                            ret, frame = cap.read()
                            if ret:
                                frame = cv2.resize(frame, (200, 200), interpolation=cv2.INTER_AREA)
                                thumb_path = os.path.join(tdir, f"{meme_id}.jpg")
                                cv2.imwrite(thumb_path, frame)
                                with psycopg.connect(DATABASE_URL) as conn:
                                    with conn.cursor() as cur:
                                        cur.execute('UPDATE memes SET thumbnail_url = %s WHERE meme_id = %s',
                                                  (f"/static/thumbs/{meme_id}.jpg", meme_id))
                                        conn.commit()
                            cap.release()

                        action = "uploaded" if is_new else "replaced"
                        deleted_msg = f" (deleted: {', '.join(deleted)})" if deleted else ""
                        message = f"Video {action} for MEME ID {meme_id}!{deleted_msg}"
                        if is_new:
                            next_meme_id = get_next_id('memes')
                        return redirect('/admin?upload_success=true')
                    except Exception as e:
                        current_app.logger.error(f"Upload error: {e}")
                        message = f"Upload failed: {str(e)}"

        # === DELETE MEME ===
        elif 'delete_meme_id' in request.form:
            meme_id = request.form['delete_meme_id']
            if meme_id.isdigit():
                try:
                    with psycopg.connect(DATABASE_URL) as conn:
                        with conn.cursor() as cur:
                            cur.execute('SELECT owner FROM memes WHERE meme_id = %s', (int(meme_id),))
                            row = cur.fetchone()
                            if row and (is_santo or row[0] == user_id):
                                delete_existing_files(int(meme_id))
                                cur.execute('DELETE FROM memes WHERE meme_id = %s', (int(meme_id),))
                                conn.commit()
                                message = f"Meme {meme_id} deleted."
                            else:
                                message = "You can only delete your own memes."
                except psycopg.Error as e:
                    message = f"Error: {e}"

        # === EDIT MEME (non-santo only) ===
        elif 'edit_meme_id' in request.form and not is_santo:
            meme_id = request.form['edit_meme_id']
            new_type = request.form['new_type']
            new_desc = request.form['new_description']
            if meme_id.isdigit() and new_type in ['GM', 'GN', 'OTHER', 'CRYPTO', 'GRAWK']:
                try:
                    with psycopg.connect(DATABASE_URL) as conn:
                        with conn.cursor() as cur:
                            cur.execute('SELECT owner FROM memes WHERE meme_id = %s', (int(meme_id),))
                            row = cur.fetchone()
                            if row and row[0] == user_id:
                                cur.execute('UPDATE memes SET type = %s, meme_description = %s WHERE meme_id = %s',
                                          (new_type, new_desc, int(meme_id)))
                                conn.commit()
                                message = f"Meme {meme_id} updated."
                            else:
                                message = "Permission denied."
                except psycopg.Error as e:
                    message = f"Error: {e}"

        # === SANTO-ONLY: USER MANAGEMENT ===
        if is_santo:
            if 'delete_username' in request.form:
                uname = request.form['delete_username']
                if uname != SANTO_USERNAME:
                    try:
                        with psycopg.connect(DATABASE_URL) as conn:
                            with conn.cursor() as cur:
                                cur.execute('DELETE FROM users WHERE username = %s', (uname,))
                                conn.commit()
                                message = f"User {uname} deleted."
                    except psycopg.Error as e:
                        message = f"Error: {e}"
            elif 'edit_username' in request.form:
                old = request.form['edit_username']
                new_u = request.form['new_username']
                new_p = request.form['new_password']
                new_pts = request.form['new_points']
                try:
                    with psycopg.connect(DATABASE_URL) as conn:
                        with conn.cursor() as cur:
                            cur.execute('UPDATE users SET username = %s, password = %s, points = %s WHERE username = %s',
                                      (new_u, hash_password(new_p), new_pts, old))
                            conn.commit()
                            message = f"User {old} updated to {new_u}."
                except psycopg.Error as e:
                    message = f"Error: {e}"

    # === FETCH DATA FOR RENDERING ===
    try:
        with psycopg.connect(DATABASE_URL) as conn:
            with conn.cursor() as cur:
                # Fetch ALL memes for santo, filtered for others
                if is_santo:
                    cur.execute('SELECT meme_id, type, meme_description, owner, meme_download_counts FROM memes ORDER BY meme_id')
                    all_memes = [dict(zip(['meme_id','type','meme_description','owner','meme_download_counts'], r)) for r in cur.fetchall()]
                    memes = all_memes  # santo sees all
                else:
                    cur.execute('SELECT meme_id, type, meme_description, owner, meme_download_counts FROM memes WHERE owner = %s ORDER BY meme_id', (user_id,))
                    memes = [dict(zip(['meme_id','type','meme_description','owner','meme_download_counts'], r)) for r in cur.fetchall()]
                    all_memes = []  # not used

                # Fetch users only for santo
                if is_santo:
                    cur.execute('SELECT id, username, password, points FROM users')
                    users = [dict(zip(['id','username','password','points'], r)) for r in cur.fetchall()]
                else:
                    users = []
    except psycopg.Error as e:
        memes = []
        all_memes = []
        users = []
        message = f"Database error: {e}"

    return render_template('admin.html',
                           authenticated=authenticated,
                           is_santo=is_santo,
                           username=username,
                           memes=memes,
                           all_memes=all_memes,  # ← NEW: for upload dropdown
                           users=users,
                           next_meme_id=next_meme_id,
                           message=message)

# === AJAX: REGISTER ===
@memes_bp.route('/memes/register', methods=['POST'])
def register():
    username = request.form.get('register_username', '').strip()
    password = request.form.get('register_password', '')

    if not username or not password:
        return jsonify({'success': False, 'message': 'Username and password required.'})
    if not (1 <= len(username) <= 12 and username.isalnum()):
        return jsonify({'success': False, 'message': 'Username: 1–12 alphanumeric chars.'})

    try:
        with psycopg.connect(DATABASE_URL) as conn:
            with conn.cursor() as cur:
                cur.execute('SELECT 1 FROM users WHERE username = %s', (username,))
                if cur.fetchone():
                    return jsonify({'success': False, 'message': 'Username taken.'})
                cur.execute('INSERT INTO users (ip_address, username, password, user_type, points, word_list) VALUES (%s, %s, %s, %s, %s, %s)',
                          (request.remote_addr, username, hash_password(password), 'Member', 0, 'words.txt'))
                cur.execute('INSERT INTO user_stats (user_id) VALUES (currval(\'users_id_seq\'))')
                conn.commit()
                session.clear()
                session['username'] = username
                session['user_type'] = 'Member'
                return jsonify({'success': True, 'message': f'Welcome, {username}!'})
    except psycopg.Error as e:
        current_app.logger.error(f"Register error: {e}")
        return jsonify({'success': False, 'message': 'Registration failed.'})

# === AJAX: LOGIN ===
@memes_bp.route('/memes/login', methods=['POST'])
def login():
    username = request.form.get('login_username', '').strip()
    password = request.form.get('login_password', '')

    if not username or not password:
        return jsonify({'success': False, 'message': 'Credentials required.'})

    try:
        with psycopg.connect(DATABASE_URL) as conn:
            with conn.cursor() as cur:
                cur.execute('SELECT password, user_type, points FROM users WHERE username = %s', (username,))
                result = cur.fetchone()
                if result and result[0] == hash_password(password):
                    session.clear()
                    session['username'] = username
                    session['user_type'] = result[1]
                    return jsonify({'success': True, 'message': f'Welcome back, {username}!'})
                else:
                    return jsonify({'success': False, 'message': 'Invalid credentials.'})
    except psycopg.Error as e:
        current_app.logger.error(f"Login error: {e}")
        return jsonify({'success': False, 'message': 'Login failed.'})

# === INCREMENT DOWNLOAD ===
@memes_bp.route('/add_point_and_redirect/<int:meme_id>', methods=['POST'])
def add_point_and_redirect(meme_id):
    url = request.json.get('url')
    if not url:
        return jsonify({'success': False, 'error': 'No URL'}), 400

    try:
        with psycopg.connect(DATABASE_URL) as conn:
            with conn.cursor() as cur:
                cur.execute('UPDATE memes SET meme_download_counts = meme_download_counts + 1 WHERE meme_id = %s', (meme_id,))
                if cur.rowcount == 0:
                    return jsonify({'success': False, 'error': 'Meme not found'}), 404
                conn.commit()
        transformed = get_download_url(url)
        return jsonify({'success': True, 'download_url': transformed})
    except Exception as e:
        current_app.logger.error(f"Download error: {e}")
        return jsonify({'success': False, 'error': 'DB error'}), 500

@memes_bp.route('/increment_download/<int:meme_id>', methods=['POST'])
def increment_download(meme_id):
    try:
        with psycopg.connect(DATABASE_URL) as conn:
            with conn.cursor() as cur:
                cur.execute('UPDATE memes SET meme_download_counts = meme_download_counts + 1 WHERE meme_id = %s', (meme_id,))
                if cur.rowcount == 0:
                    return jsonify({'success': False, 'error': 'Not found'})
                conn.commit()
                return jsonify({'success': True})
    except Exception as e:
        current_app.logger.error(f"Increment error: {e}")
        return jsonify({'success': False, 'error': 'DB error'}), 500

# === CHECK FILE ===
@memes_bp.route('/check_file/<path:filename>')
def check_file(filename):
    file_path = os.path.join(current_app.static_folder, filename)
    return jsonify({'exists': os.path.isfile(file_path)})

# === HELPER: Google Drive URL ===
def get_download_url(url):
    if url and 'drive.google.com/file/d/' in url:
        match = re.search(r'https://drive.google.com/file/d/([^/]+)/', url)
        if match:
            return f"https://drive.google.com/uc?export=download&id={match.group(1)}"
    return url

# === DB INIT ===
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
                        type TEXT DEFAULT 'OTHER',
                        owner INTEGER DEFAULT 3,
                        thumbnail_url TEXT,
                        CONSTRAINT memes_type_check CHECK (type IN ('GM', 'GN', 'OTHER', 'CRYPTO', 'GRAWK'))
                    )
                ''')
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
                cur.execute('''
                    CREATE TABLE IF NOT EXISTS user_stats (
                        user_id INTEGER PRIMARY KEY,
                        FOREIGN KEY (user_id) REFERENCES users(id)
                    )
                ''')
                conn.commit()
                current_app.logger.info("DB tables initialized.")
    except psycopg.Error as e:
        current_app.logger.error(f"DB init error: {e}")
        raise
