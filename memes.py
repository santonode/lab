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
                column_name = {'memes': 'meme_id', 'users': 'id'}.get(table_name.split('_')[0], f"{table_name.split('_')[0]}_id")
                cur.execute(f'SELECT COALESCE(MAX({column_name}), 0) + 1 FROM {table_name}')
                return cur.fetchone()[0]
    except psycopg.Error as e:
        print(f"Database error getting next {table_name.split('_')[0]} ID: {str(e)}")
        return 1

# Generate username based on IP and session data
def generate_username(ip_address):
    seed = f"{ip_address}{datetime.now().microsecond}{random.randint(1000, 9999)}"
    hash_object = hashlib.md5(seed.encode())
    hash_hex = hash_object.hexdigest()[:8]
    username = ''.join(c for c in hash_hex if c.isalnum()).upper()[:12]
    return username

# Hash password for storage
def hash_password(password):
    return hashlib.sha256(password.encode()).hexdigest()

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
            else:
                message = "Incorrect admin password."
        elif 'delete' in request.form:
            delete_username = request.form.get('delete_username')
            try:
                with psycopg.connect(DATABASE_URL) as conn:
                    with conn.cursor() as cur:
                        cur.execute('DELETE FROM users WHERE username = %s RETURNING id', (delete_username,))
                        user_id = cur.fetchone()
                        if user_id:
                            cur.execute('DELETE FROM user_stats WHERE user_id = %s', (user_id[0],))
                            conn.commit()
                            message = f"User {delete_username} deleted successfully."
                        else:
                            message = f"User {delete_username} not found."
            except psycopg.Error as e:
                print(f"Database error during delete: {str(e)}")
                message = f"Error deleting user {delete_username}: {str(e)}"
        elif 'save' in request.form:
            edit_username = request.form.get('edit_username')
            new_username = request.form.get('new_username').strip()
            new_password = request.form.get('new_password')
            new_points = request.form.get('new_points', 0, type=int)
            if new_username and new_password and all(c.isalnum() for c in new_username) and 1 <= len(new_username) <= 12:
                try:
                    with psycopg.connect(DATABASE_URL) as conn:
                        with conn.cursor() as cur:
                            cur.execute('SELECT 1 FROM users WHERE username = %s AND username != %s', (new_username, edit_username))
                            if cur.fetchone():
                                message = "Username already taken."
                            else:
                                cur.execute('UPDATE users SET username = %s, password = %s, points = %s WHERE username = %s',
                                          (new_username, hash_password(new_password), new_points, edit_username))
                                conn.commit()
                                message = f"User {edit_username} updated to {new_username} successfully."
                except psycopg.Error as e:
                    print(f"Database error during update: {str(e)}")
                    message = f"Error updating user {edit_username}: {str(e)}"
            else:
                message = "Username must be 1-12 alphanumeric characters."
        elif 'delete_meme' in request.form:
            delete_meme_id = request.form.get('delete_meme_id', type=int)
            try:
                with psycopg.connect(DATABASE_URL) as conn:
                    with conn.cursor() as cur:
                        cur.execute('DELETE FROM memes WHERE meme_id = %s', (delete_meme_id,))
                        if cur.rowcount > 0:
                            conn.commit()
                            message = f"Meme with ID {delete_meme_id} deleted successfully."
                        else:
                            message = f"Meme with ID {delete_meme_id} not found."
            except psycopg.Error as e:
                print(f"Database error during meme delete: {str(e)}")
                message = f"Error deleting meme with ID {delete_meme_id}: {str(e)}"
        elif 'save_meme' in request.form:
            meme_id = request.form.get('edit_meme_id' if 'edit_meme_id' in request.form else 'new_meme_id', type=int)
            new_type = request.form.get('new_type').strip()
            new_description = request.form.get('new_description').strip()
            new_meme_url = request.form.get('new_meme_url')
            new_owner = request.form.get('new_owner', type=int)
            new_download_counts = request.form.get('new_download_counts', 0, type=int)
            if new_meme_url is None:
                new_meme_url = ''
            new_meme_url = new_meme_url.strip() if new_meme_url else ''
            valid_types = ['Other', 'GM', 'GN', 'Crypto', 'Grawk']
            if new_type in valid_types and new_description and new_meme_url and new_owner is not None:
                try:
                    with psycopg.connect(DATABASE_URL) as conn:
                        with conn.cursor() as cur:
                            if 'edit_meme_id' in request.form:
                                cur.execute('UPDATE memes SET type = %s, meme_description = %s, meme_url = %s, owner = %s, meme_download_counts = %s WHERE meme_id = %s',
                                          (new_type, new_description, new_meme_url, new_owner, new_download_counts, meme_id))
                                if cur.rowcount > 0:
                                    conn.commit()
                                    message = f"Meme with ID {meme_id} updated successfully."
                                else:
                                    message = f"Meme with ID {meme_id} not found."
                            elif 'add_meme' in request.form:
                                cur.execute('SELECT 1 FROM memes WHERE meme_id = %s', (meme_id,))
                                if cur.fetchone():
                                    message = f"Meme ID {meme_id} already exists."
                                else:
                                    cur.execute('INSERT INTO memes (meme_id, meme_url, meme_description, meme_download_counts, type, owner) VALUES (%s, %s, %s, %s, %s, %s)',
                                              (meme_id, new_meme_url, new_description, new_download_counts, new_type, new_owner))
                                    conn.commit()
                                    message = f"Meme added successfully with ID {meme_id}."
                except psycopg.Error as e:
                    print(f"Database error during meme update/add: {str(e)}")
                    message = f"Error {'updating' if 'edit_meme_id' in request.form else 'adding'} meme with ID {meme_id}: {str(e)}"
            else:
                message = "Invalid type, empty description, empty URL, or invalid owner ID. Type must be one of: Other, GM, GN, Crypto, Grawk. Owner ID must be a valid integer."
        elif 'save_user' in request.form and 'add_user' in request.form:
            new_username = request.form.get('new_username').strip()
            new_password = request.form.get('new_password')
            new_points = request.form.get('new_points', 0, type=int)
            if new_username and new_password and all(c.isalnum() for c in new_username) and 1 <= len(new_username) <= 12:
                try:
                    with psycopg.connect(DATABASE_URL) as conn:
                        with conn.cursor() as cur:
                            cur.execute('SELECT 1 FROM users WHERE username = %s', (new_username,))
                            if cur.fetchone():
                                message = "Username already taken."
                            else:
                                new_user_id = get_next_id('users')
                                cur.execute('INSERT INTO users (id, ip_address, username, password, user_type, points, word_list) VALUES (%s, %s, %s, %s, %s, %s, %s)',
                                          (new_user_id, request.remote_addr, new_username, hash_password(new_password), 'Member', new_points, 'words.txt'))
                                cur.execute('INSERT INTO user_stats (user_id) VALUES (%s)', (new_user_id,))
                                conn.commit()
                                message = f"User {new_username} added successfully with ID {new_user_id}."
                except psycopg.Error as e:
                    print(f"Database error during user add: {str(e)}")
                    message = f"Error adding user {new_username}: {str(e)}"
            else:
                message = "Username must be 1-12 alphanumeric characters."

    if not authenticated:
        return render_template('admin.html', authenticated=False, message=message)

    try:
        with psycopg.connect(DATABASE_URL) as conn:
            with conn.cursor() as cur:
                cur.execute('SELECT id, username, password, points FROM users')
                users = [{'id': row[0], 'username': row[1], 'password': row[2], 'points': row[3]} for row in cur.fetchall()]
                cur.execute('SELECT meme_id, type, meme_description, meme_download_counts, meme_url, owner FROM memes ORDER BY meme_id')
                memes = [{'meme_id': row[0], 'type': row[1], 'meme_description': row[2], 'meme_download_counts': row[3], 'meme_url': row[4], 'owner': row[5]} for row in cur.fetchall()]
                print(f"Debug - Memes fetched in admin: {memes}")
        return render_template('admin.html', authenticated=True, users=users, memes=memes, message=message, next_meme_id=next_meme_id)
    except psycopg.Error as e:
        print(f"Database error in admin: {str(e)}")
        message = "Error fetching user or meme data."
        return render_template('admin.html', authenticated=True, users=[], memes=[], message=message, next_meme_id=next_meme_id)

@memes_bp.route('/memes')
def memes():
    try:
        with psycopg.connect(DATABASE_URL) as conn:
            with conn.cursor() as cur:
                cur.execute('SELECT meme_id, meme_url, meme_description, meme_download_counts, type, owner FROM memes ORDER BY meme_id')
                memes = [{'meme_id': row[0], 'meme_url': row[1], 'meme_description': row[2], 'meme_download_counts': row[3], 'type': row[4], 'owner': row[5]} for row in cur.fetchall()]
                cur.execute('SELECT id, username FROM users')
                users = [{'id': row[0], 'username': row[1]} for row in cur.fetchall()]
                cur.execute('SELECT COUNT(*) FROM memes')
                meme_count = cur.fetchone()[0]
                verified_count = len(memes)
                if meme_count != verified_count:
                    print(f"Warning: Meme count mismatch - SQL COUNT: {meme_count}, Fetched rows: {verified_count}")
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
                print(f"Database error fetching user_type for memes: {str(e)}")

        print(f"Debug - Memes fetched: {memes}, users: {users}, meme_count: {meme_count}, total_downloads: {total_downloads}")
        return render_template('memes.html', memes=memes, users=users, meme_count=meme_count, total_downloads=total_downloads, username=username, user_type=user_type, points=points if user_type != 'Guest' else None, message=None)
    except psycopg.Error as e:
        print(f"Database error in memes: {str(e)}")
        return render_template('memes.html', memes=[], users=[], message="Error fetching meme data.", meme_count=0, total_downloads=0, username=None, user_type='Guest', points=0)
    except Exception as e:
        print(f"Unexpected error in memes: {str(e)}")
        return render_template('memes.html', memes=[], users=[], message="Error fetching meme data.", meme_count=0, total_downloads=0, username=None, user_type='Guest', points=0)

@memes_bp.route('/memes/register', methods=['POST'])
def memes_register():
    ip_address = request.remote_addr
    username = session.get('username')
    user_type = session.get('user_type', 'Guest')
    points = 0
    word_list = 'words.txt'

    if not username:
        username = generate_username(ip_address)
        session['username'] = username

    message = None
    if request.method == 'POST':
        new_username = request.form.get('register_username', '').strip()
        new_password = request.form.get('register_password', '')
        if new_username and new_password and all(c.isalnum() for c in new_username) and 1 <= len(new_username) <= 12:
            try:
                with psycopg.connect(DATABASE_URL) as conn:
                    with conn.cursor() as cur:
                        cur.execute('SELECT 1 FROM users WHERE username = %s', (new_username,))
                        if cur.fetchone():
                            message = "Username already taken."
                        else:
                            cur.execute('INSERT INTO users (ip_address, username, password, user_type, points, word_list) VALUES (%s, %s, %s, %s, %s, %s)', 
                                      (ip_address, new_username, hash_password(new_password), 'Member', 0, 'words.txt'))
                            cur.execute('INSERT INTO user_stats (user_id) VALUES (currval(\'users_id_seq\'))')
                            conn.commit()
                            session.clear()
                            session['username'] = new_username
                            session['user_type'] = 'Member'
                            session['word_list'] = 'words.txt'
                            user_type = 'Member'
                            points = 0
                            message = "Registration successful! You are now a Member."
                            print(f"Debug - Registration successful for {new_username}")
            except psycopg.Error as e:
                print(f"Database error during registration: {str(e)}")
                message = "Error during registration."
        else:
            message = "Username must be 1-12 alphanumeric characters."
            print(f"Debug - Invalid username: {new_username}")

    return jsonify({'message': message, 'success': message == "Registration successful! You are now a Member."})

@memes_bp.route('/add_point_and_redirect/<int:meme_id>/<path:url>')
def add_point_and_redirect(meme_id, url):
    try:
        with psycopg.connect(DATABASE_URL) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    'UPDATE memes SET meme_download_counts = meme_download_counts + 1 WHERE meme_id = %s',
                    (meme_id,)
                )
                conn.commit()
                print(f"Debug - Incremented download count for meme_id {meme_id} to {cur.rowcount}")
        return redirect(url, code=302)
    except psycopg.Error as e:
        print(f"Database error in add_point_and_redirect: {str(e)}")
        return "Error updating download count", 500
    except Exception as e:
        print(f"Unexpected error in add_point_and_redirect: {str(e)}")
        return "Error updating download count", 500

@memes_bp.route('/increment_download/<int:meme_id>', methods=['POST'])
def increment_download(meme_id):
    try:
        with psycopg.connect(DATABASE_URL) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    'UPDATE memes SET meme_download_counts = meme_download_counts + 1 WHERE meme_id = %s',
                    (meme_id,)
                )
                conn.commit()
                print(f"Debug - Incremented download count for meme_id {meme_id} to {cur.rowcount}")
        return jsonify({'success': True})
    except psycopg.Error as e:
        print(f"Database error in increment_download: {str(e)}")
        return jsonify({'success': False, 'error': str(e)}), 500
    except Exception as e:
        print(f"Unexpected error in increment_download: {str(e)}")
        return jsonify({'success': False, 'error': str(e)}), 500
