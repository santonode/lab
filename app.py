# app.py
from flask import Flask, render_template, request, session, redirect, url_for, jsonify, send_file
import os
import logging
from datetime import datetime
from db import get_conn
from erate import erate_bp  # E-Rate SAFE

app = Flask(__name__)
app.secret_key = os.getenv('SECRET_KEY', 'dev-secret-key-change-in-prod')

# === REGISTER BLUEPRINTS ===
app.register_blueprint(erate_bp)

# === LOGGING ===
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)s: %(message)s',
    handlers=[
        logging.FileHandler("import.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# === STRFTIME FILTER (SAFE + DEFAULT FORMAT) ===
@app.template_filter('strftime')
def _jinja2_filter_strftime(date, fmt='%m/%d/%Y'):
    if date is None:
        return ''
    return date.strftime(fmt)

# === ROUTES ===
@app.route('/')
def index():
    username = session.get('username')
    user_type = session.get('user_type', 'Guest')

    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute('SELECT COUNT(*) FROM memes')
                meme_count = cur.fetchone()[0]

                cur.execute('SELECT COALESCE(SUM(meme_download_counts), 0) FROM memes')
                total_downloads = cur.fetchone()[0]

                cur.execute('''
                    SELECT m.meme_id, m.type, m.meme_description, m.meme_download_counts, m.owner,
                           u.username
                    FROM memes m
                    LEFT JOIN users u ON m.owner = u.id
                    ORDER BY m.meme_download_counts DESC
                ''')
                rows = cur.fetchall()

                memes = []
                users = []
                for row in rows:
                    meme = {
                        'meme_id': row[0],
                        'type': row[1] or '',
                        'meme_description': row[2] or '',
                        'meme_download_counts': row[3],
                        'owner': row[4]
                    }
                    memes.append(meme)
                    if row[5]:
                        users.append({'id': row[4], 'username': row[5]})

    except Exception as e:
        logger.error(f"DB Error on /: {e}")
        meme_count = total_downloads = 0
        memes = []
        users = []

    return render_template(
        'index.html',
        username=username,
        user_type=user_type,
        meme_count=meme_count,
        total_downloads=total_downloads,
        memes=memes,
        users=users
    )

# === REGISTER ===
@app.route('/memes/register', methods=['POST'])
def register():
    username = request.form['register_username'].strip()
    password = request.form['register_password']

    if not (3 <= len(username) <= 12 and username.isalnum()):
        return jsonify({'success': False, 'message': 'Username: 3–12 chars, letters/numbers only'})

    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute('SELECT id FROM users WHERE username = %s', (username,))
                if cur.fetchone():
                    return jsonify({'success': False, 'message': 'Username taken'})

                cur.execute(
                    'INSERT INTO users (username, password_hash) VALUES (%s, %s) RETURNING id',
                    (username, password)
                )
                user_id = cur.fetchone()[0]
                conn.commit()

                session['username'] = username
                session['user_id'] = user_id
                session['user_type'] = 'User'
                return jsonify({'success': True})
    except Exception as e:
        logger.error(f"Register error: {e}")
        return jsonify({'success': False, 'message': 'Server error'})

# === LOGIN ===
@app.route('/memes/login', methods=['POST'])
def login():
    username = request.form['login_username'].strip()
    password = request.form['login_password']

    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute('SELECT id, password_hash FROM users WHERE username = %s', (username,))
                user = cur.fetchone()
                if user and user[1] == password:
                    session['username'] = username
                    session['user_id'] = user[0]
                    session['user_type'] = 'User'
                    return jsonify({'success': True})
                else:
                    return jsonify({'success': False, 'message': 'Invalid credentials'})
    except Exception as e:
        logger.error(f"Login error: {e}")
        return jsonify({'success': False, 'message': 'Server error'})

# === INCREMENT DOWNLOAD ===
@app.route('/increment_download/<int:meme_id>', methods=['POST'])
def increment_download(meme_id):
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    'UPDATE memes SET meme_download_counts = meme_download_counts + 1 WHERE meme_id = %s',
                    (meme_id,)
                )
                conn.commit()
        return jsonify({'success': True})
    except Exception as e:
        logger.error(f"Download increment failed: {e}")
        return jsonify({'success': False})

# === LOG VIEWER ===
@app.route('/import-log')
def view_log():
    try:
        return send_file('import.log', mimetype='text/plain')
    except FileNotFoundError:
        return "No log yet.", 404

# === PLACEHOLDERS ===
@app.route('/profile')
def profile():
    return render_template('profile.html')

@app.route('/admin')
def admin():
    return render_template('admin.html')

# === RUN ===
if __name__ == '__main__':
    app.run(debug=True)
