# memes.py
from flask import Blueprint, render_template, request, flash, redirect, url_for, session, jsonify
import os
import hashlib
from werkzeug.utils import secure_filename
from psycopg import connect  # ← psycopg v3 (from psycopg[binary])
from datetime import datetime
from extensions import db

memes_bp = Blueprint('memes', __name__, url_prefix='/memes')

DATABASE_URL = os.environ.get('DATABASE_URL')
if not DATABASE_URL:
    raise ValueError("DATABASE_URL environment variable must be set")

ALLOWED_EXTENSIONS = {'png', 'jpg', 'jpeg', 'gif'}

def allowed_file(filename):
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS

def init_db():
    try:
        with connect(DATABASE_URL) as conn:
            with conn.cursor() as cur:
                cur.execute('''
                    CREATE TABLE IF NOT EXISTS memes (
                        id SERIAL PRIMARY KEY,
                        filename TEXT NOT NULL,
                        title TEXT NOT NULL,
                        upload_time TIMESTAMP NOT NULL,
                        uploader_ip TEXT NOT NULL,
                        uploader_username TEXT NOT NULL,
                        likes INTEGER DEFAULT 0,
                        dislikes INTEGER DEFAULT 0
                    )
                ''')
                cur.execute('''
                    CREATE TABLE IF NOT EXISTS votes (
                        id SERIAL PRIMARY KEY,
                        meme_id INTEGER NOT NULL,
                        voter_ip TEXT NOT NULL,
                        vote_type TEXT NOT NULL,
                        FOREIGN KEY (meme_id) REFERENCES memes(id) ON DELETE CASCADE
                    )
                ''')
                conn.commit()
        print("Memes database initialized.")
    except Exception as e:
        print(f"Memes DB init error: {str(e)}")
        raise

@memes_bp.route('/', methods=['GET', 'POST'])
def memes():
    if request.method == 'POST':
        if 'file' not in request.files:
            flash('No file part')
            return redirect(request.url)
        file = request.files['file']
        title = request.form.get('title', '').strip()
        ip_address = request.remote_addr
        uploader_username = session.get('username', 'Guest') or 'Guest'

        if file.filename == '':
            flash('No selected file')
            return redirect(request.url)
        if title == '':
            flash('Title is required')
            return redirect(request.url)
        if not file or not allowed_file(file.filename):
            flash('Invalid file type.')
            return redirect(request.url)

        filename = secure_filename(file.filename)
        file_path = os.path.join('static', filename)
        file.save(file_path)

        try:
            with connect(DATABASE_URL) as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        'INSERT INTO memes (filename, title, upload_time, uploader_ip, uploader_username) VALUES (%s, %s, %s, %s, %s)',
                        (filename, title, datetime.now(), ip_address, uploader_username)
                    )
                    conn.commit()
            flash('Meme uploaded successfully!')
        except Exception as e:
            print(f"Database error uploading meme: {str(e)}")
            flash('Error uploading meme.')

        return redirect(url_for('memes.memes'))

    # GET: Show all memes
    try:
        with connect(DATABASE_URL) as conn:
            with conn.cursor() as cur:
                cur.execute('SELECT id, filename, title, upload_time, uploader_username, likes, dislikes FROM memes ORDER BY upload_time DESC')
                memes = [
                    {
                        'id': row[0],
                        'filename': row[1],
                        'title': row[2],
                        'upload_time': row[3],
                        'uploader': row[4],
                        'likes': row[5],
                        'dislikes': row[6]
                    }
                    for row in cur.fetchall()
                ]
        return render_template('memes.html', memes=memes)
    except Exception as e:
        print(f"Database error fetching memes: {str(e)}")
        return render_template('memes.html', memes=[])

@memes_bp.route('/vote', methods=['POST'])
def vote():
    meme_id = request.form.get('meme_id')
    vote_type = request.form.get('vote_type')
    ip_address = request.remote_addr

    if not meme_id or vote_type not in ['like', 'dislike']:
        return jsonify({'success': False, 'message': 'Invalid request.'})

    try:
        with connect(DATABASE_URL) as conn:
            with conn.cursor() as cur:
                # Check if already voted
                cur.execute('SELECT 1 FROM votes WHERE meme_id = %s AND voter_ip = %s', (meme_id, ip_address))
                if cur.fetchone():
                    return jsonify({'success': False, 'message': 'You have already voted on this meme.'})

                # Record vote
                cur.execute('INSERT INTO votes (meme_id, voter_ip, vote_type) VALUES (%s, %s, %s)', (meme_id, ip_address, vote_type))

                # Update likes/dislikes
                if vote_type == 'like':
                    cur.execute('UPDATE memes SET likes = likes + 1 WHERE id = %s', (meme_id,))
                else:
                    cur.execute('UPDATE memes SET dislikes = dislikes + 1 WHERE id = %s', (meme_id,))

                conn.commit()
        return jsonify({'success': True})
    except Exception as e:
        print(f"Database error in vote: {str(e)}")
        return jsonify({'success': False, 'message': 'Database error.'})
