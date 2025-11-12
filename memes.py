# memes.py — FINAL: READ-ONLY + DOWNLOAD COUNT (real memes table)
from flask import Blueprint, render_template, jsonify
from db import get_conn

memes_bp = Blueprint('memes', __name__, url_prefix='/memes')

@memes_bp.route('/')
def index():
    """Display all memes from real DB table"""
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute('''
                    SELECT meme_id, meme_url, meme_description, meme_download_counts, 
                           type, owner, thumbnail_url
                    FROM memes 
                    ORDER BY meme_id DESC
                ''')
                rows = cur.fetchall()
                memes = [
                    {
                        'meme_id': r[0],
                        'meme_url': r[1],
                        'meme_description': r[2],
                        'meme_download_counts': r[3],
                        'type': r[4],
                        'owner': r[5],
                        'thumbnail_url': r[6]
                    }
                    for r in rows
                ]
        return render_template('memes.html', memes=memes)
    except Exception as e:
        print(f"Error loading memes: {e}")
        return render_template('memes.html', memes=[])

@memes_bp.route('/increment_download/<int:meme_id>', methods=['POST'])
def increment_download(meme_id):
    """Increment download count"""
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
        print(f"Download count error: {e}")
        return jsonify({'success': False})
