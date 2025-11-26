# app.py
from flask import Flask, send_from_directory, redirect, url_for, render_template
import os
from datetime import datetime

# === IMPORTS ===
from db import init_app  # ← CORRECT: init_app (not init_db_app)
from erate import erate_bp
from memes import memes_bp

# === CREATE APP ===
app = Flask(__name__, static_folder='static', template_folder='templates')
app.secret_key = os.getenv('SECRET_KEY', os.urandom(24).hex())

# === DATABASE URL ===
app.config['DATABASE_URL'] = os.getenv(
    'DATABASE_URL',
    'postgresql://wurdle_db_user:your_password@dpg-d2qcuan5r7bs73aid7p0-a/wurdle_db'
)

# === JINJA FILTERS ===
def strftime_filter(value, format="%m/%d/%Y %H:%M"):
    """Format datetime for Jinja templates"""
    if value is None:
        return ""
    return value.strftime(format)

app.jinja_env.filters['strftime'] = strftime_filter

# === CACHE BUSTER ===
@app.context_processor
def inject_cache_buster():
    return dict(cache_buster=int(datetime.now().timestamp()))

# === REGISTER BLUEPRINTS WITH PREFIXES ===
app.register_blueprint(erate_bp, url_prefix='/erate')  # /erate/, /erate/import-interactive
app.register_blueprint(memes_bp, url_prefix='/memes')   # /memes, /memes/register, etc.

from flask import render_template  # ← ADD THIS AT THE TOP OF app.py

@app.route('/erate_test_lab_2025')
def erate_test_lab():
    # Hard-coded minimal context — no imports, no risk
    fake_context = {
        'session': {'ft': 100, 'username': 'king'},
        'total_filtered': 33,
        'total': 302208,
        'cache_buster': 123456789,
        # Fake filters so Jinja doesn't explode
        'filters': lambda x: x,
        'strftime': lambda dt, fmt='%m/%d/%Y': dt.strftime(fmt) if dt else '',
        # url_for stub
        'url_for': lambda *args, **kwargs: '#'
    }
    
    try:
        with open('templates/erate_test.html', 'r', encoding='utf-8') as f:
            html = f.read()
        from flask import render_template_string
        return render_template_string(html, **fake_context)
    except Exception as e:
        return f"<pre>DEBUG ERROR:\n{str(e)}\n\nSTACK:\n{__import__('traceback').format_exc()}</pre>", 500

# === SERVE /static/thumbs/ AND /static/vids/ ===
@app.route('/static/thumbs/<path:filename>')
def serve_thumbs(filename):
    return send_from_directory('static/thumbs', filename)

@app.route('/static/vids/<path:filename>')
def serve_vids(filename):
    return send_from_directory('static/vids', filename)

# === SERVE static2/ (gear-icon, styles) ===
@app.route('/static2/<path:filename>')
def static2_files(filename):
    response = send_from_directory('static2', filename)
    response.headers['Cache-Control'] = 'no-cache'
    return response

# === ROOT REDIRECT → E-RATE DASHBOARD ===
@app.route('/')
def root():
    return redirect(url_for('erate.dashboard'))

# === INIT DB ON START ===
init_app(app)  # ← CORRECT: calls init_db() + teardown

# === GUNICORN HANDLES $PORT — NO app.run() ===
# DO NOT run Flask dev server in production
