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

# === PRIVATE TEST LAB — FINAL DEBUG VERSION (100% WORKING) ===
@app.route('/erate_test_lab_2025')
def erate_test_lab():
    import os
    from flask import render_template_string
    
    # Show exactly where Flask is looking
    template_dir = app.template_folder or "NOT SET"
    template_path = os.path.join(template_dir, 'erate_test.html')
    
    if not os.path.exists(template_path):
        files_list = "NO FILES — TEMPLATES FOLDER MISSING"
        if os.path.exists(template_dir):
            try:
                files_list = "<br>".join(os.listdir(template_dir))
            except:
                files_list = "ERROR READING FOLDER"
        return f"""
        <h1>TEMPLATE NOT FOUND</h1>
        <hr>
        <p><strong>Expected file:</strong> <code>{template_path}</code></p>
        <p><strong>Template folder:</strong> <code>{template_dir}</code></p>
        <p><strong>Files in folder:</strong></p>
        <pre>{files_list}</pre>
        <hr>
        <p>Make sure:</p>
        <ul>
            <li>File is named: <code>erate_test.html</code></li>
            <li>File is in <code>templates/</code> folder</li>
            <li>Folder is named exactly <code>templates</code></li>
        </ul>
        """, 500

    # File exists — render it
    with open(template_path, 'r', encoding='utf-8') as f:
        content = f.read()
    
    return render_template_string(content)

@app.route('/debug_test')
def debug_test():
    return "DEBUG: Flask is alive! Routes are working. Template is erate_test.html"

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
