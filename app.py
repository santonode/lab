# app.py
from flask import Flask, send_from_directory, redirect, url_for
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

@app.route('/erate_test_lab_2025')
def erate_test_lab():
    import os
    template_path = os.path.join(app.root_path, 'templates', 'erate_test.html')
    if not os.path.exists(template_path):
        return f"""
        <h1>TEMPLATE NOT FOUND</h1>
        <p>Expected path:</p>
        <code>{template_path}</code>
        <p>Check your project structure on Render.</p>
        """, 500
    try:
        return render_template('erate_test.html')
    except Exception as e:
        return f"""
        <h1>TEMPLATE ERROR</h1>
        <p>{str(e)}</p>
        <p>Template exists but failed to render.</p>
        """, 500

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
