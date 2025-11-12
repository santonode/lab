# app.py
from flask import Flask, send_from_directory, session, request, redirect, url_for, flash
import os
from datetime import datetime
from db import init_db, init_app as init_db_app
from erate import erate_bp
from memes import memes_bp

# === CREATE APP ===
app = Flask(__name__, static_folder='static', template_folder='templates')

# === SECRET KEY (REQUIRED FOR SESSION) ===
app.secret_key = os.getenv('SECRET_KEY', os.urandom(24))

# === DATABASE URL ===
app.config['DATABASE_URL'] = os.getenv(
    'DATABASE_URL',
    'postgresql://wurdle_db_user:your_password@dpg-d2qcuan5r7bs73aid7p0-a/wurdle_db'
)

# === JINJA FILTERS — SAFE strftime (handles str → datetime) ===
def strftime_filter(value, format="%m/%d/%Y %H:%M"):
    """Format datetime for Jinja templates — safely handles string input"""
    if value is None:
        return ""
    if isinstance(value, str):
        try:
            # Handle ISO format: "2025-03-15T14:22:10.123456+00:00" or with Z
            cleaned = value.replace('Z', '+00:00')
            value = datetime.fromisoformat(cleaned)
        except ValueError:
            return value  # Return original string if parsing fails
    return value.strftime(format)

app.jinja_env.filters['strftime'] = strftime_filter

# === CACHE BUSTER ===
@app.context_processor
def inject_cache_buster():
    return dict(cache_buster=int(datetime.now().timestamp()))

# === REGISTER BLUEPRINTS WITH PREFIXES ===
app.register_blueprint(erate_bp, url_prefix='/erate')
app.register_blueprint(memes_bp, url_prefix='/memes')

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

# === ADMIN LOGOUT HANDLER (SHARED) ===
@app.route('/admin')
def admin_logout():
    if request.args.get('logout'):
        session.pop('admin_authenticated', None)
        session.pop('username', None)
        session.pop('user_id', None)
        flash("Logged out successfully.", "info")
    return redirect(url_for('erate.admin'))

# === INIT DB ON START ===
init_db_app(app)  # Calls: init_db() + teardown

# === GUNICORN HANDLES $PORT — NO app.run() ===
# DO NOT run Flask dev server in production
