# app.py
from flask import Flask, send_from_directory, redirect, url_for
from flask_login import LoginManager
import os
from datetime import datetime

# === IMPORTS ===
from db import init_app
from erate import erate_bp
from memes import memes_bp
from models import User  # ← CRITICAL — your User model

# === CREATE APP ===
app = Flask(__name__, static_folder='static', template_folder='templates')
app.secret_key = os.getenv('SECRET_KEY', os.urandom(24).hex())

# === LOGIN MANAGER — FULLY WORKING ===
login_manager = LoginManager()
login_manager.init_app(app)
login_manager.login_view = 'erate.admin'  # your login route

@login_manager.user_loader
def load_user(user_id):
    try:
        return User.query.get(int(user_id))
    except:
        return None

@login_manager.unauthorized_handler
def unauthorized():
    return redirect(url_for('erate.admin'))

# === DATABASE URL ===
app.config['DATABASE_URL'] = os.getenv(
    'DATABASE_URL',
    'postgresql://wurdle_db_user:your_password@dpg-d2qcuan5r7bs73aid7p0-a/wurdle_db'
)

# === JINJA FILTERS ===
def strftime_filter(value, format="%m/%d/%Y %H:%M"):
    if value is None:
        return ""
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

# === ROOT REDIRECT → E-RATE DASHBOARD ===
@app.route('/')
def root():
    return redirect(url_for('erate.dashboard'))

# === INIT DB ON START ===
init_app(app)

# === GUNICORN HANDLES $PORT — NO app.run() ===
