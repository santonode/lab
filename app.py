# app.py
from flask import Flask, send_from_directory, redirect, url_for
from flask_login import LoginManager, UserMixin
import os
from datetime import datetime

# === IMPORTS ===
from db import init_app
from erate import erate_bp
from memes import memes_bp

# === CREATE APP ===
app = Flask(__name__, static_folder='static', template_folder='templates')
app.secret_key = os.getenv('SECRET_KEY', os.urandom(24).hex())

# === LOGIN MANAGER — WORKS WITHOUT User MODEL ===
login_manager = LoginManager()
login_manager.init_app(app)
login_manager.login_view = 'erate.admin'

# Simple in-memory user for current_user (no DB needed)
class SimpleUser(UserMixin):
    def __init__(self, id, username):
        self.id = str(id)
        self.username = username

@login_manager.user_loader
def load_user(user_id):
    # Fake user — just so current_user works
    return SimpleUser(1, "king")

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

# === REGISTER BLUEPRINTS ===
app.register_blueprint(erate_bp, url_prefix='/erate')
app.register_blueprint(memes_bp, url_prefix='/memes')

# === STATIC ROUTES ===
@app.route('/static/thumbs/<path:filename>')
def serve_thumbs(filename):
    return send_from_directory('static/thumbs', filename)

@app.route('/static/vids/<path:filename>')
def serve_vids(filename):
    return send_from_directory('static/vids', filename)

@app.route('/static2/<path:filename>')
def static2_files(filename):
    response = send_from_directory('static2', filename)
    response.headers['Cache-Control'] = 'no-cache'
    return response

# === ROOT REDIRECT ===
@app.route('/')
def root():
    return redirect(url_for('erate.dashboard'))

# === INIT DB ===
init_app(app)
