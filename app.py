# app.py — TOP OF FILE
import os
import ctypes
from ctypes.util import find_library

# === FIX: undefined symbol: _PyInterpreterState_Get ===
# This is a known issue with psycopg2-binary on Python 3.13
# Load libpython3.13.so to resolve missing symbols
libpython = find_library("python3.13")
if libpython:
    ctypes.CDLL(libpython, mode=ctypes.RTLD_GLOBAL)
# === END FIX ===

from flask import Flask
import re

# === IMPORT BLUEPRINTS ===
# from wurdle import wurdle_bp  # ← DISABLED
from memes import memes_bp, init_db
from erate import erate_bp

# === IMPORT EXTENSIONS ===
from extensions import db

# === CREATE APP ===
app = Flask(__name__, static_folder='static', template_folder='templates')
app.secret_key = os.getenv('SECRET_KEY', os.urandom(24))

# === CONFIG ===
DATABASE_URL = os.getenv('DATABASE_URL')
if DATABASE_URL and DATABASE_URL.startswith("postgres://"):
    DATABASE_URL = DATABASE_URL.replace("postgres://", "postgresql://", 1)

app.config['SQLALCHEMY_DATABASE_URI'] = DATABASE_URL
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False

# === INIT EXTENSIONS ===
db.init_app(app)

# === REGISTER BLUEPRINTS ===
# app.register_blueprint(wurdle_bp)  # ← DISABLED
app.register_blueprint(memes_bp)
app.register_blueprint(erate_bp)

# === CUSTOM FILTER ===
def get_download_url(url):
    if url and 'drive.google.com/file/d/' in url:
        match = re.search(r'https://drive.google.com/file/d/([^/]+)/view\?usp=drive_link', url)
        if match:
            file_id = match.group(1)
            return f"https://drive.google.com/uc?export=download&id={file_id}"
    return url

app.jinja_env.filters['get_download_url'] = get_download_url

# === DATABASE INIT ===
try:
    with app.app_context():
        init_db()
        print("Database initialized.")
except Exception as e:
    print(f"DB init failed: {e}")
    raise

# === PORT ===
port = int(os.getenv("PORT", 5000))

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=port, debug=True)
