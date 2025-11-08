# app.py
# === PATCH FIRST — BEFORE ANYTHING ===
import patch_psycopg2  # ← MUST BE FIRST

from flask import Flask
import os
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
    DATABASE_URL = DATABASE_URL.replace("postgres://", "postgresql+psycopg://", 1)

app.config['SQLALCHEMY_DATABASE_URI'] = DATABASE_URL
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
app.config['SQLALCHEMY_ENGINE_OPTIONS'] = {
    'connect_args': {'sslmode': 'require'}
}

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
