# app.py
from flask import Flask
import os
import re

# === IMPORT ALL BLUEPRINTS ===
from wurdle import wurdle_bp
from memes import memes_bp, init_db
from erate import erate_bp  # ← E-RATE ADDED

# === CREATE APP ===
app = Flask(__name__, static_folder='static', template_folder='templates')
app.secret_key = os.getenv('SECRET_KEY', os.urandom(24))

# === REGISTER ALL BLUEPRINTS ===
app.register_blueprint(wurdle_bp)
app.register_blueprint(memes_bp)
app.register_blueprint(erate_bp)  # ← E-RATE ROUTES

# === CUSTOM JINJA FILTER: Google Drive Direct Download ===
def get_download_url(url):
    if url and 'drive.google.com/file/d/' in url:
        match = re.search(r'https://drive.google.com/file/d/([^/]+)/view\?usp=drive_link', url)
        if match:
            file_id = match.group(1)
            return f"https://drive.google.com/uc?export=download&id={file_id}"
    return url

app.jinja_env.filters['get_download_url'] = get_download_url

# === DATABASE INIT (WITH ERROR HANDLING) ===
try:
    with app.app_context():
        init_db()
        print("Database initialized successfully.")
except Exception as e:
    print(f"Database initialization failed: {str(e)}")
    raise

# === PORT CONFIG FOR RENDER ===
port = int(os.getenv("PORT", 5000))

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=port, debug=True)
