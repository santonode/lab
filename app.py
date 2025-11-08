# app.py
from flask import Flask
import os
import re
from models import Base
from extensions import engine
from memes import memes_bp, init_db
from erate import erate_bp

app = Flask(__name__, static_folder='static', template_folder='templates')
app.secret_key = os.getenv('SECRET_KEY', os.urandom(24))

# === REGISTER BLUEPRINTS ===
app.register_blueprint(memes_bp)
app.register_blueprint(erate_bp)

# === CREATE TABLES ===
with app.app_context():
    Base.metadata.create_all(bind=engine)
    init_db()

# === CUSTOM FILTER ===
def get_download_url(url):
    if url and 'drive.google.com/file/d/' in url:
        match = re.search(r'https://drive.google.com/file/d/([^/]+)/view\?usp=drive_link', url)
        if match:
            file_id = match.group(1)
            return f"https://drive.google.com/uc?export=download&id={file_id}"
    return url

app.jinja_env.filters['get_download_url'] = get_download_url

port = int(os.getenv("PORT", 5000))
if __name__ == '__main__':
    app.run(host='0.0.0.0', port=port, debug=True)
