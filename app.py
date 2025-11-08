# app.py
from flask import Flask
import os  # ← ADD THIS LINE
from db import init_db
from erate import erate_bp
from memes import memes_bp

app = Flask(__name__, static_folder='static', template_folder='templates')
app.secret_key = os.urandom(24)

app.register_blueprint(erate_bp)
app.register_blueprint(memes_bp)

with app.app_context():
    init_db()

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=int(os.getenv("PORT", 5000)))
