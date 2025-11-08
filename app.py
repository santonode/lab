# app.py
from flask import Flask
import os
from models import Base
from extensions import engine
from erate import erate_bp
from memes import memes_bp, init_db

app = Flask(__name__, static_folder='static', template_folder='templates')
app.secret_key = os.urandom(24)

app.register_blueprint(erate_bp)
app.register_blueprint(memes_bp)

# Create tables
with app.app_context():
    Base.metadata.create_all(bind=engine)
    init_db()

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=int(os.getenv("PORT", 5000)), debug=True)
