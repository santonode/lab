# app.py
from flask import Flask
import os
from models import Base
from extensions import engine
from erate import erate_bp
from memes import memes_bp, init_db   # keep memes if you still need it

app = Flask(__name__, static_folder='static', template_folder='templates')
app.secret_key = os.getenv('SECRET_KEY', os.urandom(24))

app.register_blueprint(erate_bp)
app.register_blueprint(memes_bp)

# Create tables on first start
with app.app_context():
    Base.metadata.create_all(bind=engine)
    init_db()          # creates any extra tables used by memes

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=int(os.getenv("PORT", 5000)), debug=True)
