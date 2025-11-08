# app.py
from flask import Flask
import os
from datetime import datetime
from db import init_db
from erate import erate_bp
from memes import memes_bp

# === CREATE APP ===
app = Flask(__name__, static_folder='static', template_folder='templates')
app.secret_key = os.getenv('SECRET_KEY', os.urandom(24))

# === JINJA FILTERS ===
def strftime_filter(value, format="%m/%d/%Y %H:%M"):
    """Format datetime for Jinja templates"""
    if value is None:
        return ""
    return value.strftime(format)

app.jinja_env.filters['strftime'] = strftime_filter

# === REGISTER BLUEPRINTS ===
app.register_blueprint(erate_bp)
app.register_blueprint(memes_bp)

# === INIT DB ON START ===
with app.app_context():
    init_db()

# === RUN ===
if __name__ == '__main__':
    port = int(os.getenv("PORT", 5000))
    app.run(host='0.0.0.0', port=port, debug=True)
