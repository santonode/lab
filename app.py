# app.py
import os
from flask import Flask
from extensions import db
from erate import erate_bp

def create_app():
    app = Flask(__name__)

    # === CRITICAL: SECRET KEY FOR SESSIONS ===
    app.secret_key = os.getenv('SECRET_KEY', 'dev-secret-change-in-production')

    # === DATABASE ===
    app.config['SQLALCHEMY_DATABASE_URI'] = os.getenv('DATABASE_URL')
    app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False

    # === INIT DB ===
    db.init_app(app)
    with app.app_context():
        db.create_all()
        print("Database initialized successfully.")
        print("DB URL:", app.config['SQLALCHEMY_DATABASE_URI'])

    # === REGISTER BLUEPRINT ===
    app.register_blueprint(erate_bp)

    return app

# === CREATE APP ===
app = create_app()

# === RUN LOCALLY ===
if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)
