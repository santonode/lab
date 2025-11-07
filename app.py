# app.py
import os
from flask import Flask
from extensions import db
from erate import erate_bp

def create_app():
    app = Flask(__name__)
    
    # USE RENDER'S DATABASE_URL
    app.config['SQLALCHEMY_DATABASE_URI'] = os.getenv('DATABASE_URL')
    app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False

    # INIT DB
    db.init_app(app)

    # CREATE TABLES (only if not exist)
    with app.app_context():
        db.create_all()
        print("Database initialized successfully with URL:", app.config['SQLALCHEMY_DATABASE_URI'])

    # REGISTER BLUEPRINT
    app.register_blueprint(erate_bp)

    return app

app = create_app()

if __name__ == '__main__':
    app.run()
