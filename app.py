# app.py
from flask import Flask
from extensions import db
from erate import erate_bp

def create_app():
    app = Flask(__name__)
    app.config['SQLALCHEMY_DATABASE_URI'] = 'your-postgres-url'
    app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False

    db.init_app(app)

    with app.app_context():
        db.create_all()  # Create tables if not exist
        print("Database initialized successfully.")

    app.register_blueprint(erate_bp)

    return app

app = create_app()

if __name__ == '__main__':
    app.run()
