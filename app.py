# app.py
from flask import Flask, send_from_directory
import os
import re

# === Blueprints ===
from wurdle import wurdle_bp
from memes import memes_bp, init_db
from erate import erate_bp  # <-- NEW: E-Rate dashboard

# === App Setup ===
def create_app():
    app = Flask(
        __name__,
        static_folder='static2',           # CSS, JS, icons
        static_url_path='/static2',
        template_folder='templates'
    )

    # === Security ===
    app.secret_key = os.getenv('SECRET_KEY', os.urandom(24))

    # === Custom Jinja Filter ===
    def get_download_url(url):
        if url and 'drive.google.com/file/d/' in url:
            match = re.search(r'https://drive.google.com/file/d/([^/]+)/view\?usp=drive_link', url)
            if match:
                file_id = match.group(1)
                return f"https://drive.google.com/uc?export=download&id={file_id}"
        return url

    app.jinja_env.filters['get_download_url'] = get_download_url

    # === Static File Routes ===
    @app.route('/static/<path:filename>')
    def static_files(filename):
        """Serve videos, thumbs from persistent disk (static/)"""
        return send_from_directory('static', filename)

    # === Register Blueprints ===
    app.register_blueprint(wurdle_bp)
    app.register_blueprint(memes_bp)
    app.register_blueprint(erate_bp, url_prefix='/erate')  # <-- E-Rate at /erate

    # === Database Init (with context) ===
    try:
        with app.app_context():
            init_db()
            print("Database initialized successfully.")
    except Exception as e:
        print(f"Database initialization failed: {str(e)}")
        raise

    # === Health Check Route (Optional) ===
    @app.route('/health')
    def health():
        return {"status": "healthy", "service": "wurdle"}, 200

    return app

# === Create App Instance ===
app = create_app()

# === Run for Render / Local ===
if __name__ == '__main__':
    port = int(os.getenv("PORT", 5000))
    app.run(host='0.0.0.0', port=port, debug=False)
