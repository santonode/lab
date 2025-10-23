from flask import Flask, send_from_directory
from wurdle import wurdle_bp
from memes import memes_bp, init_db
import os
import re

# ✅ UPDATED: static2 for CSS + static for videos/thumbs
app = Flask(__name__, 
            static_folder='static2',      # ← CSS from static2
            static_url_path='/static2',    # ← /static2/styles.css
            template_folder='templates')
app.secret_key = os.getenv('SECRET_KEY', os.urandom(24))

# Register blueprints
app.register_blueprint(wurdle_bp)
app.register_blueprint(memes_bp)

# ✅ NEW: Serve videos/thumbs from original static/ (persistent disk)
@app.route('/static/<path:filename>')
def static_files(filename):
    """Serve videos/thumbs from persistent disk static/"""
    return send_from_directory('static', filename)

# Define the get_download_url function (updated to accept a URL string)
def get_download_url(url):
    if url and 'drive.google.com/file/d/' in url:
        match = re.search(r'https://drive.google.com/file/d/([^/]+)/view\?usp=drive_link', url)
        if match:
            file_id = match.group(1)
            return f"https://drive.google.com/uc?export=download&id={file_id}"
    return url

# Register the custom filter with the app's Jinja environment
app.jinja_env.filters['get_download_url'] = get_download_url

# Initialize database within app context with error handling
try:
    with app.app_context():
        init_db()
except Exception as e:
    print(f"Database initialization failed: {str(e)}")
    raise  # Re-raise to fail the app startup if init_db fails

# Configure port for Render
port = int(os.getenv("PORT", 5000))
if __name__ == '__main__':
    app.run(host='0.0.0.0', port=port)
