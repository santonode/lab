from flask import Flask
from wurdle import wurdle_bp
from memes import memes_bp, init_db
import os
import re

app = Flask(__name__, static_folder='static', template_folder='templates')
app.secret_key = os.getenv('SECRET_KEY', os.urandom(24))  # Use env var or generate secure key

# Register blueprints
app.register_blueprint(wurdle_bp)
app.register_blueprint(memes_bp)

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
port = int(os.getenv("PORT", 5000))  # Default to 5000 if PORT not set
if __name__ == '__main__':
    app.run(host='0.0.0.0', port=port)
