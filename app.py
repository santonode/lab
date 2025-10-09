from flask import Flask
from wurdle import wurdle_bp
from memes import memes_bp, init_db
import os

app = Flask(__name__, static_folder='static', template_folder='templates')
app.secret_key = os.urandom(24)

# Register blueprints
app.register_blueprint(wurdle_bp)
app.register_blueprint(memes_bp)

# Define the get_download_url function (moved here to avoid import confusion)
def get_download_url(meme):
    meme_url = meme.get('meme_url', '')
    if 'drive.google.com' in meme_url:
        import re
        match = re.search(r'/d/([a-zA-Z0-9-_]+)', meme_url) or re.search(r'id=([a-zA-Z0-9-_]+)', meme_url)
        if match:
            asset_id = match.group(1)
            return f"https://drive.google.com/uc?export=download&id={asset_id}"
    return meme_url

# Register the custom filter with the app's Jinja environment
app.jinja_env.filters['get_download_url'] = get_download_url

# Initialize database within app context
with app.app_context():
    init_db()

# Configure port for Render
port = int(os.getenv("PORT", 5000))  # Default to 5000 if PORT not set
if __name__ == '__main__':
    app.run(host='0.0.0.0', port=port)
