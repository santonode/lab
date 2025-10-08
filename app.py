from flask import Flask
from wurdle import wurdle_bp
from memes import memes_bp, get_download_url
import os

# Initialize Flask app with static folder
app = Flask(__name__, static_folder='static')
app.secret_key = os.urandom(24)

# Register blueprints
app.register_blueprint(wurdle_bp)
app.register_blueprint(memes_bp, url_prefix='/memes')

# Register the custom filter with the app's Jinja environment
app.jinja_env.filters['get_download_url'] = get_download_url

# Configure port for Render
port = int(os.getenv("PORT", 5000))  # Default to 5000 if PORT not set
if __name__ == '__main__':
    app.run(host='0.0.0.0', port=port, debug=True)
