from flask import Flask
from wurdle import wurdle_bp
from memes import memes_bp, get_download_url
import os

app = Flask(__name__)
app.secret_key = os.urandom(24)
app.register_blueprint(wurdle_bp)
app.register_blueprint(memes_bp, url_prefix='/memes')

# Register the custom filter with the app's Jinja environment
app.jinja_env.filters['get_download_url'] = get_download_url

if __name__ == '__main__':
    app.run(debug=True)
