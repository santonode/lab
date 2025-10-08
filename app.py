from flask import Flask
from wurdle import wurdle_bp
from memes import memes_bp, get_download_url
import os

app = Flask(__name__, static_folder='static', template_folder='templates')
app.secret_key = os.urandom(24)

app.register_blueprint(wurdle_bp)
app.register_blueprint(memes_bp)

app.jinja_env.filters['get_download_url'] = get_download_url

port = int(os.getenv("PORT", 5000))
if __name__ == '__main__':
    app.run(host='0.0.0.0', port=port, debug=True)
