from flask import Flask
from wurdle import wurdle_bp
from memes import memes_bp

app = Flask(__name__)
app.secret_key = os.urandom(24)
app.register_blueprint(wurdle_bp)
app.register_blueprint(memes_bp, url_prefix='/memes')

if __name__ == '__main__':
    app.run(debug=True)
