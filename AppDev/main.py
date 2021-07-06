from flask import Flask
from flask_cors import CORS

from src.firestore_api import firestore_api
from src.storage_api import storage_api

app = Flask(__name__)
app.register_blueprint(firestore_api,url_prefix='/api')
app.register_blueprint(storage_api,url_prefix='/api')
CORS(app)

if __name__ == "__main__":
    app.run('127.0.0.1',port='5075',debug=True)

