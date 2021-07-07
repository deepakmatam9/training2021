from flask import Flask
from flask_cors import CORS

from src.firestore_api import firestore_api
from src.storage_api import storage_api

app = Flask(__name__,static_folder='angular/dist/angular')
app.register_blueprint(firestore_api,url_prefix='/api')
app.register_blueprint(storage_api,url_prefix='/api')
angular = Blueprint('angular',__name__,template_folder='angular/dist/angular')
app.register_blueprint(angular)
CORS(app)

@app.route('/assets/<path:filename>')
def custom_static_for_assets(filename):
    return send_from_directory('angular/dist/angular/assets',filename)

@app.route('/<path:filename>')
def custom_static(filename):
    return send_from_directory('angular/dist/angular/', filename)

@app.route('/')
def index():
    return render_template('index.html')

if __name__ == "__main__":
    #app.run('127.0.0.1',port='8080',debug=True)
    app.run(host='0.0.0.0', port='8080',debug=True)

