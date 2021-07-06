import firebase_admin
from flask import Blueprint, request, abort, jsonify
from firebase_admin import firestore, auth, credentials
import json

from .utils.secret_manager import get_secret
from .utils.Constants import Constants

firebase_secret = get_secret(Constants.PROJECT_ID, Constants.FIREBASE_SECRET_ID)
cred = credentials.Certificate(json.loads(firebase_secret))
firebase_app = firebase_admin.initialize_app(cred)

db = firestore.client()

firestore_api = Blueprint('firestore_api', __name__)

@firestore_api.route('/list-all-files', methods=['GET'])
def get_all_files():
    docs = db.collection(u'json_documents').stream()
    response = []
    for doc in docs:
        response.append({"filename":doc.id,"file_content":doc.to_dict()})
        #response.append({doc.id: doc.to_dict()})
    return jsonify(response)


    
    
