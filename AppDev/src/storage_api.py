import os
from flask import Blueprint, request, abort, jsonify
import json
from flask_cors.extension import CORS

from google.cloud.storage import bucket

from .utils.Constants import Constants
from .utils.storage_utils import upload_blob_as_string

storage_api = Blueprint('storage_api', __name__)

@storage_api.route('/upload-file', methods=['POST'])
def uploadFile():
    print(request)
    req_files = request.files
    file = [[req_files[file].filename, req_files[file].read()] for file in req_files][0]
    filename = file[0]
    content= file[1]
    print(filename, content)
    bucket_name = Constants.FILE_UPLOAD_BUCKET
    blob_path = Constants.FILE_OBJECT_PATH
    destination_blob_name = blob_path + '/' + filename
    is_file_uploaded = upload_blob_as_string(bucket_name, content, destination_blob_name)
    if is_file_uploaded:
        return jsonify('{"status":"Uploaded"}')
    else:
        return jsonify('{"status":"Upload failed"}')

    
