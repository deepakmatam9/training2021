import traceback
from google.cloud import storage

storage_client = storage.Client()

def upload_blob_as_string(bucket_name, content, destination_blob_name):
    try:
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(destination_blob_name)
        blob.upload_from_string(content)
        return True
    except Exception as e:
        traceback.print_exc()
        return False



