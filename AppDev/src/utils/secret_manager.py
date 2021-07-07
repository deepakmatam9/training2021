import traceback
from google.cloud import secretmanager

sm_client = secretmanager.SecretManagerServiceClient()

def get_secret(project_id, secret_id, version_id='latest'):
    try:
        name = f"projects/{project_id}/secrets/{secret_id}/versions/{version_id}"
        response = sm_client.access_secret_version(request={"name":name})
        #response = sm_client.access_secret_version(name)
        return response.payload.data.decode()
    except Exception as e:
        traceback.print_exc()
        return None
