import base64
import os
import io
import imageio
import satpy

from google.cloud import storage

def process(event, context):
    """Triggered from a message on a Cloud Pub/Sub topic.
    Args:
         event (dict): Event payload.
         context (google.cloud.functions.Context): Metadata for the event.
    """
    
    name = base64.b64decode(event['data']).decode('utf-8')
    
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(os.environ['BUCKET_NAME'])
    
    blob = bucket.blob(name)
    data = blob.download_as_string()
    f = io.BytesIO(data)
    f.seek(0)
    
    fname = name.split('/')[1]

    with open('/tmp/{}'.format(fname), 'wb') as mf:
        mf.write(f.read())
    f.close()
    
    global_scene = satpy.Scene(reader="ahi_hsd", filenames=['/tmp/{}'.format(fname)])
    global_scene.load(['B08'])
    
    imfile = io.BytesIO()
    imageio.imwrite(imfile, global_scene['B08'].values[::4,::4], format="png")
    imfile.seek(0)
    
    os.remove('/tmp/{}'.format(fname))
    global_scene = None
    
    blob = bucket.blob(name[:-3]+'png')
    blob.upload_from_string(imfile.read(), content_type='image/png')
    imfile.close()
