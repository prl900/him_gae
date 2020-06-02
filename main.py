# [START gae_python37_app]

from flask import Flask
#from flask import render_template
#from flask import send_file
from flask import request

from datetime import datetime
from datetime import timedelta
import pytz
import base64
import io
import os
import bz2
import json

import ftplib
#import imageio
#import satpy

from google.cloud import storage
from google.cloud import datastore
from google.cloud import pubsub_v1 as pubsub

# If `entrypoint` is not defined in app.yaml, App Engine will look for an app
# called `app` in `main.py`.
app = Flask(__name__)

storage_client = storage.Client()
bucket_name = os.environ['BUCKET_NAME']
bucket = storage_client.get_bucket(bucket_name)

datastore_client = datastore.Client(os.environ['PROJ_ID'])

publisher = pubsub.PublisherClient()
topic = 'projects/{}/topics/{}'.format(os.environ['PROJ_ID'], os.environ['TOPIC'])

def add_entry(client, start, name):
    key = client.key('Image')

    im = datastore.Entity(key, exclude_from_indexes=['name'])

    im.update({
        'received': datetime.utcnow(),
        'im_start': start,
        'name': name,
    })

    client.put(im)

    key = client.key('Task', 'sample_task')
    client.delete(key)

    # Delete entries older than 48 hours
    query = client.query(kind='Image')
    query.add_filter('received', '<', datetime.utcnow() - timedelta(seconds=48*60*60))
    keys = list([entity.key for entity in query.fetch()])
    client.delete_multi(keys)

    return None


@app.route('/')
def entry():
    return 'Welcome to the Himawari8 GAE process pipeline' 

@app.route('/update')
def proc():
    try:
        ftp = ftplib.FTP("ftp.ptree.jaxa.jp") 
        ftp.login(os.environ['USER'], os.environ['PSWD'])
    except:
        return 'failed'

    now = datetime.utcnow()
    rnow = datetime(now.year, now.month, now.day, now.hour, (now.minute//10)*10, tzinfo=pytz.utc)

    actv_dates = [rnow - timedelta(seconds=600*i) for i in range(4)]

    for d in actv_dates:
        for band, res in [(1,10),(2,10),(3,5),(4,10)]:
            for sec in [7,8,9]:

                fname = f"HS_H08_{d.strftime('%Y%m%d_%H%M')}_B{band:02d}_FLDK_R{res:02d}_S{sec:02d}10.DAT.bz2"
    
                blob = bucket.blob("himawari8/"+fname[:-4])
                if blob.exists():
                    continue
                
                try:
                    ftp_dir = f"/jma/hsd/{d.strftime('%Y%m')}/{d.strftime('%d')}/{d.strftime('%H')}/"
                    ftp.cwd(ftp_dir)
                    b = io.BytesIO()
                    ftp.retrbinary('RETR %s' % fname, b.write)
                    b.seek(0)
                    blob.upload_from_string(bz2.decompress(b.read()), content_type='application/binary')
                    b.close()
                    add_entry(datastore_client, d, fname[:-4])
                except Exception as e:
                    continue

                publisher.publish(topic, data=('himawari8/'+fname[:-4]).encode("utf-8"))
    
    ftp.quit()
    
    #delete older than 60 minutes
    blobs = bucket.list_blobs(prefix='himawari8')
    blob_names = sorted([blb.name for blb in blobs], reverse=True)
    blobs = bucket.list_blobs(prefix='himawari8')
    for blb in blobs:
        if blb.name in blob_names[72:]:
            blb.delete()

    return 'ok'


@app.route('/stats')
def stats():
    query = datastore_client.query(kind='Image')
    query.order = ['im_start']
    data = list([dict(entity) for entity in query.fetch()])

    return json.dumps(data, indent=4, sort_keys=True, default=str)

@app.route('/image')
def dashboard():
    sector = int(request.args.get('sector'))
    i = int(request.args.get('i'))
    j = int(request.args.get('j'))

    query = datastore_client.query(kind='Image')
    query.order = ['im_start']
    data = list([dict(entity) for entity in query.fetch()])

    fname = None
    for entry in data:
        if entry['name'][-9:] == f"S{sector:02d}10.DAT":
            fname = entry['name'][:-3] + "png"

    #return render_template('dashboard.html', img_log=json.dumps(keys, indent=4, sort_keys=True, default=str))
    return fname

"""
@app.route('/show')
def show():

    storage_client = storage.Client()
    bucket = storage_client.get_bucket(bucket_name)
    blobs = bucket.list_blobs(prefix='himawari8')

    for blob in blobs:
        name = blob.name

    fname = name.split('/')[1]

    blob = bucket.blob(name)
    data = blob.download_as_string()
    f = io.BytesIO(data)
    f.seek(0)

    with open('/tmp/{}'.format(fname), 'wb') as mf:
        mf.write(f.read())
    f.close()

    global_scene = satpy.Scene(reader="ahi_hsd", filenames=['/tmp/{}'.format(fname)])
    global_scene.load(['B08'])
    imfile = io.BytesIO()
    imageio.imwrite(imfile, global_scene['B08'].values[::4,::4], format="png")

    os.remove('/tmp/{}'.format(fname))
    global_scene = None
    imfile.seek(0)

    return send_file(imfile, attachment_filename='him.png', mimetype='image/png')


@app.route('/last')
def last():
    now = datetime.utcnow()
    rnow = datetime(now.year, now.month, now.day, now.hour, (now.minute//10)*10, tzinfo=pytz.utc)

    actv_dates = [rnow - timedelta(seconds=600*i) for i in range(4)]

    for d in actv_dates:
        fname = 'HS_H08_{}_B08_FLDK_R20_S0710.DAT.bz2'.format(d.strftime("%Y%m%d_%H%M"))

    return fname
"""


if __name__ == '__main__':
    # This is used when running locally only. When deploying to Google App
    # Engine, a webserver process such as Gunicorn will serve the app. This
    # can be configured by adding an `entrypoint` to app.yaml.
    app.run(host='127.0.0.1', port=8080, debug=True)
# [END gae_python37_app]
