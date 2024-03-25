import os
from http import HTTPStatus as Status
from pathlib import Path
from flask import Flask, request, redirect, url_for, send_from_directory, jsonify, make_response, Response, stream_with_context
from werkzeug.utils import secure_filename
from pathlib import Path
import typing as tp
import pandas as pd
import json
import io

from dbinterface import DB, BUCKET_NAME, s3_client

STATIC_FILE_DIR = './static'
ALLOWED_EXTENSIONS = {'.png', '.jpg', '.jpeg', '.gif', '.tif', '.tiff', '.csv', '.json'}

mydb=DB()

app = Flask(__name__)
app.config['STATIC_FILE_DIR'] = STATIC_FILE_DIR
# limit max upload file size (will raise an exception if exceeded)
# app.config['MAX_CONTENT_LENGTH'] = 16 * 1000 * 1000

cached_files:tp.Dict[str,'{"content":str,"timestamp":int}']={}

def read_file(dirname:str,filename:str)->str:
    """ if file is in cached_files and its timestamp does not match the timestamp of the file, read the file again and save the new timestamp """

    full_file_path=Path(dirname)/filename
    if not full_file_path.exists():
        return f"File {full_file_path} does not exist",Status.NOT_FOUND
    
    # get last modified timestamp of the file
    file_timestamp=full_file_path.stat().st_mtime

    # disable caching for now, seems to not work right
    if filename in cached_files:
        cached_file=cached_files[filename]
        if cached_file["timestamp"]==file_timestamp:
            return cached_file["content"]
        
    with full_file_path.open("rb") as f:
        content=f.read()
        cached_files[filename]={"content":content,"timestamp":file_timestamp}
        return content


def filenameIsAllowed(filename:str|Path)->bool:
    return Path(filename).suffix.lower() in ALLOWED_EXTENSIONS

def get_file_mimetype(filename:str)->str:
    switcher={
        ".png":"image/png",
        ".jpg":"image/jpeg",
        ".jpeg":"image/jpeg",
        ".gif":"image/gif",
        ".tif":"image/tiff",
        ".tiff":"image/tiff",
        ".csv":"text/csv",
        ".json":"application/json",
        ".html":"text/html",
        ".css":"text/css",
        ".js":"application/javascript",
    }
    return switcher.get(Path(filename).suffix.lower(),"application/octet-stream")

@app.route('/static/<name>')
def serveStaticFile(name:str)->tp.Tuple[Response,Status]:
    mimetype=get_file_mimetype(name)
    file_contents=read_file(app.config["STATIC_FILE_DIR"], name)
    response=make_response(file_contents)
    response.headers["Content-Type"]=mimetype
    return response,Status.OK

@app.route('/download/<bucket>/<path:filename>')
def download_file(bucket, filename):
    def generate_file():
        with io.BytesIO() as file_stream:
            s3_client.download_fileobj(Bucket=bucket, Key=filename, Fileobj=file_stream)
            file_stream.seek(0)  # Go to the beginning of the file-like object
            while chunk := file_stream.read(4096):  # Read in chunks of 4KB
                yield chunk

    response = Response(stream_with_context(generate_file()), content_type='application/octet-stream')
    response.headers['Content-Disposition'] = f'attachment; filename={filename}'
    return response

@app.route("/",methods=["GET"])
def serveHome():
    return serveStaticFile("home.html")

def cp_map(*args,**kwargs)->tp.Any:
    res=mydb.celery.send_task("cp_map",args=args,kwargs=kwargs,queue="map_queue")
    return res

@app.route('/api/upload', methods=['POST'])
def uploadFile()->tp.Tuple[str,int]:
    # check if the post request has the file part
    if len(request.files)==0:
        return jsonify({"error":"no files provided"}),Status.BAD_REQUEST

    coordinates=pd.read_csv(request.files["coordinate_file"])
    # print(coordinates.head(5))

    experiment=json.load(request.files["experiment_file"])
    # print pretty formatted experiment metadata
    print(json.dumps(experiment,indent=4))
    experiment["experiment_name"]=Path(experiment["output_path"]).name

    imageFileList=mydb.insertExperimentMetadata(
        experiment,
        coordinates,
        images=request.files.getlist("image_files"),
        image_s3_bucketname=BUCKET_NAME
    )

    res=cp_map(
        [
            dict(
                filename=Path(image.real_filename).name,
                s3path=image.storage_filename,
            )
            for image
            in imageFileList
        ],
        project_name=experiment["project_name"],
        plate_name=experiment["plate_name"],
        imageBatchID=0,
    )
    print(res.get())

    # redirect to display last uploaded file
    return jsonify(dict(
        info="success",
        imagefiles=[
            {
                "filename":image.real_filename,
                "downloadpath":f"/download/{image.storage_filename}"
            }
            for image
            in imageFileList
        ],
    )),Status.OK

print("Running web frontend on port",os.getenv("WEB_FRONTEND_PORT"))
app.run(debug=True,host="0.0.0.0",port=os.getenv("WEB_FRONTEND_PORT"))
