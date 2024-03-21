import os
from http import HTTPStatus as Status
from pathlib import Path
from flask import Flask, request, redirect, url_for, send_from_directory, jsonify, make_response, Response
from werkzeug.utils import secure_filename
from pathlib import Path
import typing as tp
import pandas as pd
import json

from dbinterface import DB, ImageMetadata

UPLOAD_FOLDER = './uploads'
STATIC_FILE_DIR = './static'
ALLOWED_EXTENSIONS = {'.png', '.jpg', '.jpeg', '.gif', '.tif', '.tiff', '.csv', '.json'}

mydb=DB()

app = Flask(__name__)
app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER
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


if not Path(app.config['UPLOAD_FOLDER']).exists():
    Path(app.config['UPLOAD_FOLDER']).mkdir()

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

@app.route('/uploads/<name>')
def serveUpladedFile(name:str)->tp.Any:
    return send_from_directory(app.config["UPLOAD_FOLDER"], name)

@app.route("/",methods=["GET"])
def serveHome():
    return serveStaticFile("home.html")

@app.route('/api/upload', methods=['POST'])
def uploadFile()->tp.Tuple[str,int]:
    # check if the post request has the file part
    if len(request.files)==0:
        return jsonify({"error":"no files provided"}),Status.BAD_REQUEST

    files_uploaded=[]

    file_sets={}

    # type(request.files)==werkzeug.MultiDict[key=str,value=werkzeug.FileStorage]
    #    see https://tedboy.github.io/flask/generated/generated/werkzeug.MultiDict.html
    for inputname,files in request.files.lists():
        if inputname not in file_sets:
            file_sets[inputname]=[]

        for file in files:
            # If the user does not select a file, the browser may submit an empty file without a filename.
            if file.filename == '':
                return jsonify({"error":"empty filename"}),Status.BAD_REQUEST

            file_is_valid=file is not None # and filenameIsAllowed(file.filename)

            if file_is_valid:
                sec_filename = secure_filename(file.filename)
                save_filepath=os.path.join(app.config['UPLOAD_FOLDER'], sec_filename)
                file.save(save_filepath)
                files_uploaded.append((file.filename,save_filepath,file_is_valid))

                file_sets[inputname].append((file.filename,save_filepath))
            else:
                files_uploaded.append((file.filename,None,file_is_valid))

    # inputname: image_files filename: G11_s3_x0_y1_Fluorescence_488_nm_Ex.tiff
    coordinates=pd.read_csv(file_sets["coordinate_file"][0][1])
    print(coordinates.head(5))
    with Path(file_sets["experiment_file"][0][1]).open("r") as f:
        experiment=json.load(f)
    # print pretty formatted experiment metadata
    print(json.dumps(experiment,indent=4))
    print("got {} images".format(len(file_sets["image_files"])))
    for filename,filepath in file_sets["image_files"][:5]:
        metadata=ImageMetadata(
            image=filename,
            db=mydb,
        )
        print(filename,repr(metadata))


    # redirect to display last uploaded file
    return jsonify(dict(
                files=[
                    dict(
                        filename=filename,
                        url=save_filepath,
                        success=success,
                    )
                    for (filename,save_filepath,success)
                    in files_uploaded
                ]
            ))

print("Running web frontend on port",os.getenv("WEB_FRONTEND_PORT"))
app.run(debug=True,host="0.0.0.0",port=os.getenv("WEB_FRONTEND_PORT"))
