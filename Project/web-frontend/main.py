import os
from http import HTTPStatus as Status
from pathlib import Path
from flask import Flask, request, redirect, url_for, send_from_directory, jsonify, render_template
from werkzeug.utils import secure_filename
from pathlib import Path
import typing as tp

UPLOAD_FOLDER = './uploads'
STATIC_FILE_DIR = './static'
ALLOWED_EXTENSIONS = {'.png', '.jpg', '.jpeg', '.gif', '.tif', '.tiff'}

app = Flask(__name__)
app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER
app.config['STATIC_FILE_DIR'] = STATIC_FILE_DIR
# limit max upload file size (will raise an exception if exceeded)
# app.config['MAX_CONTENT_LENGTH'] = 16 * 1000 * 1000

if not Path(app.config['UPLOAD_FOLDER']).exists():
    Path(app.config['UPLOAD_FOLDER']).mkdir()

def filenameIsAllowed(filename:str|Path)->bool:
    return Path(filename).suffix.lower() in ALLOWED_EXTENSIONS

@app.route('/static/<name>')
def serveStaticFile(name:str)->tp.Any:
    return send_from_directory(app.config["STATIC_FILE_DIR"], name)

@app.route('/uploads/<name>')
def serveUpladedFile(name:str)->tp.Any:
    return send_from_directory(app.config["UPLOAD_FOLDER"], name)

@app.route("/",methods=["GET"])
def serveHome():
    return render_template("home.html"),Status.OK

@app.route('/api/upload', methods=['POST'])
def uploadFile()->tp.Tuple[str,int]:
    # check if the post request has the file part
    if len(request.files)==0:
        return jsonify({"error":"no files provided"}),Status.BAD_REQUEST

    files_uploaded=[]

    # type(request.files)==werkzeug.MultiDict[key=str,value=werkzeug.FileStorage]
    #    see https://tedboy.github.io/flask/generated/generated/werkzeug.MultiDict.html
    for inputname,files in request.files.lists():
        for file in files:
            # If the user does not select a file, the browser may submit an empty file without a filename.
            if file.filename == '':
                return jsonify({"error":"empty filename"}),Status.BAD_REQUEST

            filename_is_allowed=filenameIsAllowed(file.filename)

            if file and filename_is_allowed:
                sec_filename = secure_filename(file.filename)
                save_filepath=os.path.join(app.config['UPLOAD_FOLDER'], sec_filename)
                file.save(save_filepath)
                files_uploaded.append((file.filename,sec_filename,save_filepath,filename_is_allowed))
            else:
                files_uploaded.append((file.filename,None,None,filename_is_allowed))
    
    # redirect to display last uploaded file
    return jsonify(dict(
                files=[
                    dict(
                        filename=filename,
                        url=save_filepath,
                        success=success,
                    )
                    for (filename,sec_filename,save_filepath,success)
                    in files_uploaded
                ]
            ))

app.run(debug=True,host="0.0.0.0",port=os.getenv("WEB_FRONTEND_PORT"))
