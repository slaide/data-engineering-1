import os
from http import HTTPStatus as Status
from pathlib import Path
from flask import Flask, request, jsonify, make_response, Response, stream_with_context
from pathlib import Path
import typing as tp
import pandas as pd
import json
import io
from dataclasses import dataclass

from celery.result import AsyncResult
from dbi import DB, BUCKET_NAME, S3Client, ObjectStorageFileReference, ImageMetadata

mydb=DB()
s3client=S3Client()

STATIC_FILE_DIR = './static'

app = Flask(__name__)
app.config['STATIC_FILE_DIR'] = STATIC_FILE_DIR
# limit max upload file size (will raise an exception if exceeded)
# app.config['MAX_CONTENT_LENGTH'] = 16 * 1000 * 1000

@dataclass
class CachedFileInformation:
    content:tp.Union[bytes,str]
    timestamp:float

cached_files:tp.Dict[str,CachedFileInformation]={}

def read_file(dirname:str,filename:str)->tp.Tuple[tp.Union[str,bytes],Status]:
    """ if file is in cached_files and its timestamp does not match the timestamp of the file, read the file again and save the new timestamp """

    full_file_path=Path(dirname)/filename
    if not full_file_path.exists():
        return f"File {full_file_path} does not exist",Status.NOT_FOUND
    
    # get last modified timestamp of the file
    file_timestamp=full_file_path.stat().st_mtime

    # disable caching for now, seems to not work right
    if filename in cached_files:
        cached_file=cached_files[filename]
        if cached_file.timestamp==file_timestamp:
            return cached_file.content,Status.OK
        
    with full_file_path.open("rb") as f:
        content=f.read()
        cached_files[filename]=CachedFileInformation(content=content,timestamp=file_timestamp)
        return content,Status.OK


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
def download_file(bucket:str, filename:str):
    def generate_file():
        with io.BytesIO() as file_stream:
            s3client.handle.download_fileobj(Bucket=bucket, Key=filename, Fileobj=file_stream)
            file_stream.seek(0)  # Go to the beginning of the file-like object
            while chunk := file_stream.read(4096):  # Read in chunks of 4KB
                yield chunk

    response = Response(stream_with_context(generate_file()), content_type='application/octet-stream')
    response.headers['Content-Disposition'] = f'attachment; filename={filename}'
    return response

@app.route('/serve/<bucket>/<path:filename>')
def serve_s3_file(bucket:str, filename:str):
    # download file from s3 and serve it (NOT as download!)
    file_contents=io.BytesIO()
    s3client.handle.download_fileobj(Bucket=bucket, Key=filename, Fileobj=file_contents)
    file_contents.seek(0)
    mimetype=get_file_mimetype(filename)
    response=make_response(file_contents.read())
    response.headers["Content-Type"]=mimetype
    return response,Status.OK

@app.route("/",methods=["GET"])
def serveHome():
    return serveStaticFile("home.html")

def cp_map(*args,**kwargs):
    """
    wrapper for async call to cp_map

    returns a handle to the async result, though the actual result is None
    """
    res:AsyncResult=mydb.celery_tasks.send_task("cp_map",args=args,kwargs=kwargs,queue="map_queue")
    return res

@app.route("/api/get_projects",methods=["GET"])
def getProjectNames():
    projectnames:dict=mydb.getProjectNames().__dict__
    return jsonify(projectnames),Status.OK

@app.route("/api/get_experiments",methods=["GET"])
def getExperiments():
    projectname=request.args.get("projectname",type=str)
    assert projectname is not None
    experiments:dict=mydb.getExperiments(projectname=projectname).__dict__
    return jsonify(experiments),Status.OK

@app.route("/api/get_experiment_progress",methods=["GET"])
def getExperimentProgress():
    projectname=request.args.get("projectname",type=str)
    experimentname=request.args.get("experimentname",type=str)
    assert projectname is not None
    assert experimentname is not None
    progress=mydb.checkExperimentProcessingStatus(
        project_name=projectname,
        experiment_name=experimentname
    )
    ret:str=progress.model_dump_json(exclude={"resultfiles":True})
    return ret,Status.OK

def cp_reduce(*args,**kwargs)->AsyncResult:
    """
    wrapper for async call to cp_reduce

    returns a handle to the async result, though the actual result is None
    """

    res:AsyncResult=mydb.celery_tasks.send_task("cp_reduce",args=args,kwargs=kwargs,queue="reduce_queue")
    return res

@app.route("/api/get_experiment_results",methods=["GET"])
def getExperimentResults():
    projectname=request.args.get("projectname",type=str)
    experimentname=request.args.get("experimentname",type=str)
    assert projectname is not None
    assert experimentname is not None
    plot_filename="plot.html"
    plot_s3_name=f"{projectname}/{experimentname}/{plot_filename}"
    _results=cp_reduce(projectname,experimentname,plot_s3_name=plot_s3_name)
    
    res={"resultfiles":[
        {
            "filename":plot_filename,
            "download":f"/download/{BUCKET_NAME}/{plot_s3_name}",
            "display_src":f"/serve/{BUCKET_NAME}/{plot_s3_name}"
        }
    ]}
    return jsonify(res),Status.OK

@app.route('/api/upload', methods=['POST'])
def uploadFile():
    # check if the post request has the file part
    if len(request.files)==0:
        return jsonify({"error":"no files provided"}),Status.BAD_REQUEST

    # coordinate file
    coordinate_file=request.files.get("coordinate_file")
    if coordinate_file is None:
        return jsonify({"error":"no coordinate file provided"}),Status.BAD_REQUEST
    
    coordinates=pd.read_csv(coordinate_file.stream)

    # experiment metadata file
    experiment_file=request.files.get("experiment_file")
    if experiment_file is None:
        return jsonify({"error":"no experiment file provided"}),Status.BAD_REQUEST
    
    experiment=json.load(experiment_file.stream)
    experiment_name=Path(experiment["output_path"]).name
    experiment["experiment_name"]=experiment_name

    # compound layout file
    compound_layout_file=request.files.get("compound_layout_file")
    if compound_layout_file is None:
        return jsonify({"error":"no compound layout file provided"}),Status.BAD_REQUEST
    
    #compound_layout=pd.read_csv(compound_layout_file.stream)
    project_name=experiment["project_name"]
    experiment_name=experiment["experiment_name"]
    assert compound_layout_file.filename is not None
    compound_layout_s3_filename=f"{project_name}/{experiment_name}/{Path(compound_layout_file.filename).name}"
    s3client.uploadFile(compound_layout_file,compound_layout_s3_filename)

    # image files
    image_files=request.files.getlist("image_files")
    if len(image_files)==0:
        return jsonify({"error":"no image files provided"}),Status.BAD_REQUEST
    
    imageSets=mydb.insertExperimentMetadata(
        experiment,
        coordinates,
        compound_layout_s3_filename,
        images=image_files,
        image_s3_bucketname=BUCKET_NAME,
    )

    imageFileList=[]
    for _setName,imageSet in imageSets.items():
        batch_id=mydb.getProcessingBatchID(experiment["project_name"],experiment_name)

        print(
            "registering batch with id",batch_id,
            "for experiment",experiment_name,
            "in project",experiment["project_name"],
            "for well",imageSet[0].wellname,
            "for site",imageSet[0].site,
            "with",len(imageSet),"images"
        )
        
        mydb.registerBatch(
            project_name=experiment["project_name"],
            experiment_name=experiment_name,
            batchid=batch_id
        )

        imageFileList.extend(imageSet)

        cp_map(
            [
                ObjectStorageFileReference(
                    filename=Path(image.real_filename).name,
                    s3path=image.storage_filename,
                ).model_dump()
                for image
                in imageSet
            ],
            project_name=experiment["project_name"],
            experiment_name=experiment_name,
            plate_name=experiment["plate_name"],
            imageBatchID=batch_id,
        )

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

_WEB_FRONTEND_PORT_ENV=os.getenv("WEB_FRONTEND_PORT") ; assert _WEB_FRONTEND_PORT_ENV is not None
print("Running web frontend on port",_WEB_FRONTEND_PORT_ENV)
app.run(debug=True,host="0.0.0.0",port=int(_WEB_FRONTEND_PORT_ENV))
