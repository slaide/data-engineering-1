from celery import Celery
import os
import typing as tp
from pathlib import Path
import boto3
from botocore.client import Config
import pandas as pd

BUCKET_NAME=os.getenv("S3_BUCKET_NAME")

s3_client = boto3.client('s3', 
                            endpoint_url=f'http://{os.getenv("S3_HOSTNAME")}:{int(os.getenv("S3_PORT"))}',
                            aws_access_key_id=os.getenv("S3_ACCESS_KEY_ID"),
                            aws_secret_access_key=os.getenv("S3_SECRET_ACCESS_KEY"),
                            config=Config(signature_version='s3v4'),
                            use_ssl=False,  # Disable SSL
                            verify=False)  # Disable SSL certificate verification


app = Celery('tasks', backend="rpc://", broker=os.getenv('APP_BROKER_URI'), broker_pool_limit = 0)

@app.task(name="cp_map",queue="map_queue")
def cp_map(
    filelist:tp.List['{filename:str,s3path:str}'],
    project_name:str,
    plate_name:str,
    imageBatchID:int,
)->tp.List[str]:
    cellprofilerInputFileListFilePath=Path("./cellprofilerinput/imagefilelist.txt")
    local_files:tp.List[Path]=[]
    with cellprofilerInputFileListFilePath.open("w+") as f:
        for file in filelist:
            filename,s3path=file["filename"],file["s3path"]

            local_image_filename:Path=Path("./cellprofilerinput")/filename
            s3bucketname, s3path = s3path.split("/",1)
            # download image from s3
            s3_client.download_file(s3bucketname, s3path, local_image_filename.absolute())
            # write local image file path to cellprofiler input file
            f.write(f"{str(local_image_filename.absolute())}\n")
            # save image file path for later deletion
            local_files.append(local_image_filename)

    # then run command below
    os.system("venv/bin/python3 -m cellprofiler --run-headless --run --project=cellprofilerinput/morphology_pipeline.cpproj --file-list=cellprofilerinput/imagefilelist.txt --image-directory=cellprofilerinput/ --output-directory=cellprofileroutput/ --conserve-memory True")
    
    # delete all local files again
    for local_image_filename in local_files:
        local_image_filename.unlink()
    cellprofilerInputFileListFilePath.unlink()

    # output files are the following
    outputFilepaths=[
        "cellprofileroutput/Experiment.csv",
        "cellprofileroutput/Image.csv",
        "cellprofileroutput/membrane.csv",
        "cellprofileroutput/nucleus.csv",
    ]
    
    for file in outputFilepaths:
        file_csv=Path(file)

        # convert file to parquet using pandas before upload
        df=pd.read_csv(file_csv)
        file_parquet=file_csv.with_suffix(".parquet")
        df.to_parquet(file_parquet)

        s3Filename = f"{project_name}/{plate_name}/{imageBatchID}/{file_parquet.name}"
        s3_client.upload_file(str(file_parquet), os.getenv("S3_BUCKET_NAME"), s3Filename)

        # delete both local files
        file_csv.unlink()
        file_parquet.unlink()

    return []

@app.task(name="cp_reduce",queue="reduce_queue")
def cp_reduce(
    project_name:str,
    plate_name:str,
)->str:
    """
        reduce the results from a plate within a project to a single image file
        i.e. combine dataframes from all cellprofiler batches on the same plate into a single dataframe, then create a single plot showing the distribution of the data

        Args:
            project_name (str): project name
            plate_name (str): plate name

        Returns:
            str: path to the reduced file
    """



    # TODO lots of python dataframe code
    ret=f"{BUCKET_NAME}/{project_name}/{plate_name}.res"
    return ret

