from celery import Celery
import os
import typing as tp
from pathlib import Path
import pandas as pd
pd.set_option('display.width', None)
pd.set_option('display.max_columns', None)
pd.set_option('display.max_colwidth', None)

from dbi import BUCKET_NAME, s3_client, WellSite, ObjectStorageFileReference, Result_cp_map

app = Celery('tasks', backend="rpc://", broker=os.getenv('APP_BROKER_URI'), broker_pool_limit = 0)

@app.task(name="cp_map",queue="map_queue")
def cp_map(
    filelist:tp.List[tp.Union["ObjectStorageFileReference",dict]],
    project_name:str,
    plate_name:str,
    imageBatchID:int,
)->tp.Union["Result_cp_map",dict]:
    """
        run cellprofiler on a batch of images

        Args:
            filelist (tp.List['{filename:str,s3path:str}']): list of image files to process (objects of custom class are difficult to send over the network, so we use a small dictionary instead)
            project_name (str): project name
            plate_name (str): plate name
            imageBatchID (int): image batch id. This is used to identify the batch of images to process, result files are stored in the object storage with a key made from projectname+platename+batchid. a batch may contain any number of image sets

        Returns:
            Result_cp_map:
                converted to a json serializable dictionary for sending over the network.

                contains 1) list of cellprofiler output filename and corresponding s3 path¨
                         2) list of wells and sites processed
    """

    cellprofilerInputFileListFilePath=Path("./cellprofilerinput/imagefilelist.txt")
    local_files:tp.List[Path]=[]
    with cellprofilerInputFileListFilePath.open("w+") as f:
        for file in filelist:
            assert type(file)==dict, f"{type(file)=} {file=}"
            file=ObjectStorageFileReference(**file)

            filename,s3path=file.filename,file.s3path

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

    ret_filepaths:tp.List[ObjectStorageFileReference]=[]
    well_site_list:tp.List[WellSite]=[]
    for file in outputFilepaths:
        file_csv=Path(file)
        assert file_csv.exists()

        # convert file to parquet using pandas before upload
        df=pd.read_csv(file_csv)
        file_parquet=file_csv.with_suffix(".parquet")
        df.to_parquet(file_parquet)

        s3Filename = f"{project_name}/{plate_name}/{imageBatchID}/{file_parquet.name}"
        s3_client.upload_file(str(file_parquet), os.getenv("S3_BUCKET_NAME"), s3Filename)

        # delete both local files
        file_csv.unlink()
        file_parquet.unlink()

        ret_filepaths.append(ObjectStorageFileReference(
            filename=file_parquet.name,
            s3path=s3Filename,
        ))

        target_colname="FileName_nucleus"
        if target_colname in df.columns:
            # go through each row in target column, split by underscore, first segment is well, second is site, rest is ignored
            for row in df[target_colname]:
                # TODO this should be handled better! just splitting is not very stable
                well,site,_ignore=row.split("_",2)
                site=int(site[1:]) # strip leading 's', and ensure the rest is an integer
                well_site_list.append(WellSite(well=well,site=site))

    return Result_cp_map(
        resultfiles=ret_filepaths,
        wells=well_site_list
    ).model_dump()

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

