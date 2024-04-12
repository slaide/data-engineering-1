from celery import Celery
import traceback as tb
import os, io, signal, sys
import subprocess as sp
import typing as tp
from pathlib import Path
import pandas as pd
import polars as pl
pd.set_option('display.width', None)
pd.set_option('display.max_columns', None)
pd.set_option('display.max_colwidth', None)

from dbi import DB, BUCKET_NAME, S3Client, WellSite, ObjectStorageFileReference, Result_cp_map

from cell_profile import PlateMetadata, print_time

s3client=S3Client()
mydb=DB(recreate=False)

tasks = Celery('tasks', broker=os.getenv('APP_BROKER_URI'), broker_pool_limit = 0, broker_connection_retry_on_startup = True)
#tasks.conf.broker_connection_retry_on_startup = True
rpc = Celery('tasks', backend="rpc://", broker=os.getenv('APP_BROKER_URI'), broker_pool_limit = 0, broker_connection_retry_on_startup = True)
#rpc.conf.broker_connection_retry_on_startup = True

@tasks.task(name="cp_map",queue="map_queue")
def cp_map(
    filelist:tp.List[tp.Union["ObjectStorageFileReference",dict]],
    project_name:str,
    experiment_name:str,
    plate_name:str,
    imageBatchID:int,
    retry_attempt:int=0,
):
    """
        run cellprofiler on a batch of images

        Args:
            filelist (tp.List['{filename:str,s3path:str}']): list of image files to process (objects of custom class are difficult to send over the network, so we use a small dictionary instead)
            project_name (str): project name
            plate_name (str): plate name
            imageBatchID (int): image batch id. This is used to identify the batch of images to process, result files are stored in the object storage with a key made from projectname+platename+batchid. a batch may contain any number of image sets

    """

    # register existence of this batch
    mydb.registerBatch(
        project_name=project_name,
        experiment_name=experiment_name,
        batchid=imageBatchID
    )

    experimentid=mydb.getExperimentID(project_name,experiment_name)

    # Save the original signal handler
    original_handler = signal.getsignal(signal.SIGTERM)

    # Define the custom signal handler inside the function to access `batch_id`
    def shutdown_gracefully(signum, frame):
        print(f"warning - SIGTERM received during the processing of batch {imageBatchID}.")

        # update database entry for processing batch with status=terminated, and end_time
        mydb.dbExec(f"""
            UPDATE profile_results
            SET end_time=CURRENT_TIMESTAMP,
                status='terminated'
            WHERE experimentid={experimentid}
            AND batchid={imageBatchID};
        """)

        # Call the original handler after custom handling, pass the signal and frame
        if callable(original_handler):
            original_handler(signum, frame)
        else:
            print("warning - during SIGTERM handling, previous signal handler was not callable. Exiting instead.")
            sys.exit(1)

    # Set the new signal handler
    signal.signal(signal.SIGTERM, shutdown_gracefully)

    try:
        # download files to prepare for cellprofiler ingestion
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
                s3client.downloadFile(
                    object_name=s3path, 
                    local_filename=str(local_image_filename.absolute())
                )
                # write local image file path to cellprofiler input file
                f.write(f"{str(local_image_filename.absolute())}\n")
                # save image file path for later deletion
                local_files.append(local_image_filename)

        def deleteLocalFiles():
            """ delete all local files again """
            for local_image_filename in local_files:
                local_image_filename.unlink()
            cellprofilerInputFileListFilePath.unlink()

        # update database entry for processing batch with start time and status=processing
        mydb.dbExec(f"""
            UPDATE profile_results
            SET start_time=CURRENT_TIMESTAMP,
                status='processing images'
            WHERE experimentid={experimentid}
            AND batchid={imageBatchID};
        """)

        # then actually run cellprofiler (and ignore exit code, for now)
        cellprofiler_shell_command="venv/bin/python3 -m cellprofiler --run-headless --run --project=cellprofilerinput/morphology_pipeline.cpproj --file-list=cellprofilerinput/imagefilelist.txt --image-directory=cellprofilerinput/ --output-directory=cellprofileroutput/ --conserve-memory True"
        cp_cmd=sp.run(cellprofiler_shell_command,shell=True)
        if cp_cmd.returncode!=0:
            # likely indicates OOM error
            if cp_cmd.returncode==137:
                error_status=f"failed.OOM({retry_attempt})"
                print(f"cellprofiler error - {error_status} -- OOM error, retrying for the {retry_attempt}th time")
                do_attempt_retry=True
            else:
                error_status=f"failed.{cp_cmd.returncode}"
                print(f"cellprofiler error - {error_status}")
                do_attempt_retry=False

            # set status of batch to failed.exitcode (for tracking purposes)
            # set end_time to current time (to indicate that this batch is done, regardless of success or failure)
            mydb.dbExec(f"""
                UPDATE profile_results
                SET end_time=CURRENT_TIMESTAMP,
                    status='{error_status}'
                WHERE experimentid={experimentid}
                AND batchid={imageBatchID};
            """)

            deleteLocalFiles()

            if do_attempt_retry:
                # generate new batch id (do not overwrite existing batch id, which contains error information in its status)
                new_batch_id=mydb.getProcessingBatchID(project_name,experiment_name)
                # retry with new batch id
                return cp_map(
                    filelist=filelist,
                    project_name=project_name,
                    experiment_name=experiment_name,
                    plate_name=plate_name,
                    imageBatchID=new_batch_id,
                    retry_attempt=retry_attempt+1
                )
            else:
                raise ValueError(f"cellprofiler command failed with exit code {cp_cmd.returncode}")

        deleteLocalFiles()

        # update database entry for processing batch with status=uploading
        mydb.dbExec(f"""
            UPDATE profile_results
            SET status='uploading results to storage'
            WHERE experimentid={experimentid}
            AND batchid={imageBatchID};
        """)

        # output files are the following
        outputFilepaths=[
            "cellprofileroutput/Experiment.csv",
            "cellprofileroutput/Image.csv",
            "cellprofileroutput/cytoplasm.csv",
            "cellprofileroutput/nucleus.csv",
        ]

        ret_filepaths:tp.List[ObjectStorageFileReference]=[]
        well_site_list:tp.List[WellSite]=[]
        for file in outputFilepaths:
            file_csv=Path(file)
            assert file_csv.exists(), str(file_csv)

            # convert file to parquet using pandas before upload
            df=pd.read_csv(file_csv)
            file_parquet=file_csv.with_suffix(".parquet")
            df.to_parquet(file_parquet)

            s3Filename = f"{project_name}/{plate_name}/{imageBatchID}/{file_parquet.name}"
            s3client.uploadFile(
                file=str(file_parquet),
                object_name=s3Filename
            )

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

        res=Result_cp_map(
            resultfiles=ret_filepaths,
            wells=well_site_list
        )

        # write result file information back to db

        mydb.insertProfileResultBatch(
            project_name,
            experiment_name=experiment_name,
            batchid=imageBatchID,
            result_file_paths=res.resultfiles,
            well_site_list=res.wells,
        )

        # update database entry for processing batch with end time and status=done
        mydb.dbExec(f"""
            UPDATE profile_results
            SET end_time=CURRENT_TIMESTAMP,
                status='done'
            WHERE experimentid={experimentid}
            AND batchid={imageBatchID};
        """)
    finally:
        # restore original signal handler
        signal.signal(signal.SIGTERM, original_handler)

@tasks.task(name="cp_reduce",queue="reduce_queue")
def cp_reduce(
    project_name:str,
    experiment_name:str,
    plot_s3_name:str,
):
    """
        reduce the results from a plate within a project to a single image file
        i.e. combine dataframes from all cellprofiler batches on the same plate into a single dataframe, then create a single plot showing the distribution of the data

        Args:
            project_name (str): project name
            plate_name (str): plate name

        Returns:
            str: path to the reduced file
    """

    exp_status=mydb.checkExperimentProcessingStatus(
        project_name=project_name,
        experiment_name=experiment_name
    )

    experiment_id=mydb.getExperimentID(project_name,experiment_name)

    # get result file locations
    resultfiles_res=mydb.dbExec(f"""
        SELECT s3path,filename
        FROM profile_result_files
        WHERE profile_resultid IN (
            SELECT id
            FROM profile_results
            WHERE experimentid = {experiment_id}
        );
    """)
    assert type(resultfiles_res)==list
    if len(resultfiles_res)==0:
        return None
    
    resultfiles=[
        ObjectStorageFileReference(s3path=s3path,filename=filename)
        for s3path,filename
        in resultfiles_res
    ]

    # group files by filename
    file_list:tp.Dict[str,tp.List[str]]={}
    for file in resultfiles:
        if not file.filename in file_list:
            file_list[file.filename]=[]
        file_list[file.filename].append(file.s3path)

    # download files and combine them into a single dataframe per filename
    dataframes:tp.Dict[str,pd.DataFrame]={}
    
    for filename,s3paths in file_list.items():
        if filename=="Experiment.parquet":
            # this file contains experiment metadata, such as cellprofiler version
            continue
        elif filename=="Image.parquet":
            # this file contains image QC data, which is not relevant for this analysis
            continue
        elif filename in ["cytoplasm.parquet","nucleus.parquet"]:
            # these files contain the actual data
            pass
        else:
            raise ValueError(f"unexpected filename {filename}")

        filename_suffix=Path(filename).suffix
        for s3path in s3paths:
            tempFile=io.BytesIO()

            # get merged frames from s3 as in-memory file
            s3client.downloadFileObj(
                object_name=s3path,
                fileobj=tempFile,
            )
            tempFile.seek(0)

            # read into dataframe, accounting for csv/parquet
            df=None
            if filename_suffix==".csv":
                df=pd.read_csv(tempFile)
            elif filename_suffix==".parquet":
                df=pd.read_parquet(tempFile)
            else:
                raise ValueError(f"unexpected file extension {filename_suffix} in {filename}")
            
            if filename in dataframes:
                dataframes[filename]=pd.concat([dataframes[filename],df])
            else:
                dataframes[filename]=df

        if False:
            print(f"processing {filename}")
            # print dataframe size, header and first 2 rows
            print(f"filename: {filename}")
            print(f"shape: {dataframes[filename].shape}")
            print(f"columns: {dataframes[filename].columns}")
            print(f"head:\n{dataframes[filename].head(2)}")

    cytoplasm_file_filename="cytoplasm"
    nucleus_file_filename="nucleus"

    def changeFilename(filename:str):
        # this file has a column called Parent_nucleus, which is the ObjectNumber in the nucleus file
        if filename=="cytoplasm.parquet":
            return cytoplasm_file_filename
        elif filename=="nucleus.parquet":
            return nucleus_file_filename
        else:
            return filename

    # actual metadata columns by which to group data all data so that one entry in the final dataframe is one cell
    metadata_cols=["Metadata_Well","Metadata_Site"]
    metadata_root_key={
        "root_file":nucleus_file_filename,
        "root_attribute_col":"ObjectNumber",
        "foreign_attribute_col":"{featureFilename}_Parent_nucleus"
    }

    dataframes_pl:dict[str,pl.DataFrame]={}
    for filename,df in dataframes.items():
        df=pl.DataFrame(df)

        newFilename=changeFilename(filename)
        def change_colname(colname:str)->str:
            if colname in metadata_cols:
                return colname
            if newFilename==metadata_root_key["root_file"] and colname==metadata_root_key["root_attribute_col"]:
                return colname
            
            new_colname=f"{newFilename}_{colname}"
            return new_colname
        
        colname_mapper={col:change_colname(col) for col in df.columns}

        dataframes_pl[newFilename]=df.rename(colname_mapper)

    pm=PlateMetadata(
        time_point_index=0,
        plate_name="myplate",
        feature_set_names=[cytoplasm_file_filename,nucleus_file_filename],
        feature_files=dataframes_pl,
        metadata_cols=metadata_cols,
        root_key=metadata_root_key,
    )
    # no file to parse this from, so we set it manually
    pm.time_point=0

    compound_layout_filepath=mydb.getExperimentCompoundLayout(project_name,experiment_name)
    f=io.BytesIO()
    s3client.downloadFileObj(compound_layout_filepath,f)
    f.seek(0)

    compound_layout=pl.DataFrame(pd.read_csv(f))

    try:
        print_time("processing plate metadata")
        p=pm.process(
            compound_layout,
            timeit=False,
        )
        print_time("finished processing plate metadata")
    except Exception as e:
        print_time("error processing plate metadata")
        e_str=tb.format_exc()
        print_time(f"error processing plate metadata:\n{e_str}")
        return None
    
    local_plot_filename="plot.html"
    p.plot(file_out=local_plot_filename)
    print_time("wrote plot to file")

    # upload plot to s3
    s3client.uploadFile(
        file=local_plot_filename,
        object_name=plot_s3_name
    )
    print_time(f"uploaded plot to s3 '{plot_s3_name}'")

