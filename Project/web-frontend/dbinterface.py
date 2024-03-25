import os, time, typing as tp

from celery import Celery
import mariadb as db
import pandas as pd
from sqlalchemy import create_engine, text, Table, Column, Integer, Text, MetaData, ForeignKey, Boolean, Float, DateTime
from sqlalchemy.sql.expression import Insert, Select
from datetime import datetime
from dataclasses import dataclass
from pathlib import Path
from werkzeug.datastructures import FileStorage
import re
import boto3
from botocore.exceptions import NoCredentialsError, ClientError
from botocore.client import Config
from werkzeug.utils import secure_filename

# setting limits to none to display all columns
pd.set_option('display.width', None)
pd.set_option('display.max_columns', None)
pd.set_option('display.max_colwidth', None)

BUCKET_NAME=os.getenv("S3_BUCKET_NAME")

s3_client = boto3.client('s3', 
                            endpoint_url=f'http://{os.getenv("S3_HOSTNAME")}:{int(os.getenv("S3_PORT"))}',
                            aws_access_key_id=os.getenv("S3_ACCESS_KEY_ID"),
                            aws_secret_access_key=os.getenv("S3_SECRET_ACCESS_KEY"),
                            config=Config(signature_version='s3v4'),
                            use_ssl=False,  # Disable SSL
                            verify=False)  # Disable SSL certificate verification

def ensure_s3_bucket(bucket_name:str, region=None)->bool:
    """
    Create an S3 bucket in a specified region. If no region is specified, the bucket
    is created in the S3 default region (us-east-1).

    :param bucket_name: Bucket to create
    :param region: String region to create bucket in, e.g., 'us-west-2'
    :return: True if bucket created, False is bucket already existed
    """
    
    # Check if the bucket already exists
    try:
        s3_client.head_bucket(Bucket=bucket_name)
        return False
    except ClientError as e:
        error_code = e.response['Error']['Code']
        if error_code == '404':
            # The bucket does not exist, create it
            try:
                if region is None:
                    s3_client.create_bucket(Bucket=bucket_name)
                else:
                    location = {'LocationConstraint': region}
                    s3_client.create_bucket(Bucket=bucket_name,
                                            CreateBucketConfiguration=location)

                return True
            except ClientError as e:
                print(f"Failed to create bucket: {e}")
                raise RuntimeError(f"Failed to create bucket: {e}")
        else:
            print(f"Failed to check bucket existence: {e}")
            raise RuntimeError(f"Failed to check bucket existence: {e}")


def upload_file_to_s3_localstack(
    file:tp.Union[str,FileStorage], 
    bucket:str, 
    object_name:str
)->bool:
    """
    Uploads a file to the specified S3 bucket on LocalStack

    :param file_name: File to upload
    :param bucket: Bucket to upload to
    :param object_name: S3 object name. If not specified, file_name is used
    :return: True if file was uploaded, else False
    """

    ensure_s3_bucket(bucket)

    try:
        if isinstance(file, str):
            s3_client.upload_file(file, bucket, object_name)
        elif isinstance(file, FileStorage):
            s3_client.upload_fileobj(file, bucket, object_name)
        else:
            raise ValueError(f"file must be either a string or a FileStorage object, not {type(file)}")
    except NoCredentialsError:
        print("Credentials not available")
        return False
    return True


DBNAME="morphology_information"

dbmetadata=MetaData(schema=DBNAME)

# this table contains all projects
# a project must have a name
# a project can have any number of plates and any number of experiments
dbProjects=Table(
    "projects", dbmetadata,
    Column("id",Integer,primary_key=True,nullable=False,autoincrement=True,unique=True),
    Column("name",Text,unique=True,nullable=False),
)
# this table contains all physical plates
# a plate must have a barcode
# a plate can be part of any number of experiments
dbPlates=Table(
    "plates", dbmetadata,
    Column("id",Integer,primary_key=True,nullable=False,autoincrement=True,unique=True),
    Column("platetypeid",Integer,ForeignKey("plate_types.id",ondelete="cascade"),nullable=False),
    Column("barcode",Text,nullable=False),
)
# this table contains all experiments
# an experiment must belong to exactly one project
# an experiment must have a name
# an experiment can have a description
# an experiment involves exactly one plate

# the experiment contains meta information, such as:
# - the microscope used
# - the objective used
# - the list of wells that were imaged (this information is in the experiment_wells table)
# - the list of imaging channels used (this information is in the experiment_imaging_channels table)
# - the number of images per well in x/y/z/t, and the respective distances
dbExperiments=Table(
    "experiments", dbmetadata,
    Column("id",Integer,primary_key=True,nullable=False,autoincrement=True,unique=True),
    Column("projectid",Integer,ForeignKey("projects.id",ondelete="cascade"),nullable=False),
    Column("name",Text,nullable=False),
    Column("description",Text,nullable=True),
    Column("plateid",Integer,ForeignKey("plates.id",ondelete="cascade"),nullable=False),
    Column("microscopeid",Integer,ForeignKey("microscopes.id",ondelete="cascade"),nullable=False),
    Column("objectiveid",Integer,ForeignKey("objectives.id",ondelete="cascade"),nullable=False),
    Column("num_images_x",Integer,nullable=False),
    Column("num_images_y",Integer,nullable=False),
    Column("num_images_z",Integer,nullable=False),
    Column("num_images_t",Integer,nullable=False),
    Column("delta_x_mm",Float,nullable=False),
    Column("delta_y_mm",Float,nullable=False),
    Column("delta_z_um",Float,nullable=False),
    Column("delta_t_h",Float,nullable=False),
)
# this table contains a list of all plate types (manufacturers, brandnames, optional additional identifier)
dbPlateTypes=Table(
    "plate_types", dbmetadata,
    Column("id",Integer,primary_key=True,nullable=False,autoincrement=True,unique=True),
    Column("model_name",Text,nullable=False),
    Column("manufacturer",Text,nullable=False),
    Column("brand",Text,nullable=False),
    Column("num_wells",Integer,nullable=False),
    Column("other_info",Text,nullable=True),
)
# this table contains all wells for each plate type
# i.e. each row contains a reference to a plate type and the name of one well on that plate type
dbPlateTypeWells=Table(
    "platetype_wells", dbmetadata,
    Column("id",Integer,primary_key=True,nullable=False,autoincrement=True,unique=True),
    Column("platetypeid",Integer,ForeignKey("plate_types.id",ondelete="cascade"),nullable=False),
    Column("well_name",Text,nullable=False),
)
# this table contains a list of all microscopes
# each microscope has a unique name
dbMicroscopes=Table(
    "microscopes", dbmetadata,
    Column("id",Integer,primary_key=True,nullable=False,autoincrement=True,unique=True),
    Column("name",Text,nullable=False),
)
# this table contains a list of all available objectives
dbObjectives=Table(
    "objectives", dbmetadata,
    Column("id",Integer,primary_key=True,nullable=False,autoincrement=True,unique=True),
    Column("name",Text,nullable=False),
)
# this table contains a list of the wells used in an experiment (i.e. each row contains a reference to an experiment, and to a well)
dbExperimentWells=Table(
    "experiment_wells", dbmetadata,
    Column("id",Integer,primary_key=True,nullable=False,autoincrement=True,unique=True),
    Column("experimentid",Integer,ForeignKey("experiments.id",ondelete="cascade"),nullable=False),
    Column("wellid",Integer,ForeignKey("platetype_wells.id",ondelete="cascade"),nullable=False),
    Column("cell_line",Text,nullable=True),
)
# this table contains a list of all imaging channels used in an experiment (i.e. each row contains a reference to an experiment, and to an imaging channel, including imaging channel specific information, i.e. exposure time, analog gain, illumination strength)
dbExperimentImagingChannels=Table(
    "experiment_imaging_channels", dbmetadata,
    Column("id",Integer,primary_key=True,nullable=False,autoincrement=True,unique=True),
    Column("experimentid",Integer,ForeignKey("experiments.id",ondelete="cascade"),nullable=False),
    Column("channelid",Integer,ForeignKey("imaging_channels.id",ondelete="cascade"),nullable=False),
    Column("imaging_order_index",Integer,nullable=False),
    Column("exposure_time_ms",Float,nullable=False),
    Column("analog_gain",Float,nullable=False),
    Column("illumination_strength",Float,nullable=False),
)
# this table contains a list of all the available imaging channels
# mostly for legacy reasons, each channel also has a human readable name
dbImagingChannels=Table(
    "imaging_channels", dbmetadata,
    Column("id",Integer,primary_key=True,nullable=False,autoincrement=True,unique=True),
    Column("is_fluorescence",Boolean,nullable=False),
    Column("fluorescence_wavelength_nm",Integer,nullable=True),
    Column("is_brightfield",Boolean,nullable=False),
    Column("brightfield_type",Text,nullable=True),
    Column("name",Text,nullable=True),
)
# this table contains a list of all images taken
# each image belongs to one plate in one experiment
# each image is taken at a specific site (index in x/y/z/t)
# in a specific channel
# at specific coordinates (physically on the plate)
# and the image is stored in a specific location on the object storage (s3path)
dbImages=Table(
    "images", dbmetadata,
    Column("id",Integer,primary_key=True,nullable=False,autoincrement=True,unique=True),
    Column("plateid",Integer,ForeignKey("plates.id",ondelete="cascade"),nullable=False),
    Column("s3path",Text,nullable=False),
    Column("wellid",Integer,ForeignKey("platetype_wells.id",ondelete="cascade"),nullable=False),
    Column("site_x",Integer,nullable=False),
    Column("site_y",Integer,nullable=False),
    Column("site_z",Integer,nullable=False),
    Column("site_t",Integer,nullable=False),
    Column("experimentchannelid",Integer,ForeignKey("experiment_imaging_channels.id",ondelete="cascade"),nullable=False),
    Column("coord_x_mm",Float,nullable=True),
    Column("coord_y_mm",Float,nullable=True),
    Column("coord_z_um",Float,nullable=True),
    Column("coord_t",DateTime,nullable=True),
)

# deconstruct image naming scheme to get image metadata...
@dataclass(repr=True,frozen=False)
class ImageMetadata:
    storage_filename:tp.Optional[str]
    real_filename:str
    wellname:str
    well_id:int
    site:int
    site_x:int
    site_y:int
    site_z:int
    site_t:int
    channelname:str
    channel_id:int
    coord_x_mm:float
    coord_y_mm:float
    coord_z_um:float
    coord_t:datetime
    
    def __init__(self,
        real_filename:str,
        storage_filename:str,
        coords:tp.Dict[str,any],
        db:"DB"
    ):
        """
            params:

                real_filename:
                    the filename of the image, as it came out of the microscope
                    (full path, not just the filename, i.e. '/path/to/image/filename')

                storage_filename:
                    the filename of the image, as it is stored in the object storage
                    (includes bucket name, i.e. 'bucketname/filename')

            notes:
                image name: 
                    example: B03_s1_x0_y0_Fluorescence_405_nm_Ex.tif
                    format: <wellname>_s<site>_x<site_x>_y<site_y>[_z<site_z>]_<channelname>_Ex.tif
        """
        self.real_filename=real_filename
        self.storage_filename=storage_filename

        # image may be nested path, we only want the filename without the extension
        image_path=Path(real_filename).stem

        # unsure how best to get all the metadata from the image name, since splitting by _ is not enough (channel name can contain _)
        # so we will use a regex to extract the metadata
        match=re.match(r"(.+)_s(\d+)_x(\d+)_y(\d+)(_z(\d+))?_(.+)",image_path)
        assert match is not None

        wellname,site,site_x,site_y,_,site_z,channelname=match.groups()

        # site_z defaults to 1 if not present
        if site_z is None:
            site_z=1

        site,site_x,site_y,site_z=int(site),int(site_x),int(site_y),int(site_z)

        # remove underscores from channelname which are only inserted for filename formatting
        channelname=channelname.replace("_"," ")

        # query well id from database based on well name
        wellid_res=db.dbExec(f"select id from platetype_wells where well_name='{wellname}';")
        if len(wellid_res)==0:
            raise ValueError(f"well name {wellname} not found in database")
        well_id=wellid_res[0][0]

        # query channel id from database based on channel name
        channelid_res=db.dbExec(f"select id from imaging_channels where name='{channelname}';")
        if len(channelid_res)==0:
            available_channels=db.dbExec("select * from imaging_channels;",as_pd=True)
            print("available imaging channels:",available_channels)
            for channel in available_channels["name"]:
                print(channel,channelname in channel,channelname==channel)
            raise ValueError(f"channel name {channelname} not found in database")
        channel_id=channelid_res[0][0]

        self.wellname=wellname
        self.well_id=well_id
        self.site=site
        self.site_x=site_x
        self.site_y=site_y
        self.site_z=site_z
        # TODO site_t is based off of filepath, not just the name (if experiment.grid_config.t.N>1, then site_t is the name of the parent folder)
        self.site_t=1
        self.channelname=channelname
        self.channel_id=channel_id
        if coords is not None:
            self.coord_x_mm=coords["x_mm"]
            self.coord_y_mm=coords["y_mm"]
            self.coord_z_um=coords["z_um"]
            self.coord_t=coords["t"]
        else:
            self.coord_x_mm=None
            self.coord_y_mm=None
            self.coord_z_um=None
            self.coord_t=None


class DB:
    @staticmethod
    def ensureDatabase(dbname:str,conn:db.connections.Connection):
        print("---- dropping database")
        conn.execute(text(f"DROP DATABASE IF EXISTS {dbname};"))
        conn.execute(text(f"CREATE DATABASE IF NOT EXISTS {dbname};"))
        conn.execute(text(f"USE {dbname};"))
        conn.commit()
        print(f"dropped and created database {dbname}")

    def dbExec(self,
        query:tp.Union[str,Insert,Select],
        *args,
        as_pd:bool=False,
    )->tp.Optional[tp.Union[tp.List[tp.Tuple],pd.DataFrame]]:
        """
            execute a query and return the results

            :param query: the query to execute
            :param as_pd: if True, return the result as a pandas dataframe
        """
        if isinstance(query,str):
            query=text(query)

        res=self.conn.execute(query,*args)
        self.conn.commit()

        # for select statements, return all rows
        if res.returns_rows:
            if as_pd:
                return pd.DataFrame(res.fetchall(),columns=res.keys())
            return list(res)
        
        # for insert statements, return the primary key of the inserted row(s)
        if res.is_insert:
            if res.rowcount==1:
                key_list=[res.inserted_primary_key]
            else:
                key_list=res.inserted_primary_key_rows

            if as_pd:
                return pd.DataFrame(key_list,columns=res.keys())
            else:
                return key_list
        
        # for other cases, return whatever the result is
        return res
    
    def insertStaticData(self):
        """ insert some static data, e.g. imaging channels, plate types, wells for each plate type """

        # static imaging channel dataset
        res=self.dbExec(dbImagingChannels.insert(),[
            {"name":"BF full","is_brightfield":True,"is_fluorescence":False,"brightfield_type":"full"},
            {"name":"BF right half","is_brightfield":True,"is_fluorescence":False,"brightfield_type":"right half"},
            {"name":"BF left half","is_brightfield":True,"is_fluorescence":False,"brightfield_type":"left half"},
        ])
        res=self.dbExec(dbImagingChannels.insert(),[
            {"name":"Fluorescence 405 nm Ex","is_brightfield":False,"is_fluorescence":True,"fluorescence_wavelength_nm":405},
            {"name":"Fluorescence 488 nm Ex","is_brightfield":False,"is_fluorescence":True,"fluorescence_wavelength_nm":488},
            {"name":"Fluorescence 560 nm Ex","is_brightfield":False,"is_fluorescence":True,"fluorescence_wavelength_nm":560},
            {"name":"Fluorescence 638 nm Ex","is_brightfield":False,"is_fluorescence":True,"fluorescence_wavelength_nm":638},
            {"name":"Fluorescence 730 nm Ex","is_brightfield":False,"is_fluorescence":True,"fluorescence_wavelength_nm":730},
        ])

        # plate_type information
        res=self.dbExec(dbPlateTypes.insert(),[
            {"model_name":"96-CO-3603","num_wells":96,"manufacturer":"Corning","brand":"Costar","other_info":"96 well plate with a flat bottom"},
        ])

        # for each plate type, insert the wells
        # so: query the plate type id and the number of wells, then insert wells accordingly (can be somewhat hardcoded knowing that a plate has either 96 or 384 wells)
        res=self.dbExec("select id,num_wells from plate_types;")
        for platetypeid,num_wells in res:
            plate_well_list=[]
            if num_wells==96:
                num_rows=8
                num_cols=12
            elif num_wells==384:
                num_rows=16
                num_cols=24
            else:
                raise ValueError(f"unknown number of wells: {num_wells}")
            
            for row in range(num_rows):
                for col in range(num_cols):
                    wellname=f"{chr(65+row)}{col+1:02d}"
                    plate_well_list.append({"platetypeid":platetypeid,"well_name":wellname})

            self.dbExec(dbPlateTypeWells.insert(),plate_well_list)

        # checking that data was inserted correctly
        data = self.dbExec("select * from imaging_channels;", as_pd=True)
        print("imaging_channels",data,sep="\n")
        data = self.dbExec("select * from platetype_wells;", as_pd=True)
        print("platetype_wells",data,sep="\n")

    def __init__(self):
        self.celery = Celery('dbwatcher', backend='rpc://', broker=os.getenv('APP_BROKER_URI'), broker_pool_limit = 0)

        dbengine=create_engine(f"mariadb+mariadbconnector://{os.getenv('MARIADB_USER_USERNAME')}:{os.getenv('MARIADB_USER_PASSWORD')}@{os.getenv('MARIADB_HOSTNAME')}:{os.getenv('MARIADB_PORT')}")
        conn=dbengine.connect()

        DB.ensureDatabase(dbname=DBNAME,conn=conn)

        conn.close()

        dbengine=create_engine(f"mariadb+mariadbconnector://{os.getenv('MARIADB_USER_USERNAME')}:{os.getenv('MARIADB_USER_PASSWORD')}@{os.getenv('MARIADB_HOSTNAME')}:{os.getenv('MARIADB_PORT')}/{DBNAME}")
        conn=dbengine.connect()

        self.conn=conn

        # creates the tables, if they dont exist already
        dbmetadata.create_all(dbengine)

        self.insertStaticData()
        print("-- db init done")

    def checkTasksRegistered(self,tasks:tp.List[str]=None):
        registered_tasks=None
        try:
            inspector=self.celery.control.inspect()
            registered_tasks=inspector.registered()
        except:
            pass
        finally:
            if registered_tasks is None:
                return False

        if tasks is None:
            return True

        task_names=set(tasks)

        registered_task_names=[]
        for tl in registered_tasks.values():
            registered_task_names.extend(tl)
        registered_task_names=set(registered_task_names)
        
        return registered_task_names.issuperset(task_names)

    def insertExperimentMetadata(self,
        experiment:dict,
        coordinates:pd.DataFrame,
        images:tp.List[FileStorage],
        image_s3_bucketname:str,
    )->tp.List[ImageMetadata]:

        # create experiment if none exists with the name
        proj_name:str=experiment["project_name"]
        res=self.dbExec(f"select id from projects where name='{proj_name}';")
        if len(res)==0:
            self.dbExec(dbProjects.insert(),{"name":proj_name})
        proj_id:int=self.dbExec(f"select id from projects where name='{proj_name}';")[0][0]

        # get plate type id, throw if it does not exist
        plate_type_name:str=experiment["plate_type"]
        res=self.dbExec(f"select id from plate_types where model_name='{plate_type_name}';")
        if len(res)==0:
            raise ValueError(f"plate type {plate_type_name} not found in database")
        plate_type_id:int=res[0][0]

        # create plate if none exists with the barcode
        plate_name:str=experiment["plate_name"]
        res=self.dbExec(f"select id from plates where barcode='{plate_name}';")
        if len(res)==0:
            self.dbExec(dbPlates.insert().prefix_with("ignore"),{"projectid":proj_id,"platetypeid":plate_type_id,"barcode":plate_name})
        plate_id:int=self.dbExec(f"select id from plates where barcode='{plate_name}';")[0][0]

        # get microscope id, and create it if it does not exist
        microscope_name:str=experiment["microscope_name"]
        res=self.dbExec(f"select id from microscopes where name='{microscope_name}';")
        if len(res)==0:
            self.dbExec(dbMicroscopes.insert(),{"name":microscope_name})
        microscope_id:int=self.dbExec(f"select id from microscopes where name='{microscope_name}';")[0][0]

        # get objective id, and create it if it does not exist
        objective_name:str=experiment["objective"]
        res=self.dbExec(f"select id from objectives where name='{objective_name}';")
        if len(res)==0:
            self.dbExec(dbObjectives.insert(),{"name":objective_name})
        objective_id:int=self.dbExec(f"select id from objectives where name='{objective_name}';")[0][0]

        # create experiment
        exp_name=experiment["experiment_name"]
        exp_start_time=datetime.strptime(experiment["timestamp"], "%Y-%m-%d_%H.%M.%S")

        # ideally this code would detect the unit and then convert to the correct unit for the database
        # but it is hardcoded because i have to draw the line for this project somewhere
        grid_delta_x_unit:str=experiment["grid_config"]["x"]["unit"]
        assert grid_delta_x_unit=="mm", grid_delta_x_unit
        grid_delta_y_unit:str=experiment["grid_config"]["y"]["unit"]
        assert grid_delta_y_unit=="mm", grid_delta_y_unit
        grid_delta_z_unit:str=experiment["grid_config"]["z"]["unit"]
        assert grid_delta_z_unit=="mm", grid_delta_z_unit
        grid_delta_t_unit:str=experiment["grid_config"]["t"]["unit"]
        assert grid_delta_t_unit=="s", grid_delta_t_unit

        res=self.dbExec(dbExperiments.insert(),{
            "projectid":proj_id,
            "name":exp_name,
            "start_time":exp_start_time,
            # descriptions are currently not implemented in metadata files
            # but would be nice to have at some point
            "description":None,
            "plateid":plate_id,
            "microscopeid":microscope_id,
            "objectiveid":objective_id,
            "num_images_x":experiment["grid_config"]["x"]["N"],
            "num_images_y":experiment["grid_config"]["y"]["N"],
            "num_images_z":experiment["grid_config"]["z"]["N"],
            "num_images_t":experiment["grid_config"]["t"]["N"],
            "delta_x_mm":experiment["grid_config"]["x"]["d"],
            "delta_y_mm":experiment["grid_config"]["y"]["d"],
            # convert from mm (in metadata file) to um (in database)
            "delta_z_um":experiment["grid_config"]["z"]["d"]*1e3,
            # convert from seconds (in metadata file) to hours (in database)
            "delta_t_h":experiment["grid_config"]["t"]["d"]/3600,
        })
        exp_id:int=res[0][0]

        cell_line:str=experiment["cell_line"]

        # create experiment wells
        for wellname in experiment["well_list"]:
            res=self.dbExec(f"select id from platetype_wells where well_name='{wellname}' and platetypeid={plate_type_id};")
            if len(res)==0:
                raise ValueError(f"well name {wellname} not found in database")
            well_id=res[0][0]

            self.dbExec(dbExperimentWells.insert(),{"experimentid":exp_id,"wellid":well_id,"cell_line":cell_line})

        imaging_channel_ids={}

        # create experiment imaging channels
        for channel_imaging_order_index,channel in enumerate(experiment["channels_ordered"]):
            current_channel_config=None
            for channel_config in experiment["channels_config"]:
                if channel_config["Name"]==channel:
                    current_channel_config=channel_config
                    break
            assert current_channel_config is not None, f"channel {channel} not found in experiment config"

            # get channel id
            res=self.dbExec(f"select id from imaging_channels where name='{channel}';")
            if len(res)==0:
                raise ValueError(f"channel name {channel} not found in database")
            
            channel_id:int=res[0][0]

            res=self.dbExec(dbExperimentImagingChannels.insert(),{
                "experimentid":exp_id,
                "channelid":channel_id,
                "imaging_order_index":channel_imaging_order_index,
                "exposure_time_ms":current_channel_config["ExposureTime"],
                "analog_gain":current_channel_config["AnalogGain"],
                "illumination_strength":current_channel_config["IlluminationIntensity"],
            })
            experiment_channel_id=res[0][0]

            imaging_channel_ids[channel]=experiment_channel_id

        print(f"{plate_id = }")

        # insert image metadata
        imageInsertionList=[]
        imageMetadataList=[]
        for file in images:
            # If the user does not select a file, the browser may submit an empty file without a filename.
            if file.filename == '':
                raise ValueError("No selected file (empty filename)")

            sec_filename = secure_filename(Path(file.filename).name)
            save_filepath=os.path.join(experiment["project_name"],experiment["experiment_name"],sec_filename)

            savePathInclBucket=f"{image_s3_bucketname}/{save_filepath}"
            
            # forward file to s3 storage
            success=upload_file_to_s3_localstack(file,image_s3_bucketname,save_filepath)
            assert success, f"uploading {save_filepath} to bucket {image_s3_bucketname} failed with {success}"

            image_metadata=ImageMetadata(
                real_filename=file.filename,
                storage_filename=savePathInclBucket,
                coords=None,
                db=self
            )
            imageMetadataList.append(image_metadata)

            imageInsertionList.append({
                "plateid":plate_id,
                "s3path":savePathInclBucket,
                "wellid":image_metadata.well_id,
                "site_x":image_metadata.site_x,
                "site_y":image_metadata.site_y,
                "site_z":image_metadata.site_z,
                "site_t":image_metadata.site_t,
                "experimentchannelid":imaging_channel_ids[image_metadata.channelname],
                "coord_x_mm":image_metadata.coord_x_mm,
                "coord_y_mm":image_metadata.coord_y_mm,
                "coord_z_um":image_metadata.coord_z_um,
                "coord_t":image_metadata.coord_t,
            })

        print(f"inserting {len(imageMetadataList)} images")
        self.dbExec(dbImages.insert(),imageInsertionList)
        
        self.dumpDatabaseHead()

        return imageMetadataList


    def dumpDatabaseHead(self,n:int=5):
        # print forst 5 rows of each table, if they exist
        for table in dbmetadata.tables.values():
            res=self.dbExec(f"select * from {table.name} limit {n};",as_pd=True)
            if res is not None:
                print(table.name,res,sep="\n")

