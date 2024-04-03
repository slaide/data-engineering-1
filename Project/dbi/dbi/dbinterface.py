import os, typing as tp

from .objectstorage import S3Client

from werkzeug.datastructures import FileStorage
from celery import Celery
import pandas as pd
from sqlalchemy import create_engine, text, Table, Column, Integer, Text, MetaData, ForeignKey, Boolean, Float, DateTime
from sqlalchemy.sql.expression import Insert, Select
from sqlalchemy.engine.base import Connection
from datetime import datetime
from dataclasses import dataclass
import pydantic as pyd
from pathlib import Path
import re
from io import BytesIO
from werkzeug.utils import secure_filename

# setting limits to none to display all columns
pd.set_option('display.width', None)
pd.set_option('display.max_columns', None)
pd.set_option('display.max_colwidth', None)

MARIADB_USER_USERNAME=os.getenv('MARIADB_USER_USERNAME') ; assert MARIADB_USER_USERNAME is not None
MARIADB_USER_PASSWORD=os.getenv('MARIADB_USER_PASSWORD') or '' # password may be empty
MARIADB_HOSTNAME=os.getenv('MARIADB_HOSTNAME') ; assert MARIADB_HOSTNAME is not None
_MARIADB_PORT_ENV=os.getenv('MARIADB_PORT') ; assert _MARIADB_PORT_ENV is not None
MARIADB_PORT=int(_MARIADB_PORT_ENV)

DBNAME="morphology_information"

# deconstruct image naming scheme to get image metadata...
@dataclass(repr=True,frozen=False)
class ImageMetadata:
    storage_filename:str
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
    coord_x_mm:tp.Optional[float]
    coord_y_mm:tp.Optional[float]
    coord_z_um:tp.Optional[float]
    coord_t:tp.Optional[datetime]
    
    def __init__(self,
        real_filename:str,
        storage_filename:str,
        coords:tp.Optional[tp.Dict[str,tp.Any]],
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
        assert type(wellid_res)==list
        if len(wellid_res)==0:
            raise ValueError(f"well name {wellname} not found in database")
        well_id=wellid_res[0][0]

        # query channel id from database based on channel name
        channelid_res=db.dbExec(f"select id from imaging_channels where name='{channelname}';")
        assert type(channelid_res)==list
        if len(channelid_res)==0:
            available_channels=db.dbExec("select * from imaging_channels;",as_pd=True)
            assert type(available_channels)==pd.DataFrame
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

class ObjectStorageFileReference(pyd.BaseModel):
    s3path:str
    """ the path to the file in the object storage (excluding bucket) """
    filename:str
    """ the filename of the file in local storage (includes suffix, but not the full path) """

class WellSite(pyd.BaseModel):
    well:str
    """ name of the well """
    site:int
    """ site (1-indexed) """

class Result_cp_map(pyd.BaseModel):
    resultfiles:tp.List[ObjectStorageFileReference]
    wells:tp.List[WellSite]

class Result_getExperiments(pyd.BaseModel):
    experiments:tp.List[str]

class Result_getProjectNames(pyd.BaseModel):
    projects:tp.List[str]

class Result_processingStatus(pyd.BaseModel):
    class Config:
        arbitrary_types_allowed=True

    wells:tp.Dict[str,tp.Dict[int,int]]
    resultfiles:tp.Dict[str,pd.DataFrame]
    total_sites:int
    num_processed_sites:int

class DB:
    @staticmethod
    def createDatabase(
        dbname:str,
        conn:Connection,
    ):
        transaction=conn.begin()
        assert transaction is not None
        with transaction:
            conn.execute(text(f"DROP DATABASE IF EXISTS {dbname};"))
            conn.execute(text(f"CREATE DATABASE IF NOT EXISTS {dbname};"))
            conn.execute(text(f"USE {dbname};"))

    def __init__(self,recreate:bool=True):
        self.celery_rpc = Celery('dbwatcher', backend='rpc://', broker=os.getenv('APP_BROKER_URI'), broker_pool_limit = 0)
        """ 'request/response, i.e. rpc' task queue """
        self.celery_tasks = Celery('dbwatcher', broker=os.getenv('APP_BROKER_URI'), broker_pool_limit = 0)
        """ 'fire and forget' task queue """

        # create python table objects
        if True:
            self.dbmetadata=MetaData(schema=DBNAME)

            self.dbProjects:Table=Table(
                "projects", self.dbmetadata,
                Column("id",Integer,primary_key=True,nullable=False,autoincrement=True,unique=True),
                Column("name",Text,unique=True,nullable=False),
            )
            """
                contains all projects

                a project must have a name

                a project can have any number of experiments
            """

            self.dbPlates:Table=Table(
                "plates", self.dbmetadata,
                Column("id",Integer,primary_key=True,nullable=False,autoincrement=True,unique=True),
                Column("platetypeid",Integer,ForeignKey("plate_types.id",ondelete="cascade"),nullable=False),
                Column("barcode",Text,nullable=False),
            )
            """
                all physical plates (actual plates with stuff on them, used for experiments at some point)

                a plate must have a barcode

                a plate can be part of any number of experiments
            """

            self.dbExperiments:Table=Table(
                "experiments", self.dbmetadata,
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
            """
                all experiments

                an experiment must:
                - belong to exactly one project
                - have a name
                - use exactly one plate

                an experiment can:
                - have a description

                there is also some meta information here, such as:
                - the microscope used
                - the objective used
                - the number of images per well in x/y/z/t, and the respective distances

                more information about an experiment is stored in other places:
                - the list of wells that were imaged (see self.dbExperimentWells)
                - the list of imaging channels (see self.dbExperimentImagingChannels)

            """

            self.dbPlateTypes:Table=Table(
                "plate_types", self.dbmetadata,
                Column("id",Integer,primary_key=True,nullable=False,autoincrement=True,unique=True),
                Column("model_name",Text,nullable=False),
                Column("manufacturer",Text,nullable=False),
                Column("brand",Text,nullable=False),
                Column("num_wells",Integer,nullable=False),
                Column("other_info",Text,nullable=True),
            )
            """
                a list of all plate types, each containing information about:
                - manufacturer
                - brandname
                - total number of wells
                - optional additional information
            """

            self.dbPlateTypeWells:Table=Table(
                "platetype_wells", self.dbmetadata,
                Column("id",Integer,primary_key=True,nullable=False,autoincrement=True,unique=True),
                Column("platetypeid",Integer,ForeignKey("plate_types.id",ondelete="cascade"),nullable=False),
                Column("well_name",Text,nullable=False),
            )
            """
                all wells for each plate type

                # i.e. each row contains a reference to a plate type and
                the name of one well on that plate type
            """

            self.dbMicroscopes:Table=Table(
                "microscopes", self.dbmetadata,
                Column("id",Integer,primary_key=True,nullable=False,autoincrement=True,unique=True),
                Column("name",Text,nullable=False,unique=True),
            )
            """
                a list of all microscopes
                each microscope has a unique name
            """

            self.dbObjectives:Table=Table(
                "objectives", self.dbmetadata,
                Column("id",Integer,primary_key=True,nullable=False,autoincrement=True,unique=True),
                Column("name",Text,nullable=False),
            )
            """
                a list of all available objectives
            """

            self.dbExperimentWells:Table=Table(
                "experiment_wells", self.dbmetadata,
                Column("id",Integer,primary_key=True,nullable=False,autoincrement=True,unique=True),
                Column("experimentid",Integer,ForeignKey("experiments.id",ondelete="cascade"),nullable=False),
                Column("wellid",Integer,ForeignKey("platetype_wells.id",ondelete="cascade"),nullable=False),
                Column("cell_line",Text,nullable=True),
            )
            """
                for an experiment, contains information about a well imaged in that experiment

                i.e. this table indicates which wells are imaged in an experiment

                note: it is not forbidden to have multiple entries for a well, but
                it is not recommended
            """

            self.dbExperimentWellSites:Table=Table(
                "experiment_well_sites", self.dbmetadata,
                Column("id",Integer,primary_key=True,nullable=False,autoincrement=True,unique=True),
                Column("experiment_wellid",Integer,ForeignKey("experiment_wells.id",ondelete="cascade"),nullable=False),
                # site_id within well! e.g. 1-4 with 4 sites per well in xyzt combined
                Column("site_id",Integer,nullable=False),
                Column("site_x",Integer,nullable=True),
                Column("site_y",Integer,nullable=True),
                Column("site_z",Integer,nullable=True),
                Column("site_t",Integer,nullable=True),
            )
            """
            for a well imaged in an experiment, contains information about a site within that well

            i.e. this table may contain multiple site information entries for the same well
            """

            self.dbExperimentImagingChannels:Table=Table(
                "experiment_imaging_channels", self.dbmetadata,
                Column("id",Integer,primary_key=True,nullable=False,autoincrement=True,unique=True),
                Column("experimentid",Integer,ForeignKey("experiments.id",ondelete="cascade"),nullable=False),
                Column("channelid",Integer,ForeignKey("imaging_channels.id",ondelete="cascade"),nullable=False),
                Column("imaging_order_index",Integer,nullable=False),
                Column("exposure_time_ms",Float,nullable=False),
                Column("analog_gain",Float,nullable=False),
                Column("illumination_strength",Float,nullable=False),
            )
            """
                this table contains a list of all imaging channels used in an experiment
                
                each row contains a reference to
                - an experiment
                - an imaging channel, including imaging channel specific information (in another table!), like
                    - exposure time
                    - analog gain
                    - illumination strength
            """

            self.dbImagingChannels:Table=Table(
                "imaging_channels", self.dbmetadata,
                Column("id",Integer,primary_key=True,nullable=False,autoincrement=True,unique=True),
                Column("is_fluorescence",Boolean,nullable=False),
                Column("fluorescence_wavelength_nm",Integer,nullable=True),
                Column("is_brightfield",Boolean,nullable=False),
                Column("brightfield_type",Text,nullable=True),
                Column("name",Text,nullable=True),
            )
            """
                contains a list of all the available imaging channels
                mostly for legacy reasons, each channel also has a human readable name
            """
            
            self.dbImages:Table=Table(
                "images", self.dbmetadata,
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
            """
                this table contains a list of all images taken.

                each image belongs to one plate in one experiment

                each image is taken:
                - at a specific site (index in x/y/z/t) 
                - in a specific channel
                - at specific coordinates (physically, on the plate)

                and the image is stored in a specific location on the object storage (s3path)
            """

            self.dbProfileResults:Table=Table(
                "profile_results", self.dbmetadata,
                Column("id",Integer,primary_key=True,nullable=False,autoincrement=True,unique=True),
                Column("experimentid",Integer,ForeignKey("experiments.id",ondelete="cascade"),nullable=False),
                Column("batchid",Integer,nullable=False),
                Column("start_time",DateTime,nullable=True),
                Column("end_time",DateTime,nullable=True),
                Column("status",Text,nullable=True),
            )
            """
                contains information about a processing batch

                processing batch = n sets of c images, where n>=1 and c is the number of imaging channels
            """

            self.dbProfileResultBatchSites:Table=Table(
                "profile_result_batch_sites", self.dbmetadata,
                Column("id",Integer,primary_key=True,nullable=False,autoincrement=True,unique=True),
                Column("profile_resultid",Integer,ForeignKey("profile_results.id",ondelete="cascade"),nullable=False),
                Column("siteid",Integer,ForeignKey("experiment_well_sites.id",ondelete="cascade"),nullable=False),
            )
            """
                contains information about the sites that were processed in a batch
            """

            self.dbProfileResultFiles:Table=Table(
                "profile_result_files", self.dbmetadata,
                Column("id",Integer,primary_key=True,nullable=False,autoincrement=True,unique=True),
                Column("profile_resultid",Integer,ForeignKey("profile_results.id",ondelete="cascade"),nullable=False),
                Column("s3path",Text,nullable=False),
                Column("filename",Text,nullable=True),
            )
            """
                contains information about the files that were generated in a processing batch
            """

        if recreate:
            dbengine=create_engine(f"mariadb+mariadbconnector://{MARIADB_USER_USERNAME}:{MARIADB_USER_PASSWORD}@{MARIADB_HOSTNAME}:{MARIADB_PORT}")
            conn=dbengine.connect()
            assert type(conn)==Connection

            DB.createDatabase(dbname=DBNAME,conn=conn)

            # creates the tables, if they dont exist already
            self.dbmetadata.create_all(dbengine)

            self.conn=conn
            self.insertStaticData()

            conn.close()

            print("-- db init done")

        dbengine=create_engine(f"mariadb+mariadbconnector://{MARIADB_USER_USERNAME}:{MARIADB_USER_PASSWORD}@{MARIADB_HOSTNAME}:{MARIADB_PORT}/{DBNAME}")
        conn=dbengine.connect()
        assert type(conn)==Connection

        self.conn=conn

        self.s3client=S3Client()

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

        # commit changes to the database
        transaction=self.conn.begin()
        assert transaction is not None
        res=None
        with transaction:
            res=self.conn.execute(query,*args)
        assert res is not None

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
        res=self.dbExec(self.dbImagingChannels.insert(),[
            {"name":"BF full","is_brightfield":True,"is_fluorescence":False,"brightfield_type":"full"},
            {"name":"BF right half","is_brightfield":True,"is_fluorescence":False,"brightfield_type":"right half"},
            {"name":"BF left half","is_brightfield":True,"is_fluorescence":False,"brightfield_type":"left half"},
        ])
        res=self.dbExec(self.dbImagingChannels.insert(),[
            {"name":"Fluorescence 405 nm Ex","is_brightfield":False,"is_fluorescence":True,"fluorescence_wavelength_nm":405},
            {"name":"Fluorescence 488 nm Ex","is_brightfield":False,"is_fluorescence":True,"fluorescence_wavelength_nm":488},
            {"name":"Fluorescence 560 nm Ex","is_brightfield":False,"is_fluorescence":True,"fluorescence_wavelength_nm":560},
            {"name":"Fluorescence 638 nm Ex","is_brightfield":False,"is_fluorescence":True,"fluorescence_wavelength_nm":638},
            {"name":"Fluorescence 730 nm Ex","is_brightfield":False,"is_fluorescence":True,"fluorescence_wavelength_nm":730},
        ])

        # plate_type information
        res=self.dbExec(self.dbPlateTypes.insert(),[
            {"model_name":"96-CO-3603","num_wells":96,"manufacturer":"Corning","brand":"Costar","other_info":"96 well plate with a flat bottom"},
        ])

        # for each plate type, insert the wells
        # so: query the plate type id and the number of wells, then insert wells accordingly (can be somewhat hardcoded knowing that a plate has either 96 or 384 wells)
        res=self.dbExec("select id,num_wells from plate_types;")
        assert type(res)==list
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

            self.dbExec(self.dbPlateTypeWells.insert(),plate_well_list)

        self.dumpDatabaseHead()

    def insertExperimentMetadata(self,
        experiment:dict,
        coordinates:pd.DataFrame,
        images:tp.List[FileStorage],
        image_s3_bucketname:str,
    )->tp.List[ImageMetadata]:

        # create experiment if none exists with the name
        proj_name:str=experiment["project_name"]
        res=self.dbExec(f"select id from projects where name='{proj_name}';")
        assert type(res)==list
        if len(res)==0:
            self.dbExec(self.dbProjects.insert(),{"name":proj_name})
        res=self.dbExec(f"select id from projects where name='{proj_name}';")
        assert type(res)==list
        proj_id:int=res[0][0]

        # get plate type id, throw if it does not exist
        plate_type_name:str=experiment["plate_type"]
        res=self.dbExec(f"select id from plate_types where model_name='{plate_type_name}';")
        assert type(res)==list
        if len(res)==0:
            raise ValueError(f"plate type {plate_type_name} not found in database")
        plate_type_id:int=res[0][0]

        # create plate if none exists with the barcode
        plate_name:str=experiment["plate_name"]
        res=self.dbExec(f"select id from plates where barcode='{plate_name}';")
        assert type(res)==list
        if len(res)==0:
            self.dbExec(self.dbPlates.insert().prefix_with("ignore"),{"projectid":proj_id,"platetypeid":plate_type_id,"barcode":plate_name})
        res=self.dbExec(f"select id from plates where barcode='{plate_name}';")
        assert type(res)==list
        plate_id:int=res[0][0]

        # get microscope id, and create it if it does not exist
        microscope_name:str=experiment["microscope_name"]
        res=self.dbExec(f"select id from microscopes where name='{microscope_name}';")
        assert type(res)==list
        if len(res)==0:
            self.dbExec(self.dbMicroscopes.insert(),{"name":microscope_name})
        res=self.dbExec(f"select id from microscopes where name='{microscope_name}';")
        assert type(res)==list
        microscope_id:int=res[0][0]

        # get objective id, and create it if it does not exist
        objective_name:str=experiment["objective"]
        res=self.dbExec(f"select id from objectives where name='{objective_name}';")
        assert type(res)==list
        if len(res)==0:
            self.dbExec(self.dbObjectives.insert(),{"name":objective_name})
        res=self.dbExec(f"select id from objectives where name='{objective_name}';")
        assert type(res)==list
        objective_id:int=res[0][0]

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

        grid_num_x=int(experiment["grid_config"]["x"]["N"])
        grid_num_y=int(experiment["grid_config"]["y"]["N"])
        grid_num_z=int(experiment["grid_config"]["z"]["N"])
        grid_num_t=int(experiment["grid_config"]["t"]["N"])

        res=self.dbExec(self.dbExperiments.insert(),{
            "projectid":proj_id,
            "name":exp_name,
            "start_time":exp_start_time,
            # descriptions are currently not implemented in metadata files
            # but would be nice to have at some point
            "description":None,
            "plateid":plate_id,
            "microscopeid":microscope_id,
            "objectiveid":objective_id,
            "num_images_x":grid_num_x,
            "num_images_y":grid_num_y,
            "num_images_z":grid_num_z,
            "num_images_t":grid_num_t,
            "delta_x_mm":experiment["grid_config"]["x"]["d"],
            "delta_y_mm":experiment["grid_config"]["y"]["d"],
            # convert from mm (in metadata file) to um (in database)
            "delta_z_um":experiment["grid_config"]["z"]["d"]*1e3,
            # convert from seconds (in metadata file) to hours (in database)
            "delta_t_h":experiment["grid_config"]["t"]["d"]/3600,
        })
        assert type(res)==list
        exp_id:int=res[0][0]

        cell_line:str=experiment["cell_line"]

        # create experiment wells
        for wellname in experiment["well_list"]:
            res=self.dbExec(f"select id from platetype_wells where well_name='{wellname}' and platetypeid={plate_type_id};")
            assert type(res)==list
            if len(res)==0:
                raise ValueError(f"well name {wellname} not found in database")
            well_id=res[0][0]

            self.dbExec(self.dbExperimentWells.insert(),{"experimentid":exp_id,"wellid":well_id,"cell_line":cell_line})

            num_sites=grid_num_x*grid_num_y*grid_num_z

            for site_id in range(num_sites):
                site_id+=1 # sites are 1-indexed
                site_x=site_id%grid_num_x
                site_y=(site_id//grid_num_x)%grid_num_y
                site_z=(site_id//(grid_num_x*grid_num_y))%grid_num_z
                site_t=(site_id//(grid_num_x*grid_num_y*grid_num_z))%grid_num_t

                self.dbExec(self.dbExperimentWellSites.insert(),{
                    "experiment_wellid":exp_id,
                    "site_id":site_id,
                    "site_x":site_x,
                    "site_y":site_y,
                    "site_z":site_z,
                    "site_t":site_t,
                })

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
            assert type(res)==list
            if len(res)==0:
                raise ValueError(f"channel name {channel} not found in database")
            
            channel_id:int=res[0][0]

            res=self.dbExec(self.dbExperimentImagingChannels.insert(),{
                "experimentid":exp_id,
                "channelid":channel_id,
                "imaging_order_index":channel_imaging_order_index,
                "exposure_time_ms":current_channel_config["ExposureTime"],
                "analog_gain":current_channel_config["AnalogGain"],
                "illumination_strength":current_channel_config["IlluminationIntensity"],
            })
            assert type(res)==list
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

            filename=str(file.filename)
            sec_filename = secure_filename(Path(filename).name)
            save_filepath=os.path.join(experiment["project_name"],experiment["experiment_name"],sec_filename)

            savePathInclBucket=f"{image_s3_bucketname}/{save_filepath}"
            
            # forward file to s3 storage
            success=self.s3client.uploadFile(file,save_filepath,bucket_override=image_s3_bucketname)
            assert success, f"uploading {save_filepath} to bucket {image_s3_bucketname} failed with {success}"

            image_metadata=ImageMetadata(
                real_filename=filename,
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
        self.dbExec(self.dbImages.insert(),imageInsertionList)
        
        self.dumpDatabaseHead()

        return imageMetadataList
    
    def insertProfileResultBatch(self,
        project_name:str,
        experiment_name:str,
        batchid:int,
        result_file_paths:tp.List[ObjectStorageFileReference],
        well_site_list:tp.List[WellSite]
    ):
        
        self.dumpDatabaseHead()

        res=self.dbExec(f"select id from projects where name='{project_name}';")
        assert type(res)==list
        project_id=res[0][0]
        print(f"{project_id = }")
        res=self.dbExec(f"select id from experiments where projectid={project_id} and name='{experiment_name}';")
        assert type(res)==list
        experiment_id=res[0][0]

        # create profile result batch
        res=self.dbExec(self.dbProfileResults.insert(),{
            "experimentid":experiment_id,
            "batchid":batchid,
        })
        assert type(res)==list
        profile_result_id=res[0][0]

        # write result file paths
        for result_file in result_file_paths:
            self.dbExec(self.dbProfileResultFiles.insert(),{
                "profile_resultid":profile_result_id,
                "s3path":result_file.s3path,
                "filename":result_file.filename,
            })

        # write well list
        for well_info in well_site_list:
            well,site=well_info.well,well_info.site

            # get experiment well site id by joining some tables
            res=self.dbExec(f"""
                select ews.id from experiment_wells ew
                join platetype_wells pw on ew.wellid=pw.id
                join experiments e on ew.experimentid=e.id
                join experiment_well_sites ews on ew.id=ews.experiment_wellid
                where pw.well_name='{well}' and e.id={experiment_id} and ews.site_id={site};
                """)
            assert type(res)==list
            if len(res)==0:
                raise ValueError(f"well {well} site {site} not found in database")
            well_site_id=res[0][0]

            self.dbExec(self.dbProfileResultBatchSites.insert(),{
                "profile_resultid":profile_result_id,
                "siteid":well_site_id,
            })

    def checkExperimentProcessingStatus(self,
        project_name:str,
        experiment_name:str,
        get_merged_frames:bool=False,
    )->Result_processingStatus:
        
        res=self.dbExec(f"select id from projects where name='{project_name}';")
        assert type(res)==list
        if len(res)==0:
            raise ValueError(f"project {project_name} not found in database")
        project_id=res[0][0]
        res=self.dbExec(f"select id from experiments where projectid={project_id} and name='{experiment_name}';")
        assert type(res)==list
        if len(res)==0:
            raise ValueError(f"experiment {experiment_name} not found in database")
        experiment_id=res[0][0]

        # get list of wells and number of sites per well from experiment definition
        experiment=self.dbExec(f"""
            select e.num_images_x,e.num_images_y,e.num_images_z,e.num_images_t
            from experiments e
            where e.id={experiment_id};
        """,as_pd=True)
        assert type(experiment)==pd.DataFrame
        assert len(experiment)==1, f"experiment {experiment_name} not found in database"
        experiment=experiment.iloc[0]
        num_sites=experiment["num_images_x"]*experiment["num_images_y"]*experiment["num_images_z"]*experiment["num_images_t"]
        assert num_sites>0, f"no sites found for experiment {experiment_name}"

        # get list of well names from experiment_wells, using experimentid and wellid
        wells=self.dbExec(f"""
            select pw.well_name from experiment_wells ew
            join platetype_wells pw on ew.wellid=pw.id
            where ew.experimentid={experiment_id};
        """,as_pd=True)
        assert type(wells)==pd.DataFrame
        assert len(wells)>0, f"no wells found for experiment {experiment_name}"
        wells=wells["well_name"].tolist()

        res=Result_processingStatus(
            wells={
                well:{(s_i+1):0 for s_i in range(num_sites)}
                for well
                in wells
            },
            resultfiles={},
            total_sites=0,
            num_processed_sites=0,
        )

        # check if all sites in all wells have been processed
        batches=self.dbExec(f"select batchid from profile_results where experimentid={experiment_id};")
        assert type(batches)==list
        for batch in batches:
            batch_id=batch[0]
            
            # get list of well+site that have been processed for this batch
            # also get the file locations of the results
            processed_sites=self.dbExec(f"""
                select pw.well_name,ews.site_id,prf.s3path,prf.filename
                from profile_result_batch_sites prbs
                join experiment_well_sites ews
                    on prbs.siteid=ews.id
                join experiment_wells ew
                    on ews.experiment_wellid=ew.id
                join platetype_wells pw 
                    on ew.wellid=pw.id
                join profile_results pr
                    on prbs.profile_resultid=pr.id
                join profile_result_files prf 
                    on prf.profile_resultid=pr.id
                where prbs.profile_resultid = (
                    select id
                    from profile_results
                    where experimentid={experiment_id}
                        and batchid={batch_id}
                );
            """,as_pd=True)
            assert type(processed_sites)==pd.DataFrame

            for _rowindex,row in processed_sites.iterrows():
                well=row["well_name"]
                site=row["site_id"]
                s3path=row["s3path"]
                filename=row["filename"]
                res.wells[well][site]=1

                if get_merged_frames:
                    file=BytesIO()

                    # get merged frames from s3 as in-memory file
                    self.s3client.downloadFileObj(object_name=s3path,fileobj=file)
                    file.seek(0)

                    # read file into memory dataframe with pandas, based on file ending .csv/.parquet
                    if filename.endswith(".csv"):
                        df=pd.read_csv(file)
                    elif filename.endswith(".parquet"):
                        df=pd.read_parquet(file)
                    else:
                        raise ValueError(f"unknown file ending for {filename}")
                    
                    # strip file ending
                    filename=Path(filename).stem

                    # if filename is not already in res, add it
                    if filename not in res.resultfiles:
                        res.resultfiles[filename]=df
                    else:
                        assert type(res.resultfiles[filename])==pd.DataFrame
                        res.resultfiles[filename]=pd.concat([res.resultfiles[filename],df],axis=1)
    

        for well,sites in res.wells.items():
            for site,c in sites.items():
                res.total_sites+=1
                res.num_processed_sites+=c

        return res

    def dumpDatabaseHead(self,n:int=5):
        """ print forst n rows of each table, if they exist """
        assert self.dbmetadata.tables is not None
        for table in self.dbmetadata.tables.values():
            res=self.dbExec(f"select * from {table.name} limit {n};",as_pd=True)
            assert type(res)==pd.DataFrame
            if res is not None:
                print(table.name,res,sep="\n")

    def getProcessingBatchID(self,project_name:str,experiment_name:str)->int:
        # get processing batch id for this experiment, by increasing the maximum batch id for this experiment by 1, and using 0 if none already exist
        
        res=self.dbExec(f"select id from projects where name='{project_name}';")
        assert type(res)==list
        project_id=res[0][0]
        res=self.dbExec(f"select id from experiments where projectid={project_id} and name='{experiment_name}';")
        assert type(res)==list
        experiment_id=res[0][0]

        res=self.dbExec(f"select max(batchid) from profile_results where experimentid={experiment_id};")
        assert type(res)==list
        if len(res)==0 or res[0][0] is None:
            return 0
        
        return res[0][0]+1

    def getProjectNames(self)->Result_getProjectNames:
        res=self.dbExec("select name from projects;",as_pd=True)
        assert type(res)==pd.DataFrame
        return Result_getProjectNames(
            projects=res["name"].tolist()
        )
    
    def getExperiments(self,projectname:str)->Result_getExperiments:
        res=self.dbExec(f"select name from experiments where projectid=(select id from projects where name='{projectname}');",as_pd=True)
        assert type(res)==pd.DataFrame
        return Result_getExperiments(
            experiments=res["name"].tolist()
        )
    

