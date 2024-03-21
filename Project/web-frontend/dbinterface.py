import os, time, typing as tp

from celery import Celery
import mariadb as db
import pandas as pd
from sqlalchemy import create_engine, text, Table, Column, Integer, Text, MetaData, ForeignKey, Boolean, Float, DateTime
from sqlalchemy.sql.expression import Insert, Select
from datetime import datetime
from dataclasses import dataclass
from pathlib import Path
import re

# setting limits to none to display all columns
pd.set_option('display.width', None)
pd.set_option('display.max_columns', None)

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
)
# this table contains a list of all imaging channels used in an experiment (i.e. each row contains a reference to an experiment, and to an imaging channel, including imaging channel specific information, i.e. exposure time, analog gain, illumination strength)
dbExperimentImagingChannels=Table(
    "experiment_imaging_channels", dbmetadata,
    Column("id",Integer,primary_key=True,nullable=False,autoincrement=True,unique=True),
    Column("experimentid",Integer,ForeignKey("experiments.id",ondelete="cascade"),nullable=False),
    Column("channelid",Integer,ForeignKey("imaging_channels.id",ondelete="cascade"),nullable=False),
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
    Column("site_t_h",Integer,nullable=False),
    Column("channelid",Integer,ForeignKey("imaging_channels.id",ondelete="cascade"),nullable=False),
    Column("coord_x_mm",Float,nullable=True),
    Column("coord_y_mm",Float,nullable=True),
    Column("coord_z_um",Float,nullable=True),
    Column("coord_t",DateTime,nullable=True),
)

# deconstruct image naming scheme to get image metadata...
@dataclass(repr=True,frozen=False)
class ImageMetadata:
    image_pathname:str
    wellname:str
    well_id:int
    site:int
    site_x:int
    site_y:int
    site_z:int
    channelname:str
    channel_id:int

    def __init__(self,image:str,db:"DB"):
        """
            image name: 
                example: B03_s1_x0_y0_Fluorescence_405_nm_Ex.tif
                format: <wellname>_s<site>_x<site_x>_y<site_y>[_z<site_z>]_<channelname>_Ex.tif
        """
        self.image_pathname=image

        # image may be nested path, we only want the filename without the extension
        image_path=Path(image).stem

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
        self.channelname=channelname
        self.channel_id=channel_id


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

    def run(self):
        def insertImageSet(projectName:str,plateName:str,images:tp.List[str]):
            res=self.dbExec(dbProjects.insert(),{"name":projectName})
            [(projectid,)]=res

            self.dbExec(dbPlates.insert().prefix_with("ignore"),[
                {"projectid":projectid, "barcode":plateName},
                {"projectid":projectid, "barcode":plateName},
            ])

            if len(images)==0:
                return
            
            projectid=self.dbExec(f"select id from projects where name='{projectName}';")[0][0]
            plateid=self.dbExec(f"select id from plates where barcode='{plateName}';")[0][0]

            images=[ImageMetadata(image) for image in images]

            res=self.dbExec(dbImages.insert(),[
                {
                    "plateid":plateid,
                    "s3path":image.image_pathname,
                    "wellid":image.well_id,
                    
                    "channelid":image.channel_id,

                    "site_x":image.site_x,
                    "site_y":image.site_y,
                    "site_z":image.site_z,
                    "site_t_h":1,

                    "coord_x_mm":11.4,
                    "coord_y_mm":12.3,
                    "coord_z_um":1631.7,
                    "coord_t":datetime.now(),
                }
                for image
                in images
            ])
            assert len(res)==len(images)

            for row in self.dbExec("select * from images;"):
                print(row)

        insertImageSet(projectName="testproject",plateName="testplate",images=[
            "A02_s1_x1_y1_Fluorescence_405_nm_Ex.tif",
            "B03_s1_x0_y0_Fluorescence_488_nm_Ex.tif",
            "D03_s1_x0_y0_Fluorescence_730_nm_Ex.tif",
            "F12_s1_x0_y0_Fluorescence_638_nm_Ex.tif",
        ])

        if False:
            while not self.checkTasksRegistered([cp_map.name,cp_reduce.name]):
                print("not ready. sleeping...")
                time.sleep(5)

            while(1):
                a_result=cp_map.delay([
                    "/mnt/squid/project/plate/A02_s1_x1_y1_Fluorescence_405_nm_Ex.tif",
                    "/mnt/squid/project/plate/B03_s1_x0_y0_Fluorescence_488_nm_Ex.tif",
                    "/mnt/squid/project/plate/D03_s1_x0_y0_Fluorescence_730_nm_Ex.tif",
                    "/mnt/squid/project/plate/F12_s1_x0_y0_Fluorescence_638_nm_Ex.tif",
                ]).get()
                print(f"mapping with cellprofiler done")
                b_result=cp_reduce.delay(a_result).get()
                print(f"reducing of cellprofiler results done")

                time.sleep(1)
