import os, time, typing as tp

from celery import Celery
import mariadb as db
import pandas as pd
from sqlalchemy import create_engine, text, Table, Column, Integer, Text, MetaData, ForeignKey, Boolean
from sqlalchemy.sql.expression import Insert, Select

# setting limits to none to display all columns
pd.set_option('display.width', None)
pd.set_option('display.max_columns', None)

app = Celery('dbwatcher', backend='rpc://', broker=os.getenv('APP_BROKER_URI'), broker_pool_limit = 0)
from tasks import cp_reduce,cp_map

dbengine=create_engine(f"mariadb+mariadbconnector://{os.getenv('MARIADB_USER_USERNAME')}:{os.getenv('MARIADB_USER_PASSWORD')}@{os.getenv('MARIADB_HOSTNAME')}:{os.getenv('MARIADB_PORT')}")
conn=dbengine.connect()

def ensureDatabase(dbname:str):
    conn.execute(text(f"DROP DATABASE IF EXISTS {dbname};"))
    conn.execute(text(f"CREATE DATABASE IF NOT EXISTS {dbname};"))
    conn.execute(text(f"USE {dbname};"))
    conn.commit()

DBNAME="morphology_information"
ensureDatabase(dbname=DBNAME)

conn.close()

dbengine=create_engine(f"mariadb+mariadbconnector://{os.getenv('MARIADB_USER_USERNAME')}:{os.getenv('MARIADB_USER_PASSWORD')}@{os.getenv('MARIADB_HOSTNAME')}:{os.getenv('MARIADB_PORT')}/{DBNAME}")
conn=dbengine.connect()

def dbExec(query:tp.Union[str,Insert,Select],*args,conn=conn)->tp.Optional[tp.List[tp.Tuple]]:
    if isinstance(query,str):
        query=text(query)

    res=conn.execute(query,*args)
    conn.commit()
    
    if res.returns_rows:
        return list(res)
    
    if res.is_insert:
        if res.rowcount==1:
            return [res.inserted_primary_key]
        
        return res.inserted_primary_key_rows
    
    return res

dbmetadata=MetaData(schema=DBNAME)

dbProjects=Table(
    "projects", dbmetadata,
    Column("id",Integer,primary_key=True,nullable=False,autoincrement=True,unique=True),
    Column("name",Text,unique=True,nullable=False),
)
dbPlates=Table(
    "plates", dbmetadata,
    Column("id",Integer,primary_key=True,nullable=False,autoincrement=True,unique=True),
    Column("projectid",Integer,ForeignKey("projects.id",ondelete="cascade"),nullable=False),
    Column("barcode",Text,nullable=False),
)
dbImagingChannels=Table(
    "imaging_channels", dbmetadata,
    Column("id",Integer,primary_key=True,nullable=False,autoincrement=True,unique=True),
    Column("is_fluorescence",Boolean,nullable=False),
    Column("fluorescence_wavelength_nm",Integer,nullable=True),
    Column("is_brightfield",Boolean,nullable=False),
    Column("brightfield_type",Text,nullable=True),
)
dbImages=Table(
    "images", dbmetadata,
    Column("id",Integer,primary_key=True,nullable=False,autoincrement=True,unique=True),
    Column("plateid",Integer,ForeignKey("plates.id",ondelete="cascade"),nullable=False),
    Column("s3path",Text,nullable=False),
    Column("sitex",Integer,nullable=False),
    Column("sitey",Integer,nullable=False),
    Column("channelid",Integer,ForeignKey("imaging_channels.id",ondelete="cascade"),nullable=False),
)

# creates the tables, if they dont exist already
dbmetadata.create_all(dbengine)

def insertStaticData():
    # static imaging channel dataset
    res=dbExec(dbImagingChannels.insert(),[
        {"is_brightfield":True,"is_fluorescence":False,"brightfield_type":"full"},
        {"is_brightfield":True,"is_fluorescence":False,"brightfield_type":"right half"},
        {"is_brightfield":True,"is_fluorescence":False,"brightfield_type":"left half"},
    ])
    res=dbExec(dbImagingChannels.insert(),[
        {"is_brightfield":False,"is_fluorescence":True,"fluorescence_wavelength_nm":405},
        {"is_brightfield":False,"is_fluorescence":True,"fluorescence_wavelength_nm":488},
        {"is_brightfield":False,"is_fluorescence":True,"fluorescence_wavelength_nm":560},
        {"is_brightfield":False,"is_fluorescence":True,"fluorescence_wavelength_nm":688},
        {"is_brightfield":False,"is_fluorescence":True,"fluorescence_wavelength_nm":730},
    ])

    # checking that data was inserted correctly
    data = pd.read_sql("select * from imaging_channels;", conn)
    print(data)

def checkTasksRegistered(tasks:tp.List[str]=None):
    registered_tasks=None
    try:
        inspector=app.control.inspect()
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

def main():
    insertStaticData()

    def insertImageSet(projectName:str,plateName:str,images:tp.List[str]):
        res=dbExec(dbProjects.insert(),{"name":projectName})
        [(projectid,)]=res

        dbExec(dbPlates.insert().prefix_with("ignore"),[
            {"projectid":projectid, "barcode":plateName},
            {"projectid":projectid, "barcode":plateName},
        ])

        if len(images)==0:
            return
        
        projectid=dbExec(f"select id from projects where name='{projectName}';")[0][0]
        plateid=dbExec(f"select id from plates where barcode='{plateName}' and projectid={projectid};")[0][0]
        
        res=dbExec(dbImages.insert(),[
            {"plateid":plateid,"s3path":image,"sitex":1,"sitey":1,"channelid":1}
            for image
            in images
        ])
        assert len(res)==len(images)

        for row in dbExec("select * from images;"):
            print(row)

    insertImageSet(projectName="testproject",plateName="testplate",images=[
        "fluo405_s1_x1_y1.tif",
        "fluo488_s1_x1_y1.tif",
        "fluo560_s1_x1_y1.tif",
        "fluo638_s1_x1_y1.tif",
        "fluo730_s1_x1_y1.tif",
    ])

    """
    insert data requires:
    - project
    - plate for the project
    - images for the plate
        - incl image metadata (i.e. channel)
    """

    while not checkTasksRegistered([cp_map.name,cp_reduce.name]):
        print("not ready. sleeping...")
        time.sleep(5)

    a_result=cp_map.delay([
        "/mnt/squid/project/plate/Fluo405_s1_x1_y1.tif",
        "/mnt/squid/project/plate/Fluo530_s1_x1_y1.tif",
        "/mnt/squid/project/plate/Fluo688_s1_x1_y1.tif",
        "/mnt/squid/project/plate/Fluo730_s1_x1_y1.tif",
    ]).get()
    print(f"{a_result = }")
    b_result=cp_reduce.delay(a_result).get()
    print(f"{b_result = }")

if __name__=="__main__":
    main()
