from flask import Flask, render_template, redirect, url_for, request, jsonify, Response, stream_with_context
from dotenv import load_dotenv
import json
import geopandas as gpd
import psycopg2
import os
import time
from datetime import datetime
from zoneinfo import ZoneInfo
import sys
import math
import paramiko
import sqlite3
import threading
import uuid

app = Flask(__name__)

DB_PATH = 'job_status.db'

load_dotenv()
REMOTE_IP = os.getenv("REMOTE_IP")
REMOTE_USER = os.getenv("REMOTE_USER")
REMOTE_PASS = os.getenv("REMOTE_PASS")
REMOTE_CONFIG_PATH = os.getenv("REMOTE_CONFIG_PATH")

host = os.getenv("HOST")
username = os.getenv("SERVER_USERNAME")
password = os.getenv("PASSWORD")
port = os.getenv("PORT")
db = os.getenv("DB")
geoentity_table = os.getenv("GEOENTITY_TABLE")
geoentity_source_table = os.getenv("GEOENTITY_SOURCE_TABLE")
geoentity_source_seq = os.getenv("GEOENTITY_SOURCE_SEQ")

def __printMsg(opt,text):
    """
    Purpose
    ----------
    This method will Print output based on selected option

    Parameters
    ----------
    opt : Options like: 'Warning','Info','Error'
    text: Text which will be printed

    Returns
    -------
    Printed text as per the selected option.
    """
    if opt=="Warning":
        print("[Warning]: "+text+"\r\n")
    elif opt=="Info":
        print("[Info]: "+text+"\r\n")
    elif opt=="Error":
        print("<Error> "+text+"\r\n")
    else:
        print("Unsupported option "+opt+" for prinitng.")


def __get_aux_data(attributes_array,row):
    returnobj={'features':{}}
    for att in attributes_array:
        if 'Level_IV' in att:
            returnobj['features']['Level_lV']=str(row[att])
        else:
            returnobj['features'][att]=str(row[att])            
        
    return json.dumps(returnobj)


def __getGeoEntityID(GeoEntityIDKeys,row,type):
    """
    Purpose
    ----------
    This method will give ID of GeoEntity

    Parameters
    ----------
    ZoneIDKeys : Ordered Arrays for Key Parsing
    row : GeoJSON row for dataset extraction

    Returns
    -------
    zoneid : String contain ZoneID
    """
    if type=="Int":
        return str(int(row[GeoEntityIDKeys]))       
    else:
        return str(row[GeoEntityIDKeys])


def __getGeom(wkt):
    """
    Purpose
    ----------
    This method will give ST_GeomFromGeoJSON fn string based on coordinate info from geojson row and can be used as geom in insert query.

    Parameters
    ----------
    wkt : WKT of Geometry

    Returns
    -------
    Geom WKT String
    """        
    return "ST_GeomFromText(\'"+str(wkt)+"\',4326)"



def read_data(file_path):
    # Reads a GeoJSON file from remote path using Paramiko and returns a GeoDataFrame.

    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh.connect(REMOTE_IP, username=REMOTE_USER, password=REMOTE_PASS)
    sftp = ssh.open_sftp()

    # Read file as text
    try:
        with sftp.open(file_path, 'r') as remote_file:
            geojson_text = remote_file.read()
    except FileNotFoundError:
        raise FileNotFoundError(f"Remote file not found: {file_path}")


    sftp.close()
    ssh.close()

    # Parse GeoJSON text into Python dict
    geojson_obj = json.loads(geojson_text)

    # Convert features to GeoDataFrame
    gdf = gpd.GeoDataFrame.from_features(geojson_obj["features"])
    return gdf


def parse_config(config_path, target_key):
    # Reads config.json from remote, searches for target_key, returns that section.

    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh.connect(REMOTE_IP, username=REMOTE_USER, password=REMOTE_PASS)
    sftp = ssh.open_sftp()

    with sftp.open(config_path, 'r') as remote_file:
        config_data = json.load(remote_file)

    sftp.close()
    ssh.close()

    if "config" in config_data and target_key in config_data["config"]:
        return config_data["config"][target_key]
    else:
        return None


def insertion(gdf, geoentity_config, geoentity):
    try:
        conn=None
        cur=None
        try:
            conn=psycopg2.connect(database=db, user=username, password=password, host=host, port=port)
            conn.autocommit = True
            cur= conn.cursor()
        except:
            __printMsg("Error"," DB Error, Please check DB Configuration once.")
            if conn is not None:
                cur.close()
                conn.close()
            __printMsg('Error',"====== GeoEntity ingestion execution is failed due to database connection. ======")
            sys.exit()  

        previous_parent_id=None


        # SourceInfo Loading
        source_name = geoentity_config["geoentity_source"]["name"]
        source_publish_date_yyyymmdd = time.mktime(datetime.strptime(
            geoentity_config["geoentity_source"]["publish_date_yyyymmdd"], "%Y%m%d").timetuple())
        source_project = geoentity_config["geoentity_source"]["project"]
        source_provider = geoentity_config["geoentity_source"]["provider"]
        source_category = geoentity_config["geoentity_source"]["category"]
        source_aux = geoentity_config["geoentity_source"]["aux_data"]

        # ConfigInfo Loading
        config_geojsonfile_file_path = geoentity_config["geoentity_config"]["geoJSON_file_config"]["file_path"]
        config_geojsonfile_parent_type = geoentity_config["geoentity_config"]["geoJSON_file_config"]["parent_type"]
        config_geojsonfile_parent_geoent_source_id = geoentity_config["geoentity_config"]["geoJSON_file_config"]["parent_geoentity_source_id"]
        config_geojsonfile_prefix_identifier = geoentity_config["geoentity_config"]["geoJSON_file_config"]["prefix_identifier"]
        config_geojsonfile_infoattr_name = geoentity_config["geoentity_config"]["geoJSON_file_config"]["geoJSON_info_attribute"]["name"]
        config_geojsonfile_infoattr_featureid = geoentity_config["geoentity_config"]["geoJSON_file_config"]["geoJSON_info_attribute"]["feature_ID"]
        config_geojsonfile_infoattr_featureid_type = "str"
        if "feature_ID_type" in geoentity_config["geoentity_config"]["geoJSON_file_config"]["geoJSON_info_attribute"]:
            config_geojsonfile_infoattr_featureid_type = geoentity_config["geoentity_config"]["geoJSON_file_config"]["geoJSON_info_attribute"]["feature_ID_type"]

        if "reprocess_flag" in geoentity_config["geoentity_source"] and geoentity_config["geoentity_source"]["reprocess_flag"] is False:
            geoentity_config["geoentity_source"]["reprocess_flag"] = True
            # config_data["config"][entity_key]["geoentity_source"] = geoentity_source

        if (previous_parent_id is not None) and (config_geojsonfile_parent_geoent_source_id==-1):
            config_geojsonfile_parent_geoent_source_id=previous_parent_id
            __printMsg("Info"," Previous parent id:"+str(previous_parent_id)+" is set for geoentity: "+geoentity)
        elif (config_geojsonfile_parent_geoent_source_id==-1):
            __printMsg("Error","First element and incase of parent insertion failure parent_id can't be inherited. please check parent_geoentity_source_id configuration for geoentity: "+geoentity)
            sys.exit()


        #None Checking for Parameters
        config_var_list=[source_name,source_publish_date_yyyymmdd,source_project,source_provider,source_category,config_geojsonfile_file_path,config_geojsonfile_parent_type,config_geojsonfile_parent_geoent_source_id,config_geojsonfile_prefix_identifier,config_geojsonfile_infoattr_name,config_geojsonfile_infoattr_featureid]
        if (None in config_var_list) or ("" in config_var_list) :
            __printMsg('Error', "=====Configuration error, please see config once for "+geoentity+" ======")
        else:
            config_var_list=None
        __printMsg('Info', geoentity+" Parameters(Global and Config) loaded successfully.")
            
        #Phase 1: Insertion Algo - Source Insertion
        __printMsg("Info", "Phase-1: GeoEntity Source Insertion Started.")
        geoentity_source_id=None
        source_insertion_query=None
        if not(source_aux=="NULL" or source_aux=="null" or source_aux=="Null" or source_aux==""):
            source_aux="'"+source_aux+"'"
        else:
            source_aux="NULL"
        

        #-----If duplicate exist then return id for phase-2 execution      
        try:
            cur.execute("SELECT setval('"+geoentity_source_seq+"', max(id)) from "+geoentity_source_table)
            if(config_geojsonfile_parent_geoent_source_id>0):
                source_insertion_query="INSERT INTO "+geoentity_source_table+"(name, publish_date, project, provider, category,auxdata,parent_source_id) VALUES ('"+source_name+"', "+str(source_publish_date_yyyymmdd)+", '"+source_project+"', '"+source_provider+"', '"+source_category+"', "+source_aux+","+str(config_geojsonfile_parent_geoent_source_id)+") returning id;"
            else:
                source_insertion_query="INSERT INTO "+geoentity_source_table+"(name, publish_date, project, provider, category,auxdata) VALUES ('"+source_name+"', "+str(source_publish_date_yyyymmdd)+", '"+source_project+"', '"+source_provider+"', '"+source_category+"', "+source_aux+") returning id;"
            __printMsg('Info', source_insertion_query)
            cur.execute(source_insertion_query)
            if(cur.rowcount<1): #0 row
                __printMsg('Error', "=====(Phase1) Source insertion failed for "+geoentity+" ======")
            else:                
                geoentity_source_id=cur.fetchone()[0]
                __printMsg('Info', " (Phase1) Source Insertion Completed Successfully.")    
        except psycopg2.Error as e:
            
            if "duplicate" in e.pgerror:
                __printMsg('Error', "=====(Phase1) Already Source is existing for "+geoentity+" ======")
                if geoentity_config["geoentity_source"]["reprocess_flag"]:
                    source_id_query="select id from "+geoentity_source_table+" where name='"+source_name+"' and publish_date="+str(source_publish_date_yyyymmdd)+" and project='"+source_project+"' and provider='"+source_provider+"' and category='"+source_category+"'"
                    cur.execute(source_id_query)
                    geoentity_source_id=cur.fetchone()[0]
                else:
                    sys.exit()
            else:
                __printMsg('Error', " Phase1 Source Insertion for <"+geoentity+"> has been failed.")
                previous_parent_id=None
        __printMsg("Info", "Phase1: GeoEntity Source Processing Completed Successfully.")


        #Phase 2: Insertion Algo - GeoEntity Insertion
        __printMsg("Info", "Phase-2: GeoEntity Insertion Started.")
        previous_parent_id=geoentity_source_id
        total_records=0
        processed_record=0
        failed_record=0
        try:
            gdf.set_crs(epsg=4326, inplace=True, allow_override=True)
            __printMsg("Info"," Reading of "+geoentity+" geojson file has been completed.")
            total_records=gdf.shape[0]
            __printMsg("Info"," In "+geoentity+" total records for processing:"+str(total_records))
        except:
            __printMsg("Error", "Phase2 Sorry reading error for "+geoentity+" geojson file, please check the file once.")
            previous_parent_id=None
        
        for i,row in gdf.iterrows():
            # if i<2288559:
            #    continue   

            geoentity_id=config_geojsonfile_prefix_identifier+__getGeoEntityID(config_geojsonfile_infoattr_featureid,row,config_geojsonfile_infoattr_featureid_type)#--
            geoentity_name=row[config_geojsonfile_infoattr_name]#--  
            #print("geoname", geoentity_name, type(geoentity_name))   

            if(geoentity_name and (not isinstance(geoentity_name,float) or not math.isnan(geoentity_name))):
                #print('i:',str(i),'geoentity_id:',geoentity_id,'geoentity_name:',geoentity_name)
                geoentity_name=geoentity_name.replace("'","")

            geoentity_geom=__getGeom(row.geometry.wkt)                
            geoentity_insertion_query=None
            if (config_geojsonfile_parent_geoent_source_id>0):
                geoentity_insertion_query ="Insert into "+geoentity_table+"(geoentity_source_id, geoentity_id, name, geom,parent_geoentity_source_id) values ({0},'{1}','{2}',{3},{4})".format(geoentity_source_id,geoentity_id,geoentity_name,geoentity_geom,config_geojsonfile_parent_geoent_source_id)
                if "geoJSON_aux_attributes" in geoentity_config["geoentity_config"]["geoJSON_file_config"]:
                    geoentity_insertion_query ="Insert into "+geoentity_table+"(geoentity_source_id, geoentity_id, name, geom,parent_geoentity_source_id,auxdata) values ({0},'{1}','{2}',{3},{4},'{5}')".format(geoentity_source_id,geoentity_id,geoentity_name,geoentity_geom,config_geojsonfile_parent_geoent_source_id,__get_aux_data(geoentity_config["geoentity_config"]["geoJSON_file_config"]["geoJSON_aux_attributes"],row))
            else:
                geoentity_insertion_query ="Insert into "+geoentity_table+"(geoentity_source_id, geoentity_id, name, geom) values ({0},'{1}','{2}',{3})".format(geoentity_source_id,geoentity_id,geoentity_name,geoentity_geom)                            
                if "geoJSON_aux_attributes" in geoentity_config["geoentity_config"]["geoJSON_file_config"]:
                    geoentity_insertion_query ="Insert into "+geoentity_table+"(geoentity_source_id, geoentity_id, name, geom,auxdata) values ({0},'{1}','{2}',{3},'{4}')".format(geoentity_source_id,geoentity_id,geoentity_name,geoentity_geom,__get_aux_data(geoentity_config["geoentity_config"]["geoJSON_file_config"]["geoJSON_aux_attributes"],row))                            
            try:
                cur.execute(geoentity_insertion_query)
                if cur.rowcount == 1:
                    processed_record=processed_record+1        
                else:
                    failed_record=failed_record+1
            except psycopg2.Error as e:
                if "duplicate" in e.pgerror:
                    if geoentity_config["geoentity_source"]["reprocess_flag"]:
                        processed_record=processed_record+1
                    else:
                        sys.exit()
                else:
                    __printMsg("Error", e.pgerror)
                    
        __printMsg("Info"," Phase2 GeoEntity Insertion: Successfully processed records:"+str(processed_record))
        __printMsg("Info"," Phase2 GeoEntity Insertion: Failed Records:"+str(failed_record)+" \n")
        __printMsg("Info", "Phase2: GeoEntity Insertion Successfully Completed.")
        if (config_geojsonfile_parent_geoent_source_id==0):
            return True
        else:

            


        #Phase3: Spaitail Join 
        #Phase3: Parent Condition Checking
        # if (config_geojsonfile_parent_geoent_source_id!=0):
            __printMsg("Info"," Phase3: Spatial join statred.")
            # geoentity_parent_update_query="UPDATE "+geoentity_table+" SET geoentity_id=CONCAT(parent.geoentity_id,"+geoentity_table+".geoentity_id), parent_id=parent.geoentity_id, parent_name=parent.name FROM (SELECT geoentity_id, name,ST_Buffer(geom::geography,2000)::geometry as geom FROM "+geoentity_table+" where geoentity_source_id="+str(config_geojsonfile_parent_geoent_source_id)+") parent WHERE geoentity_source_id="+str(geoentity_source_id)+" and geoentity.parent_geoentity_source_id="+str(config_geojsonfile_parent_geoent_source_id)+" and ST_Contains(parent.geom,geoentity.geom);"
            geoentity_parent_update_query="UPDATE "+geoentity_table+" AS child SET geoentity_id = CONCAT(parent.geoentity_id, child.geoentity_id), parent_id = parent.geoentity_id, parent_name = parent.name, parent_geoentity_source_id = parent.geoentity_source_id FROM (SELECT geoentity_source_id, geoentity_id, name, geom FROM  "+geoentity_table+" WHERE geoentity_source_id = "+str(config_geojsonfile_parent_geoent_source_id)+") AS parent WHERE child.geoentity_source_id = "+str(geoentity_source_id)+" AND ST_Intersects(parent.geom, child.geom) AND ST_Contains(parent.geom, ST_Centroid(child.geom))";                
            #If parent name is set then update query will not be executed
            update_test_query="SELECT COUNT(*) FROM geoentity where geoentity_source_id = "+str(geoentity_source_id)+" and parent_name is not NULL; "
            cur.execute(update_test_query)
            noof_updated_rows=cur.fetchone()[0]
            if noof_updated_rows<1:
                command="psql -h "+host+" -U "+username+" -d "+db+" -p "+str(port)+" -c \""+geoentity_parent_update_query+"\""
                if(geoentity_config["geoentity_config"]["geoJSON_file_config"]["spatailjoin_flag"]):
                    os.system(command)
                else:
                    __printMsg("Info","Updte Query is: "+geoentity_parent_update_query)
                __printMsg("Info", " Phase3: Spatail join is performed for "+ geoentity)
            else:
                __printMsg(geoentity_parent_update_query)                 
            __printMsg("Info", " Phase3 Process Completed for "+ geoentity) 

            
        #Local Variable Reset to None
        source_name=None
        source_publish_date_yyyymmdd=None
        source_project=None
        source_provider=None
        source_category=None
        source_aux=None            
        config_geojsonfile_file_path=None
        config_geojsonfile_parent_type=None
        config_geojsonfile_parent_geoent_source_id=None
        config_geojsonfile_prefix_identifier=None
        config_geojsonfile_infoattr_name=None
        config_geojsonfile_infoattr_featureid=None
        
        if conn is not None:    
            cur.close()
            conn.close()
        __printMsg('Info',"====== GeoEntity Execution Completed and and All DB Connections are Closed. ======")
        sys.exit() 

        return True

    except Exception as e:
        print(f"❌ Error in insertion: {e}")
        return False


def pyramid_generation(id, polygon_bool):
    try:
        # Connect to the PostGIS database move to config on a per layer basis
        conn = psycopg2.connect(database=db, user=username, password=password, host=host, port=port)
        cur = conn.cursor()



        filter_query = ""

        #Create the vedas_vector_tiles table if it does not exist
        #cur.execute("DROP TABLE IF EXISTS vedas_vector_server_geom_cache")
        # cur.execute("CREATE TABLE IF NOT EXISTS vedas_vector_server_geom_cache (layer_name VARCHAR,geo_id VARCHAR, geom GEOMETRY,tolerance VARCHAR)")
        # cur.execute("Create index vedas_vector_server_geom_cache_gist on vedas_vector_server_geom_cache using GIST(geom)")
        cur = conn.cursor()


        #************************Parameter required to change********************************
        # geoentity_source_id for which pyramid has to be generated.
        geoentity_source_id = id
        #if geometry type  is not polygon or multipolygon then False::
        isPolygon = polygon_bool


        delete_query = "DELETE FROM geoentity_pyramid_levels where geoentity_source_id = "+geoentity_source_id
        cur.execute(delete_query)
        conn.commit()

        tolerances = ["0.08192","0.04096","0.02048","0.01024","0.00512", "0.00256", "0.00128", "0.00064","0.00032","0.00016","0.00008", "0.00004", "0.00002", "0.00001","original"]
        tolerances.reverse()
        prev_tol = ""



        for level in range(len(tolerances)):

            
            insert_prefix = "INSERT INTO geoentity_pyramid_levels (geoentity_source_id,geoentity_id,level,geom) "

            # print("Looping ",level)
            yield f"Looping {level}"
            if level == 0:
                # print("Entered if",level)
                yield f"Enetered if {level}"

                # print("Ingesting ",level,tolerances[level])
                yield f"Ingesting {level} {tolerances[level]}"

                querystr = insert_prefix +" SELECT geoentity_source_id, geoentity_id, "+str(level)+",  geom FROM geoentity where geoentity_source_id="+geoentity_source_id+" " + ("and ST_IsValid(ST_Buffer(geom,0))" if isPolygon else " ")
                # print("Query",querystr)
                yield f"Query {querystr}"

                cur.execute(querystr)
                conn.commit()
            else:
                # print("Entered else",level)
                yield f"Entered else {level}"

                gridsize = str(0.000001)
                if(float(tolerances[level])>0.00001):
                    gridsize = str(0.00001)
                if(float(tolerances[level])>0.0001):
                    gridsize = str(0.0001)
                if(float(tolerances[level])>0.001):
                    gridsize = str(0.001)
                # print("Ingesting ",level,tolerances[level],gridsize)
                yield f"Ingesting {level} {tolerances[level]} {gridsize}"

                querystr = insert_prefix +" SELECT geoentity_source_id, geoentity_id,"+str(level)+", "+ ("ST_MakeValid(ST_Buffer(ST_SnapToGrid(ST_SimplifyPreserveTopology(geom,"+tolerances[level]+"),0,0,"+gridsize+","+gridsize+"),0))" if isPolygon else "geom") +" FROM geoentity_pyramid_levels where geoentity_source_id="+geoentity_source_id+" and level="+str(level-1)+" AND geom IS NOT NULL AND NOT ST_IsEmpty(geom) AND ST_IsValid(geom)"
                # print("Query",querystr)
                yield f"Query {querystr}"

                cur.execute(querystr)
                conn.commit()

    except Exception as e:
        # print(f"Error in pyramid generation: {e}")
        yield f"Error in pyramid generation: {e}"


def init_db():
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute('''
            CREATE TABLE IF NOT EXISTS jobs (
                job_id TEXT PRIMARY KEY,
                entity_key TEXT NOT NULL,
                status TEXT NOT NULL,
                started_on TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                message TEXT,
                result TEXT
            )
        ''')
        conn.commit()

init_db()

def create_job(entity_key):
    job_id = str(uuid.uuid4())
    ist = ZoneInfo("Asia/Kolkata")
    started_on = datetime.now(ist).isoformat()
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute("""
            INSERT INTO jobs (job_id, entity_key, status, started_on, message)
            VALUES (?, ?, ?, ?, ?)
        """, (job_id, entity_key, "pending", started_on, "Job submitted"))
        conn.commit()
    return job_id


def update_job(job_id, status, message=None, result=None):
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute("""
            UPDATE jobs SET status=?, message=?, result=?
            WHERE job_id=?
        """, (
            status,
            message,
            json.dumps(result) if result else None,
            job_id
        ))
        conn.commit()


def get_job_status(job_id):
    with sqlite3.connect(DB_PATH) as conn:
        cursor = conn.execute("""
            SELECT job_id, entity_key, status, started_on, message, result
            FROM jobs WHERE job_id=?
        """, (job_id,))
        row = cursor.fetchone()
        if row:
            return {
                "job_id": row[0],
                "entity_key": row[1],
                "status": row[2],
                "started_on": row[3],
                "message": row[4],
                "result": json.loads(row[5]) if row[5] else None
            }
        return None


def republish_worker(job_id, entity_key):
    try:
        update_job(job_id, "running", message="Started republish task")

        entity_data = parse_config(REMOTE_CONFIG_PATH, entity_key)
        if not entity_data:
            update_job(job_id, "failed", "Key not found in config")
            return

        print(f"[DEBUG] parse_config returned for {entity_key}:")
        print(json.dumps(entity_data, indent=4))

        # Read its GeoJSON file entirely in memory
        geojson_path = entity_data["geoentity_config"]["geoJSON_file_config"]["file_path"]
        print(f"[DEBUG] Remote geojson_path: {geojson_path}")
        gdf = read_data(geojson_path)  # <- now memory-based

        # Debug print to console
        print(f"[DEBUG] read_data returned GeoDataFrame with {len(gdf)} rows and {len(gdf.columns)} columns.")

        # insertion_success = insertion(gdf, entity_data, entity_key)

        # Example update — mark reprocess_flag true in config
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh.connect(REMOTE_IP, username=REMOTE_USER, password=REMOTE_PASS)
        sftp = ssh.open_sftp()

        with sftp.open(REMOTE_CONFIG_PATH, 'r') as remote_file:
            config_data = json.load(remote_file)

        if "config" in config_data and entity_key in config_data["config"]:
            # Replace keys_to_process with only this key
            config_data["config"]["geoentity_keys_to_process"] = [entity_key]

        # Update reprocess_flag if currently False
        geoentity_source = config_data["config"][entity_key].get("geoentity_source", {})
        if "reprocess_flag" in geoentity_source and geoentity_source["reprocess_flag"] is False:
            geoentity_source["reprocess_flag"] = True
            config_data["config"][entity_key]["geoentity_source"] = geoentity_source

        with sftp.open(REMOTE_CONFIG_PATH, 'w') as remote_file:
            remote_file.write(json.dumps(config_data, indent=4))

        sftp.close()
        ssh.close()


        update_job(job_id, "completed", message="Job completed", result={
            "rows_inserted": len(gdf),
            "entity": entity_key
        })

    except Exception as e:
        update_job(job_id, "failed", message=str(e))


@app.route('/')
def index():
    return render_template('index.html')


@app.route('/config')
def config():
    try:
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh.connect(REMOTE_IP, username=REMOTE_USER, password=REMOTE_PASS)

        sftp = ssh.open_sftp()

        with sftp.open(REMOTE_CONFIG_PATH, 'r') as remote_file:
            config_str = remote_file.read()
        config_data = json.loads(config_str)

        sftp.close()
        ssh.close()

        config_section = config_data.get("config", {})
        # Filter to only include dict values, skip lists or other types
        filtered_config = {k: v for k, v in config_section.items() if isinstance(v, dict)}

        return render_template('config.html', config=filtered_config)

    except Exception as e:
        return render_template('config.html', config={}, error=str(e))


@app.route('/register', methods=['GET', 'POST'])
def register():
    # Connect to remote and load config on both GET and POST
    try:
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh.connect(REMOTE_IP, username=REMOTE_USER, password=REMOTE_PASS)
        sftp = ssh.open_sftp()

        with sftp.open(REMOTE_CONFIG_PATH, 'r') as remote_config:
            config_data = json.load(remote_config)
        sftp.close()
        ssh.close()
    except Exception as e:
        return f"Error loading config: {e}"

    if request.method == 'GET':
        key = request.args.get('key')
        if key:
            key_name = key.replace(" ", "_")
            geoentity = config_data.get("config", {}).get(key_name)
            if geoentity:
                # Prepare data to prefill form
                return render_template('register.html', prefill=geoentity, key=key)
            else:
                return render_template('register.html', message="Geoentity key not found.")
        else:
            return render_template('register.html')

    elif request.method == 'POST':
        try:
            # Get submitted key and prepare key_name
            key = request.form.get("key")
            key_name = key.lower().replace(" ", "_")

            geojson_file = request.files.get('geojson_file')
            filename = geojson_file.filename if geojson_file else None

            # Build geoentity_source dict from form
            geoentity_source = {
                "remark": {
                    "info": request.form.get("source_remark_info", "").strip()
                },
                "name": request.form.get("name"),
                "project": request.form.get("project"),
                "provider": request.form.get("provider"),
                "publish_date_yyyymmdd": request.form.get("publish_date_yyyymmdd"),
                "category": request.form.get("category"),
                "aux_data": request.form.get("aux-data"),
                "reprocess_flag": False
            }

            geoentity_config = {
                "remark": {
                    "info": request.form.get("config_remark_info", "").strip(),
                    "note": [note.strip() for note in request.form.get("config_notes", "").splitlines() if note.strip()]
                },
                "geoJSON_file_config": {
                    "file_path": "",  # to update if new file uploaded
                    "parent_type": request.form.get("parent_type"),
                    "parent_geoentity_source_id": int(request.form.get("parent_geoentity_source_id") or 0),
                    "prefix_identifier": request.form.get("prefix_identifier"),
                    "geoJSON_aux_attributes": [x.strip() for x in request.form.get("geojson_aux_attributes", "").split(",") if x.strip()],
                    "geoJSON_info_attribute": {
                        "name": request.form.get("geojson_info_name"),
                        "feature_ID": request.form.get("geojson_feature_id")
                    }
                }
            }

            # Connect again for upload and update
            ssh = paramiko.SSHClient()
            ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            ssh.connect(REMOTE_IP, username=REMOTE_USER, password=REMOTE_PASS)
            sftp = ssh.open_sftp()

            if geojson_file and filename and filename.endswith('.geojson'):
                remote_dir = os.path.dirname(REMOTE_CONFIG_PATH)
                remote_geojson_path = f"{remote_dir.rstrip('/')}/Geojson_Files/{filename}"
                geojson_file.seek(0)
                with sftp.open(remote_geojson_path, 'wb') as remote_file:
                    remote_file.write(geojson_file.read())
                geoentity_config["geoJSON_file_config"]["file_path"] = remote_geojson_path
            else:
                # If editing and no new file uploaded, preserve existing file path if present
                existing = config_data.get("config", {}).get(key_name, {})
                existing_path = existing.get("geoentity_config", {}).get("geoJSON_file_config", {}).get("file_path", "")
                geoentity_config["geoJSON_file_config"]["file_path"] = existing_path

            # Load config again (to avoid overwriting changes) — or use loaded config_data from above
            with sftp.open(REMOTE_CONFIG_PATH, 'r') as remote_config:
                config_data = json.load(remote_config)

            if "config" not in config_data:
                config_data["config"] = {}

            config_data["config"]["geoentity_keys_to_process"] = [key_name]

            # Update the specific geoentity block
            config_data["config"][key_name] = {
                "geoentity_source": geoentity_source,
                "geoentity_config": geoentity_config
            }

            # Save updated config.json
            with sftp.open(REMOTE_CONFIG_PATH, 'w') as remote_config:
                remote_config.write(json.dumps(config_data, indent=4))

            sftp.close()
            ssh.close()

            return redirect(url_for('config'))

        except Exception as e:
            return render_template('register.html', message=f'Error: {e}', prefill=request.form)


@app.route('/republish', methods=['POST'])
def republish():
    try:
        entity_key = request.form.get("key")
        if not entity_key:
            return jsonify({"status": "error", "message": "No key provided"}), 400

        job_id = create_job(entity_key)

        # Run in background thread
        thread = threading.Thread(target=republish_worker, args=(job_id, entity_key))
        thread.start()

        return jsonify({
            "status": "submitted",
            "job_id": job_id,
            "entity_key": entity_key
        }), 202

    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500


@app.route('/status/<job_id>', methods=['GET'])
def check_job_status(job_id):
    job = get_job_status(job_id)
    if job:
        return jsonify(job), 200
    return jsonify({"status": "error", "message": "Job not found"}), 404


@app.route('/generate_pyramids', methods=['POST'])
def generate_pyramids():
    try:
        generate_pyramid_key = request.form.get("geoentity_source_id")
        if not generate_pyramid_key:
            return jsonify({"status": "error", "message": "No key provided"}), 400

        isPolygon = request.form.get("is_polygon") == "True"

        # Run the function normally without streaming logs
        # (Can be refactored to call the generator and collect logs if needed)
        logs = []
        for log in pyramid_generation(generate_pyramid_key, isPolygon):
            logs.append(log)
        # Optionally you can save logs somewhere or print
        print("".join(logs))

        return jsonify({
            "status": "success",
            "message": f"Pyramid generated for {generate_pyramid_key}",
        }), 200

    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500


@app.route('/generate_pyramids_sse_stream', methods=['GET'])
def generate_pyramids_sse_stream():
    geoentity_source_id = request.args.get("geoentity_source_id")
    if not geoentity_source_id:
        return jsonify({"status": "error", "message": "No key provided"}), 400

    isPolygon = request.args.get("is_polygon") == "True"

    def generate():
        # Yield SSE formatted messages
        for log in pyramid_generation(geoentity_source_id, isPolygon):
            # SSE event format: data: <message>\n\n
            yield f"data: {log.strip()}\n\n"

    return Response(stream_with_context(generate()), mimetype='text/event-stream')



if __name__ == '__main__':
    app.run(debug=True,host='0.0.0.0',port='5003')