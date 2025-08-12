from flask import Flask, render_template, request, jsonify
from dotenv import load_dotenv
import time
import datetime
import os
import io
import json
import paramiko
import geopandas as gpd

app = Flask(__name__)

load_dotenv()
REMOTE_IP = os.getenv("REMOTE_IP")
REMOTE_USER = os.getenv("REMOTE_USER")
REMOTE_PASS = os.getenv("REMOTE_PASS")
REMOTE_CONFIG_PATH = os.getenv("REMOTE_CONFIG_PATH")

host = os.getenv("HOST")
username = os.getenv("USERNAME")
password = os.getenv("PASSWORD")
port = os.getenv("PORT")
db = os.getenv("DB")
geoentity_table = os.getenv("GEOENTITY_TABLE")
geoentity_source_table = os.getenv("GEOENTITY_SOURCE_TABLE")
geoentity_source_seq = os.getenv("GEOENTITY_SOURCE_SEQ")

def __printMsg(self,opt,text):
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


def __get_aux_data(self,attributes_array,row):
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

def insertion(gdf, geoentity_config):
    try:
        previous_parent_id=None

        # SourceInfo Loading
        source_name = geoentity_config["geoentity_source"]["name"]
        source_publish_date_yyyymmdd = time.mktime(datetime.datetime.strptime(
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

        # Handle parent ID logic
        if (previous_parent_id is not None) and (config_geojsonfile_parent_geoent_source_id == -1):
            config_geojsonfile_parent_geoent_source_id = previous_parent_id
            print(f"Info: Previous parent id: {previous_parent_id} is set for geoentity: {geoentity_key or '[unnamed]'}")
        elif config_geojsonfile_parent_geoent_source_id == -1:
            print(f"Error: First element and in case of parent insertion failure, parent_id can't be inherited. "
                  f"Please check parent_geoentity_source_id configuration for geoentity: {geoentity_key or '[unnamed]'}")
            sys.exit()

        #None Checking for Parameters
        config_var_list=[source_name,source_publish_date_yyyymmdd,source_project,source_provider,source_category,config_geojsonfile_file_path,config_geojsonfile_parent_type,config_geojsonfile_parent_geoent_source_id,config_geojsonfile_prefix_identifier,config_geojsonfile_infoattr_name,config_geojsonfile_infoattr_featureid]
        if (None in config_var_list) or ("" in config_var_list) :
            print(f'Error', "=====Configuration error, please see config once for "+geoentity+" ======")

        else:
            config_var_list=None
        print(f'Info', geoentity+" Parameters(Global and Config) loaded successfully.")

        #Phase 1: Insertion Algo - Source Insertion
        p("Info", "Phase-1: GeoEntity Source Insertion Started.")
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
            print(f'Info', source_insertion_query)
            cur.execute(source_insertion_query)
            if(cur.rowcount<1): #0 row
                print(f'Error', "=====(Phase1) Source insertion failed for "+geoentity+" ======")

            else:                
                geoentity_source_id=cur.fetchone()[0]
                self.__printMsg('Info', " (Phase1) Source Insertion Completed Successfully.")    
        except psycopg2.Error as e:
            
            if "duplicate" in e.pgerror:
                self.__printMsg('Error', "=====(Phase1) Already Source is existing for "+geoentity+" ======")
                if __GeoEntityIngestConfig[geoentity]["geoentity_source"]["reprocess_flag"]:
                    source_id_query="select id from "+geoentity_source_table+" where name='"+source_name+"' and publish_date="+str(source_publish_date_yyyymmdd)+" and project='"+source_project+"' and provider='"+source_provider+"' and category='"+source_category+"'"
                    cur.execute(source_id_query)
                    geoentity_source_id=cur.fetchone()[0]
                else:
                    sys.exit()
            else:
                self.__printMsg('Error', " Phase1 Source Insertion for <"+geoentity+"> has been failed.")
                previous_parent_id=None
        self.__printMsg("Info", "Phase1: GeoEntity Source Processing Completed Successfully.")
        









        # # Debug Output
        # print("✔️ Insertion preparation complete with the following extracted values:")
        # print(f"  - Source Name: {source_name}")
        # print(f"  - Publish Date (epoch): {source_publish_date_yyyymmdd}")
        # print(f"  - Project: {source_project}")
        # print(f"  - Provider: {source_provider}")
        # print(f"  - Category: {source_category}")
        # print(f"  - Aux Data: {source_aux}")
        # print(f"  - File Path: {config_geojsonfile_file_path}")
        # print(f"  - Parent Type: {config_geojsonfile_parent_type}")
        # print(f"  - Parent GeoEntity Source ID: {config_geojsonfile_parent_geoent_source_id}")
        # print(f"  - Prefix Identifier: {config_geojsonfile_prefix_identifier}")
        # print(f"  - Info Attr Name: {config_geojsonfile_infoattr_name}")
        # print(f"  - Feature ID: {config_geojsonfile_infoattr_featureid} (Type: {config_geojsonfile_infoattr_featureid_type})")

        # Example: You might loop through gdf here to process or insert each row.
        # for idx, row in gdf.iterrows():
        #     print(row)

        return True

    except Exception as e:
        print(f"❌ Error in insertion: {e}")
        return False

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
    if request.method == 'POST':
        try:
            geojson_file = request.files.get('geojson_file')
            if not geojson_file:
                return render_template('register.html', message='No file uploaded.')

            filename = geojson_file.filename
            if not filename.endswith('.geojson'):
                return render_template('register.html', message='Invalid file format. Only .geojson allowed.')

            # Extract form fields
            key = request.form.get("key")
            key_name = key.lower().replace(" ", "_")  # use as JSON key

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
                "reprocess_flag": request.form.get("reprocess_flag") == "True"
            }

            geoentity_config = {
                "remark": {
                    "info": request.form.get("config_remark_info", "").strip(),
                    "note": [note.strip() for note in request.form.get("config_notes", "").splitlines() if note.strip()]
                },
                "geoJSON_file_config": {
                    "file_path": "",  # will be updated
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

            # Connect to remote server
            ssh = paramiko.SSHClient()
            ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            ssh.connect(REMOTE_IP, username=REMOTE_USER, password=REMOTE_PASS)
            sftp = ssh.open_sftp()

            # Upload GeoJSON
            remote_dir = os.path.dirname(REMOTE_CONFIG_PATH)
            remote_geojson_path = f"{remote_dir.rstrip('/')}/{filename}"
            geojson_file.seek(0)
            with sftp.open(remote_geojson_path, 'wb') as remote_file:
                remote_file.write(geojson_file.read())

            # Update file path in config
            geoentity_config["geoJSON_file_config"]["file_path"] = remote_geojson_path

            # Read and parse config.json
            with sftp.open(REMOTE_CONFIG_PATH, 'r') as remote_config:
                config_data = json.load(remote_config)

            if "config" not in config_data:
                config_data["config"] = {}

            # Append to keys_to_process list if not already present
            config_data["config"]["geoentity_keys_to_process"] = [key_name]

            # Insert new geoentity block
            config_data["config"][key_name] = {
                "geoentity_source": geoentity_source,
                "geoentity_config": geoentity_config
            }

            # Save updated config.json
            with sftp.open(REMOTE_CONFIG_PATH, 'w') as remote_config:
                remote_config.write(json.dumps(config_data, indent=4))

            sftp.close()
            ssh.close()

            return render_template('register.html', message='GeoJSON and config updated successfully.')

        except Exception as e:
            return render_template('register.html', message=f'Error: {e}')

    return render_template('register.html')

@app.route('/republish', methods=['POST'])
def republish():
    try:
        entity_key = request.form.get("key")
        if not entity_key:
            return jsonify({"status": "error", "message": "No key provided"}), 400

        # Get current entity config
        entity_data = parse_config(REMOTE_CONFIG_PATH, entity_key)
        if not entity_data:
            return jsonify({"status": "error", "message": "Key not found in config"}), 404

        # Debug print to console
        print(f"[DEBUG] parse_config returned for {entity_key}:")
        print(json.dumps(entity_data, indent=4))

        # Read its GeoJSON file entirely in memory
        geojson_path = entity_data["geoentity_config"]["geoJSON_file_config"]["file_path"]
        print(f"[DEBUG] Remote geojson_path: {geojson_path}")
        gdf = read_data(geojson_path)  # <- now memory-based

        # Debug print to console
        print(f"[DEBUG] read_data returned GeoDataFrame with {len(gdf)} rows and {len(gdf.columns)} columns.")
        # print(gdf.head().to_string())

        # insertion_success = insertion(gdf, entity_data)

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

        with sftp.open(REMOTE_CONFIG_PATH, 'w') as remote_file:
            remote_file.write(json.dumps(config_data, indent=4))

        sftp.close()
        ssh.close()

        # Also send back a short preview
        return jsonify({
            "status": "success",
            "message": f"Republish triggered for {entity_key}",
            "parse_config_preview": entity_data,
            "read_data_preview": gdf.head().to_json()
        }), 200

    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500

if __name__ == '__main__':
    app.run(debug=True,host='0.0.0.0',port='5003')