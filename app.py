from flask import Flask, render_template, request, redirect, url_for, flash
from dotenv import load_dotenv
import os
import json
import paramiko

app = Flask(__name__)

load_dotenv()
REMOTE_IP = os.getenv("REMOTE_IP")
REMOTE_USER = os.getenv("REMOTE_USER")
REMOTE_PASS = os.getenv("REMOTE_PASS")
REMOTE_CONFIG_PATH = os.getenv("REMOTE_CONFIG_PATH")

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
                "aux_data": request.form.get("aux-data") == "",
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
            if "geoentity_keys_to_process" not in config_data["config"]:
                config_data["config"]["geoentity_keys_to_process"] = []

            if key_name not in config_data["config"]["geoentity_keys_to_process"]:
                config_data["config"]["geoentity_keys_to_process"].append(key_name)

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

if __name__ == '__main__':
    app.run(debug=True,host='0.0.0.0',port='5003')