from flask import Flask, render_template
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
# def config():
    # try:
    #     # Connect to remote server using Paramiko
    #     ssh = paramiko.SSHClient()
    #     ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    #     ssh.connect(REMOTE_IP, username=REMOTE_USER, password=REMOTE_PASS)

    #     sftp = ssh.open_sftp()

    #     # Step 1: Try to open the remote config file
    #     try:
    #         with sftp.open(REMOTE_CONFIG_PATH, 'r') as remote_file:
    #             config_str = remote_file.read()
    #         config_data = json.loads(config_str)
    #     except FileNotFoundError:
    #         # File doesn't exist: create directory (if needed) and write empty config
    #         remote_dir = os.path.dirname(REMOTE_CONFIG_PATH)

    #         try:
    #             sftp.stat(remote_dir)
    #         except FileNotFoundError:
    #             # Create directory if it doesn't exist
    #             ssh.exec_command(f"mkdir -p {remote_dir}")

    #         # Create empty config
    #         with sftp.open(REMOTE_CONFIG_PATH, 'w') as remote_file:
    #             remote_file.write('{}')
    #         config_data = {}

    #     sftp.close()
    #     ssh.close()

    #     return render_template('config.html', config=config_data.get("config", {}))

    # except Exception as e:
    #     return render_template('config.html', config={}, error=str(e))



    # try:
    #     ssh = paramiko.SSHClient()
    #     ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    #     ssh.connect(REMOTE_IP, username=REMOTE_USER, password=REMOTE_PASS)

    #     sftp = ssh.open_sftp()

    #     with sftp.open(REMOTE_CONFIG_PATH, 'r') as remote_file:
    #         config_str = remote_file.read()
    #     config_data = json.loads(config_str)

    #     sftp.close()
    #     ssh.close()

    #     # Return pretty printed JSON of top-level keys and values
    #     pretty_json = json.dumps(config_data, indent=4)

    #     return f"<pre>{pretty_json}</pre>"

    # except Exception as e:
    #     return f"Error: {str(e)}"



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


@app.route('/register')
def register():
    return render_template('register.html')

if __name__ == '__main__':
    app.run(debug=True,host='0.0.0.0',port='5003')