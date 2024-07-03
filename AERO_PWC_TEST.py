import paramiko
from datetime import datetime, timedelta
import os
import time
import logging
import pandas as pd

import requests

TIMEOUT_DURATION = 30
w_flag = 1
s_flag = 1
user_path = ''
neo_dir = ''
new_neo_dir = ''
new_neo_delta = ''
new_neo_output = ''

w_dir = ''
w_ct = ''
w_prep_upload = ''
w_scope = ''

s_dir = ''
s_ct = ''
s_prep_upload = ''
s_scope = ''

folder_paths_delete = []
# Configure logging
logging.basicConfig(filename='error.log', level=logging.ERROR, format='%(asctime)s:%(levelname)s:%(message)s')


# Get the User Path
def get_user_path():
    global new_neo_trigger, neo_dir, folder_paths_delete, user_path, w_dir, w_ct, w_prep_upload, w_scope, s_dir, s_ct, s_prep_upload, s_scope
    print('Get User Path')
    user_path = input("Please enter the OneDrive Path to 'AERO_PWC'\nUser Path:")
    print(f'Path is: {user_path}')
    neo_dir = os.path.join(user_path, 'NEO')
    new_neo_trigger = os.path.join(user_path, 'TRIGGER')

    w_dir = os.path.join(user_path, 'Wealins')
    w_ct = os.path.join(w_dir, 'CT')
    w_prep_upload = os.path.join(user_path, 'PROD')
    w_scope = os.path.join(w_dir, 'SCOPE')

    s_dir = os.path.join(user_path, 'Sogelife')
    s_ct = os.path.join(s_dir, 'CT')
    s_prep_upload = os.path.join(user_path, 'PROD')
    s_scope = os.path.join(s_dir, 'SCOPE')
    folder_paths_delete = [w_prep_upload, s_prep_upload]


def log_message(message):
    global user_path
    # Step 2: Write the message to the standard output
    print(str(message))
    # Step 3: Append the message to the log file
    # Step 2: Get the current date and time
    current_datetime = datetime.now()
    # Step 3: Format the date and time (optional)
    formatted_datetime = current_datetime.strftime('%Y-%m-%d %H:%M:%S')
    log_file_path = os.path.join(user_path, 'Output.log')
    with open(log_file_path, 'a') as log_file:
        log_file.write(formatted_datetime + ' - ' + str(message) + '\n')


def clean_up():
    global new_neo_dir, new_neo_output, new_neo_delta, new_neo_trigger, neo_dir, folder_paths_delete
    log_message('Rebuilding Directories...')
    try:
        for folder in folder_paths_delete:
            for item_name in os.listdir(folder):
                item_path = os.path.join(folder, item_name)
                if os.path.isfile(item_path):
                    os.remove(item_path)
    except Exception as e:
        logging.exception("An error occurred: %s", e)
        log_message(e)
        input("Hit Enter to Exit")
        exit(1)
    today_date = datetime.now().strftime('%d-%m-%Y')
    new_neo_dir = os.path.join(neo_dir, today_date)
    new_neo_delta = os.path.join(new_neo_dir, 'Delta')
    new_neo_output = os.path.join(new_neo_dir, 'Upload')
    try:
        os.makedirs(new_neo_dir)
        print(f"Directory created in NEO: {today_date}")
        os.chdir(new_neo_dir)
        os.makedirs(new_neo_delta)
        os.makedirs(new_neo_output)
    except FileExistsError:
        print(f"Directory already exists in NEO: {today_date}")


def file_from_ftp():
    global s_flag, w_flag, w_scope, s_scope
    log_message('Initialise SFTP parameters.')
    # SFTP connection parameters
    hostname = '78.141.185.241'
    username = 'FUNDINFO_SSH'
    password = 'Fu53aWA!q_PqPqa!'
    port = 22
    max_retries = 3
    retry_delay = 5  # seconds
    remote_path_wealins = '/TCP/Wealins/Scope'
    remote_path_sogelife = '/TCP/Sogelife/Scope'
    today_str_wealins = datetime.now().strftime("ExternalFunds_%Y%m%d")
    today_str_sogelife = (datetime.now() - timedelta(days=1)).strftime("Kiidocs_scope_%Y%m%d")

    # Function to perform the file check and download
    def check_and_download_file(sftp, today_str, local_path):
        try:
            for filename in sftp.listdir():
                if filename.startswith(today_str):
                    local_file_path = os.path.join(local_path, filename)
                    sftp.get(filename, local_file_path)
                    print(f"Downloaded {filename} to {local_file_path}")
                    return True
            return False
        except Exception as es:
            logging.exception("An error occurred: %s", es)
            log_message(es)
            input("Hit Enter to Exit")
            exit(1)

    # Main connection and retry logic
    for attempt in range(max_retries):
        try:
            # Create an SSH client instance
            ssh_client = paramiko.SSHClient()
            ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            ssh_client.connect(hostname, port=port, username=username, password=password)
            sftp = ssh_client.open_sftp()
            if s_flag == 0:
                sftp.chdir(remote_path_sogelife)
                log_message('Drill Down - SogeLife')
                if check_and_download_file(sftp, today_str_sogelife, s_scope):
                    print('SogeLife Download Completed.')
                    s_flag = 1

            if w_flag == 0:
                sftp.chdir(remote_path_wealins)
                log_message('Drill Down - Wealins')
                if check_and_download_file(sftp, today_str_wealins, w_scope):
                    print("Wealins Download Completed.")
                    w_flag = 1
            if s_flag == 1 and w_flag == 1:
                break

        except (paramiko.AuthenticationException, paramiko.SSHException, Exception) as e:
            logging.exception("An error occurred: %s", e)
            log_message(e)

            print(f"Attempt {attempt + 1} failed with error: {e}")
            if attempt < max_retries - 1:
                print(f"Retrying in {retry_delay} seconds...")
                time.sleep(retry_delay)
            else:
                print("Maximum retries reached. Exiting.")
                input("Hit Enter to Exit")
                exit(1)
        finally:
            if sftp:
                sftp.close()
            if ssh_client:
                ssh_client.close()


def download_efs_ct():
    global w_ct, s_ct
    log_message('CONNECTING TO EFS...')
    log_message('CONNECTING TO SWAGGER...')
    file_name = datetime.now().strftime("%d-%m-%Y")
    s_api = 'https://datafeeds.fefundinfo.com/api/v1/Feeds/e85c7488-fc77-479a-9d82-d008073bdc3f'
    w_api = 'https://datafeeds.fefundinfo.com/api/v1/Feeds/91453a9f-cbb1-498e-a98c-7baa3932f976'
    log_message(f'using feeds\ns_api-{s_api}\nw_api-{w_api}')

    def get_bearer_token():
        url = 'https://auth.fefundinfo.com/connect/token'
        headers = {'Content-Type': 'application/x-www-form-urlencoded'}
        data = {
            'client_id': 'EFS-migration-for-support',
            'client_secret': 'iz63fbucsQ9IEQKIC5eveeGpNlK8MfV',
            'grant_type': 'client_credentials',
            'scope': 'ssf-api-feed-read'
        }
        try:
            response = requests.post(url, headers=headers, data=data, timeout=TIMEOUT_DURATION)
            response.raise_for_status()
            log_message('AUTHENTICATION ACQUIRED')
            return response.json()['access_token']
        except Exception as e:
            logging.exception("An error occurred: %s", e)
            log_message(e)
            input("Hit Enter to Exit")
            exit(1)

    def get_blob(url, token):
        headers = {
            'Authorization': f'Bearer {token}',
            'accept': 'application/json'
        }
        try:
            response = requests.get(url, headers=headers, timeout=TIMEOUT_DURATION)
            response.raise_for_status()

            # Parse JSON response
            json_data = response.json()
            payload = json_data.get('payload', {})

            # Extract the values for 'isinsBlobName' and 'permissionContainerId'
            isins_blob_name = payload['isinsBlobName']
            permission_container_id = payload['permissionContainerId']

            return isins_blob_name, permission_container_id
        except Exception as e:
            logging.exception("An error occurred: %s", e)
            log_message(e)
            input("Hit Enter to Exit")
            exit(1)

    def download_file(url, token, filename, filepath):
        headers = {
            'Authorization': f'Bearer {token}',
            'accept': '*/*'
        }
        try:
            response = requests.get(url, headers=headers, timeout=TIMEOUT_DURATION)
            response.raise_for_status()
            os.chdir(filepath)
            with open(f'{filename}_CT.csv', 'wb') as file:
                file.write(response.content)
            log_message(f'Download Completed: {filepath}')
        except Exception as e:
            logging.exception("An error occurred: %s", e)
            log_message(e)
            input("Hit Enter to Exit")
            exit(1)

    # Get the bearer token
    log_message('AUTHENTICATION IN PROGRESS...')
    token = get_bearer_token()
    s_blob, s_container = get_blob(s_api, token)
    w_blob, w_container = get_blob(w_api, token)

    s_cust_file_url = f'https://datafeeds.fefundinfo.com/api/v1/CustomFiles/{s_blob}?permissionContainerId={s_container}'
    download_file(s_cust_file_url, token, file_name, s_ct)
    w_cust_file_url = f'https://datafeeds.fefundinfo.com/api/v1/CustomFiles/{w_blob}?permissionContainerId={w_container}'
    download_file(w_cust_file_url, token, file_name, w_ct)


def neo_w():
    global w_ct, w_prep_upload, w_scope
    try:
        w_ct_file_name = datetime.now().strftime("%d-%m-%Y_CT.csv")
        w_upload_file_name = datetime.now().strftime("%d-%m-%Y_Wealins-Upload.csv")
        today_str_wealins = datetime.now().strftime("ExternalFunds_%Y%m%d")
        # Load the CSV files into DataFrames
        custom_table_df = pd.read_csv(os.path.join(w_ct, w_ct_file_name))
        for item in os.listdir(w_scope):
            if today_str_wealins in item:
                w_scope_file_name = item
        scope_list_df = pd.read_excel(os.path.join(w_scope, w_scope_file_name))

        # Filter for ISINs that are 12 characters long
        custom_table_isins = custom_table_df[custom_table_df.iloc[:, 0].str.len() == 12].iloc[:, 0].drop_duplicates()
        scope_list_isins = scope_list_df.iloc[:, 0].str[:12].drop_duplicates()

        # Find ISINs that were added or removed
        added_isins = scope_list_isins[~scope_list_isins.isin(custom_table_isins)]
        processed_add_isins = added_isins.str.strip().loc[lambda x: x.str.len() == 12]
        removed_isins = custom_table_isins[~custom_table_isins.isin(scope_list_isins)]
        processed_removed_isins = removed_isins.str.strip().loc[lambda x: x.str.len() == 12]

        # Update the custom table DataFrame
        custom_table_df = custom_table_df[~custom_table_df.iloc[:, 0].isin(processed_removed_isins)].drop_duplicates()
        # Create a DataFrame from added_isins list with a column named 'ISIN'
        additions_df = pd.DataFrame(processed_add_isins, columns=['ISIN'])
        updated_custom_table_df = pd.concat([custom_table_df, additions_df], ignore_index=True)
        updated_custom_table_df = updated_custom_table_df.drop_duplicates(subset=[updated_custom_table_df.columns[0]])
        updated_custom_table_df = updated_custom_table_df.iloc[:, [0]]  # Select only the first column
        updated_custom_table_df.iloc[:, 0] = updated_custom_table_df.iloc[:, 0].str.strip().loc[
            lambda x: x.str.len() == 12]

        # Save the updated custom table to a new CSV file
        updated_custom_table_df.to_csv(os.path.join(w_prep_upload, w_upload_file_name), index=False, header=['ISIN'])
        updated_custom_table_df.to_csv(os.path.join(new_neo_output, w_upload_file_name), index=False, header=['ISIN'])
        log_message('Wealins Upload file written')
        # Create a DataFrame for added and removed ISINs
        delta_df = pd.DataFrame({
            'added': pd.Series(processed_add_isins).reset_index(drop=True),
            'removed': pd.Series(processed_removed_isins).reset_index(drop=True)
        })

        # Save the delta DataFrame to a CSV file
        delta_file_name = 'Wealins_Delta' + datetime.now().strftime('%d-%m-%Y') + '.csv'
        delta_df.to_csv(os.path.join(new_neo_delta, delta_file_name), index=False)
        log_message('Wealins Delta file written')
    except Exception as e:
        logging.exception("An error occurred: %s", e)
        log_message(e)
        input("Hit Enter to Exit")
        exit(1)


def neo_s():
    global s_ct, s_prep_upload, s_scope
    try:
        s_ct_file_name = datetime.now().strftime("%d-%m-%Y_CT.csv")
        s_upload_file_name = datetime.now().strftime("%d-%m-%Y_SogeLife-Upload.csv")
        today_str_sogelife = (datetime.now() - timedelta(days=1)).strftime("Kiidocs_scope_%Y%m%d")
        # Load the CSV files into DataFrames
        custom_table_df = pd.read_csv(os.path.join(s_ct, s_ct_file_name))
        for item in os.listdir(s_scope):
            if today_str_sogelife in item:
                s_scope_file_name = item
        scope_list_df = pd.read_csv(os.path.join(s_scope, s_scope_file_name))

        # Filter for ISINs that are 12 characters long
        custom_table_isins = custom_table_df[custom_table_df.iloc[:, 0].str.len() == 12].iloc[:, 0].drop_duplicates()
        scope_list_isins = scope_list_df[scope_list_df.iloc[:, 0].str.len() == 12].iloc[:, 0].drop_duplicates()

        # Find ISINs that were added or removed
        added_isins = scope_list_isins[~scope_list_isins.isin(custom_table_isins)]
        processed_add_isins = added_isins.str.strip().loc[lambda x: x.str.len() == 12]
        removed_isins = custom_table_isins[~custom_table_isins.isin(scope_list_isins)]
        processed_removed_isins = removed_isins.str.strip().loc[lambda x: x.str.len() == 12]
        # Create a DataFrame from added_isins list with a column named 'ISIN'
        additions_df = pd.DataFrame(processed_add_isins, columns=['ISIN'])
        updated_custom_table_df = pd.concat([custom_table_df, additions_df], ignore_index=True)
        updated_custom_table_df = updated_custom_table_df.drop_duplicates(subset=[updated_custom_table_df.columns[0]])
        updated_custom_table_df = updated_custom_table_df.iloc[:, [0]]
        updated_custom_table_df.iloc[:, 0] = updated_custom_table_df.iloc[:, 0].str.strip().loc[
            lambda x: x.str.len() == 12]

        # Save the updated custom table to a new CSV file
        updated_custom_table_df.to_csv(os.path.join(s_prep_upload, s_upload_file_name), index=False, header=['ISIN'])
        updated_custom_table_df.to_csv(os.path.join(new_neo_output, s_upload_file_name), index=False, header=['ISIN'])
        log_message('SogeLife upload file written')
        # Create a DataFrame for added and removed ISINs
        delta_df = pd.DataFrame({
            'added': pd.Series(processed_add_isins).reset_index(drop=True),
            'removed': pd.Series(processed_removed_isins).reset_index(drop=True)
        })

        # Save the delta DataFrame to a CSV file
        delta_file_name = 'SogeLife_Delta' + datetime.now().strftime('%d-%m-%Y') + '.csv'
        delta_df.to_csv(os.path.join(new_neo_delta, delta_file_name), index=False)
        log_message('SogeLife delta file written')
    except Exception as e:
        logging.exception("An error occurred: %s", e)
        log_message(e)
        input("Hit Enter to Exit")
        exit(1)

def triggers():
    global new_neo_trigger, new_neo_delta, s_flag, w_flag
    s_table = ""
    w_table = ""
    w_delta_file_name = 'Wealins_Delta' + datetime.now().strftime('%d-%m-%Y') + '.csv'
    s_delta_file_name = 'SogeLife_Delta' + datetime.now().strftime('%d-%m-%Y') + '.csv'
    s_trigger_file = os.path.join(new_neo_delta, s_delta_file_name)
    w_trigger_file = os.path.join(new_neo_delta, w_delta_file_name)
    # Read the content of the trigger files

    def generate_table_row(added, removed):
        return f"<tr><td>{added or '-'}</td><td>{removed or '-'}</td></tr>"

    def generate_table(content):
        lines = content.strip().split('\n')
        rows = [generate_table_row(*line.split(',')) for line in lines[1:] if line]  # Start from second row
        return f"<table><tr><th>Added</th><th>Removed</th></tr>{''.join(rows)}</table>"

    # Read the content of the trigger files
    if s_flag == 1:
        with open(s_trigger_file, 'r') as s_file:
            s_content = s_file.read()
            s_table = generate_table(s_content)
    if w_flag == 1:
        with open(w_trigger_file, 'r') as w_file:
            w_content = w_file.read()
            w_table = generate_table(w_content)

    # Get today's date in the format 'YYYY-MM-DD'
    today_date = datetime.now().strftime("%Y-%m-%d")

    html_content = f"""
    <html>
        <head>
            <style>
                table {{
                    border-collapse: collapse;
                }}
                th, td {{
                    border: 1px solid black;
                    text-align: center;
                }}
            </style>
        </head>
        <body>
            <h2>{today_date}</h2>
            <h2>SogeLife Delta</h2>
            {s_table}
            <h2>Wealins Delta</h2>
            {w_table}
        </body>
    </html>
    """

    # Write the HTML content to a new file
    os.chdir(new_neo_trigger)
    today = datetime.now().strftime('%d-%m-%Y')
    file_name = f'Trigger_{today}.html'
    with open(file_name, 'w') as html_file:
        html_file.write(html_content)
    log_message('Trigger Generated')


get_user_path()
clean_up()
# file_from_ftp()
# download_efs_ct()
neo_s()
neo_w()
triggers()


