# AERO - Automation Engine for Routine Operations
import os
import time
import requests
import pandas as pd
import shutil
from datetime import datetime, timedelta
import tkinter as tk
import glob
import csv
import datetime
import calendar
import webbrowser
import logging

import os
import requests
import json
import logging
import datetime
import sys
import openpyxl
import xml.etree.ElementTree as ET
import ast
import time
import uuid

TIMEOUT_DURATION = 30
# Configure logging
logging.basicConfig(filename='error.log', level=logging.ERROR, format='%(asctime)s:%(levelname)s:%(message)s')

token = ''
user_path = ''
EXCEL = ''
UPLOAD = ''
BF = ''
ARCHIVE = ''
PROD = ''
AIM = ''
CT = ''
PREP = ''
PREP_PREP_DCR_BANKDATA = ''
PREP_PREP_DCR_CARNEGIE = ''
PREP_PREP_DCR_DANSKEBANK = ''
PREP_PREP_DCR_SDC = ''
PREP_PREP_FILES = ''
PREP_PREP_Team_Trigger = ''
PROD_PROD_DCR_BANKDATA = ''
PROD_PROD_DCR_CARNEGIE = ''
PROD_PROD_DCR_DANSKEBANK = ''
PROD_PROD_DCR_SDC = ''
PROD_PROD_FILES = ''
PROD_PROD_Team_Trigger = ''
TRINITY = ''
folder_paths = []
delete_folder_paths = []
DCR_Folder_Paths = []
DCR_file_list = ['2_002_Aws-lambda_BankData',
                 '2_002_Aws-lambda_Danske', '2_002_Aws-lambda_SDC']
DCR_Clients = ['BankData', 'Danske', 'SDC']


def log_message(message):
    global user_path
    # Step 2: Write the message to the standard output
    print(str(message))
    # Step 3: Append the message to the log file
    # Step 2: Get the current date and time
    current_datetime = datetime.datetime.now()
    # Step 3: Format the date and time (optional)
    formatted_datetime = current_datetime.strftime('%Y-%m-%d %H:%M:%S')
    log_file_path = os.path.join(user_path, 'Output.log')
    with open(log_file_path, 'a') as log_file:
        log_file.write(formatted_datetime + ' - ' + str(message) + '\n')


def get_user_path():
    global UPLOAD, folder_paths, delete_folder_paths, user_path, BF, PROD, ARCHIVE, EXCEL, AIM, CT, PREP, \
        PREP_PREP_DCR_BANKDATA, PREP_PREP_DCR_CARNEGIE, PREP_PREP_DCR_DANSKEBANK, \
        PREP_PREP_DCR_SDC, PREP_PREP_FILES, PREP_PREP_Team_Trigger, \
        PROD_PROD_DCR_BANKDATA, PROD_PROD_DCR_CARNEGIE, PROD_PROD_DCR_DANSKEBANK, \
        PROD_PROD_DCR_SDC, PROD_PROD_FILES, PROD_PROD_Team_Trigger, TRINITY, DCR_Folder_Paths

    print('Get User Path')
    user_path = input("Please enter the OneDrive Path to 'AERO_BF'\nUser Path:")
    print(f'Path yo have entered is: {user_path}')
    log_message(f'User Path entered: {user_path}')
    AIM = os.path.join(user_path, 'AIM')
    BF = os.path.join(user_path, 'BF')
    EXCEL = os.path.join(user_path, 'AUTOMATE.xlsx')
    UPLOAD = os.path.join(user_path, 'AUTO_UPLOAD.xlsx')
    ARCHIVE = os.path.join(user_path, 'ARCHIVE')
    PROD = os.path.join(user_path, 'PROD')
    CT = os.path.join(user_path, 'CT')
    PREP = os.path.join(user_path, 'PREP')
    PREP_PREP_DCR_BANKDATA = os.path.join(user_path, r'PREP\PREP_DCR_BANKDATA')
    PREP_PREP_DCR_CARNEGIE = os.path.join(user_path, r'PREP\PREP_DCR_CARNEGIE')
    PREP_PREP_DCR_DANSKEBANK = os.path.join(user_path, r'PREP\PREP_DCR_DANSKEBANK')
    PREP_PREP_DCR_SDC = os.path.join(user_path, r'PREP\PREP_DCR_SDC')
    PREP_PREP_FILES = os.path.join(user_path, r'PREP\PREP_FILES')
    PREP_PREP_Team_Trigger = os.path.join(user_path, r'PREP\PREP_Team_Trigger')
    PROD_PROD_DCR_BANKDATA = os.path.join(user_path, r'PROD\PROD_DCR_BANKDATA')
    PROD_PROD_DCR_CARNEGIE = os.path.join(user_path, r'PROD\PROD_DCR_CARNEGIE')
    PROD_PROD_DCR_DANSKEBANK = os.path.join(user_path, r'PROD\PROD_DCR_DANSKEBANK')
    PROD_PROD_DCR_SDC = os.path.join(user_path, r'PROD\PROD_DCR_SDC')
    PROD_PROD_FILES = os.path.join(user_path, r'PROD\PROD_FILES')
    PROD_PROD_Team_Trigger = os.path.join(user_path, r'PROD\PROD_Team_Trigger')
    TRINITY = os.path.join(user_path, 'TRINITY')
    DCR_Folder_Paths = [PREP_PREP_DCR_BANKDATA, PREP_PREP_DCR_CARNEGIE, PREP_PREP_DCR_DANSKEBANK, PREP_PREP_DCR_SDC]
    # Set the folder path
    folder_paths = [
        AIM,
        BF,
        EXCEL,
        UPLOAD,
        ARCHIVE,
        PROD,
        CT,
        PREP,
        PREP_PREP_DCR_BANKDATA,
        PREP_PREP_DCR_CARNEGIE,
        PREP_PREP_DCR_DANSKEBANK,
        PREP_PREP_DCR_SDC,
        PREP_PREP_FILES,
        PREP_PREP_Team_Trigger,
        PROD_PROD_DCR_BANKDATA,
        PROD_PROD_DCR_CARNEGIE,
        PROD_PROD_DCR_DANSKEBANK,
        PROD_PROD_DCR_SDC,
        PROD_PROD_FILES,
        PROD_PROD_Team_Trigger,
        TRINITY
    ]
    delete_folder_paths = [
        AIM,
        CT,
        PREP,
        PREP_PREP_DCR_BANKDATA,
        PREP_PREP_DCR_CARNEGIE,
        PREP_PREP_DCR_DANSKEBANK,
        PREP_PREP_DCR_SDC,
        PREP_PREP_FILES,
        PREP_PREP_Team_Trigger,
        TRINITY,
        PROD_PROD_FILES
    ]


def ready_folders():
    global folder_paths, delete_folder_paths, user_path, BF, PROD, ARCHIVE, EXCEL, AIM, CT, PREP, \
        PREP_PREP_DCR_BANKDATA, PREP_PREP_DCR_CARNEGIE, PREP_PREP_DCR_DANSKEBANK, \
        PREP_PREP_DCR_SDC, PREP_PREP_FILES, PREP_PREP_Team_Trigger, \
        PROD_PROD_DCR_BANKDATA, PROD_PROD_DCR_CARNEGIE, PROD_PROD_DCR_DANSKEBANK, \
        PROD_PROD_DCR_SDC, PROD_PROD_FILES, PROD_PROD_Team_Trigger, TRINITY
    # Set the folder path
    root_path = os.path.join(user_path)
    # Check if the folder exists
    log_message('Checking Directory Architecture...\n')
    try:
        if not os.path.exists(root_path):
            print('ROOT DIRECTORY DOES NOT EXIST - ABORT PROCESS')
            print("Cannot continue - Rerun automation when AERO_BF is available.")
            input("Hit Enter to Exit")
            exit(1)
        else:
            log_message('ROOT DIRECTORY EXISTS - PATH RECONSTRUCTION IN PROGRESS')
            for item_name in os.listdir(root_path):
                if item_name == 'BF' or item_name == 'ARCHIVE' or item_name == 'PROD':
                    continue
                # Construct full path to the item
                item_path = os.path.join(root_path, item_name)
                # Check if the item is a directory
                if os.path.isdir(item_path):
                    # Delete the directory
                    shutil.rmtree(item_path)
            shutil.rmtree(PROD_PROD_FILES)
            for create_path in delete_folder_paths:
                os.mkdir(create_path)
        excel_path = EXCEL
        df = pd.read_excel(excel_path)
        log_message("INITIALISING - TRINITY")
        # Iterate over rows in the DataFrame
        os.chdir(TRINITY)
        for index, row in df.iterrows():
            api = row['API']
            os.mkdir(api)
        log_message('TRINITY IS READY TO ROCK AND ROLL!')
        log_message('PATH RECONSTRUCTION COMPLETED')
    except Exception as e:
        logging.exception("An error occurred: %s", e)
        log_message(e)
        input("Hit Enter to Exit")
        exit(1)


def download_aim():
    log_message('Download AIM files.')
    log_message('*** LOADING AIM URLs - PLEASE WAIT FOR BROWSER TO OPEN ***')
    time.sleep(3)
    current_date = datetime.datetime.now()
    today = current_date.strftime('%Y%m%d')
    yesterday = (current_date - datetime.timedelta(days=1)).strftime('%Y%m%d')
    friday = (current_date - datetime.timedelta(days=3)).strftime('%Y%m%d')
    is_monday = current_date.weekday() == calendar.MONDAY
    url_clients = ['bank-data', 'danskebank', 'sdc']
    try:
        for url_client in url_clients:
            if url_client == 'bank-data':
                if is_monday:
                    url = f'https://aim.fundconnect.com/Files/feed-files/{url_client}/clients-requests-emt/{friday}/'
                    webbrowser.open(url)
                else:
                    url = f'https://aim.fundconnect.com/Files/feed-files/{url_client}/clients-requests-emt/{yesterday}/'
                    webbrowser.open(url)
            else:
                url = f'https://aim.fundconnect.com/Files/feed-files/{url_client}/clients-requests-emt/{today}/'
                webbrowser.open(url)
    except Exception as e:
        logging.exception("An error occurred: %s", e)
        log_message(e)
        input("Hit Enter to Exit")
        exit(1)


def file_checks():
    global folder_paths, delete_folder_paths, user_path, BF, PROD, ARCHIVE, EXCEL, AIM, CT, PREP, \
        PREP_PREP_DCR_BANKDATA, PREP_PREP_DCR_CARNEGIE, PREP_PREP_DCR_DANSKEBANK, \
        PREP_PREP_DCR_SDC, PREP_PREP_FILES, PREP_PREP_Team_Trigger, \
        PROD_PROD_DCR_BANKDATA, PROD_PROD_DCR_CARNEGIE, PROD_PROD_DCR_DANSKEBANK, \
        PROD_PROD_DCR_SDC, PROD_PROD_FILES, PROD_PROD_Team_Trigger
    AUTOMATE_file = EXCEL
    BF_file = BF
    AIM_file = AIM
    log_message('SCANNING PATHS FOR NEW ENTRIES')
    # Show the message box
    user_response_AUTOMATE = input("Is AUTOMATE.xlsx available in path?\n(Y/N): ").strip().upper()
    if user_response_AUTOMATE == 'Y':
        user_response_AUTOMATE = True
    elif user_response_AUTOMATE == 'N':
        user_response_AUTOMATE = False
    else:
        print("Invalid input. Please enter 'Y' for Yes or 'N' for No.")
    # Optional: Perform actions based on the response
    if user_response_AUTOMATE:
        log_message("User chose 'Yes' for AUTOMATE condition check")
        if not os.path.isfile(AUTOMATE_file):
            print(f"File not found: {AUTOMATE_file}")
            log_message(f"Error - File not found: {AUTOMATE_file}")
            print("Cannot continue - Rerun automation when file is available.")
            input("Hit Enter to Exit")
            exit(1)
    else:
        log_message("User chose 'No' for AUTOMATE condition check")
        print("Cannot continue - Rerun automation when file is available.")
        log_message("Abort!")
        input("Hit Enter to Exit")
        exit(1)

    # Show the message box
    user_response_BF = input("Is Latest Billing Feed file available BF folder? \n(Y/N): ").strip().upper()
    if user_response_BF == 'Y':
        user_response_BF = True
    elif user_response_BF == 'N':
        user_response_BF = False
    else:
        print("Invalid input. Please enter 'Y' for Yes or 'N' for No.")
    # Optional: Perform actions based on the response
    if user_response_BF:
        log_message("User chose 'Yes' for BF condition check")
        # Get list of files in the directory
        files = [entry for entry in os.listdir(BF_file) if os.path.isfile(os.path.join(BF_file, entry))]

        # Check if there are any files
        if len(files) == 0:
            print(f"No files found in: {BF_file}")
            log_message(f"Error - No files found in: {BF_file}")
            print("Cannot continue - Rerun automation when file is available.")
            input("Hit Enter to Exit")
            exit(1)

        # Find the latest file
        latest_file = max(files, key=lambda x: os.path.getmtime(os.path.join(BF_file, x)))

        # Get the modification time of the latest file
        mod_time = datetime.datetime.fromtimestamp(os.path.getmtime(os.path.join(BF_file, latest_file)))

        # Get the current time
        current_time = datetime.datetime.now()

        # Check if the latest file was created today
        if mod_time.date() == current_time.date():
            log_message(f"The latest file, {latest_file}, was created today.")
        else:
            log_message(f"The latest file, {latest_file}, was not created today.")
            print(f"The latest file, {latest_file}, was not created today.")
            print("Cannot continue - Rerun automation when file is available.")
            input("Hit Enter to Exit")
            exit(1)
    else:
        log_message("User chose 'No' for BF condition check")
        print("Cannot continue - Rerun automation when file is available.")
        log_message("Abort!")
        input("Hit Enter to Exit")
        exit(1)

        # Show the message box
    user_response_AIM = input("DCR files moved to AIM folder? \n(Y/N): ").strip().upper()
    if user_response_AIM == 'Y':
        user_response_AIM = True
    elif user_response_AIM == 'N':
        user_response_AIM = False
    else:
        print("Invalid input. Please enter 'Y' for Yes or 'N' for No.")
    # Optional: Perform actions based on the response
    if user_response_AIM:
        log_message("User chose 'Yes' for AIM check")
        # Get list of files in the directory
        files = [entry for entry in os.listdir(AIM_file) if os.path.isfile(os.path.join(AIM_file, entry))]

        # Check if there are any files
        if len(files) == 0:
            print(f"No files found in: {AIM_file}")
            log_message(f"Error - No files found in: {AIM_file}")
            print("Cannot continue - Rerun automation when file is available.")

        # Find the latest file
        latest_file = max(files, key=lambda x: os.path.getmtime(os.path.join(AIM_file, x)))

        # Get the modification time of the latest file
        mod_time = datetime.datetime.fromtimestamp(os.path.getmtime(os.path.join(AIM_file, latest_file)))

        # Get the current time
        current_time = datetime.datetime.now()

        # Check if the latest file was created today
        if mod_time.date() == current_time.date():
            log_message(f"The latest file, {latest_file}, was created today.")
        else:
            log_message(f"The latest AIM file, {latest_file}, was not created today.")
            print(f"The latest AIM file, {latest_file}, was not created today.")
            log_message("Cannot continue - Rerun automation when file is available.")
            input("Hit Enter to Exit")
            exit(1)
    else:
        log_message("User chose 'No' for AIM check")
        print("Cannot continue - Rerun automation when file is available.")
        log_message("Abort!")
        input("Hit Enter to Exit")
        exit(1)


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
        return response.json()['access_token']
    except Exception as e:
        logging.exception("An error occurred: %s", e)
        log_message(e)
        input("Hit Enter to Exit")
        exit(1)


def upload_file(file_path, permission_container_id):
    global token
    url = f'https://datafeeds.fefundinfo.com/api/v1/CustomFiles?permissionContainerId={permission_container_id}'
    headers = {
        'accept': 'application/json',
        'Authorization': f'Bearer {token}'
    }
    files = {
        'file': (file_path, open(file_path, 'rb'), 'text/csv')
    }

    response = requests.post(url, headers=headers, files=files)

    if response.status_code == 200:
        response_text = json.dumps(response.json(), indent=4)  # Format JSON output
        data = json.loads(response_text)
        success = data['payload'].get('itemsCount', '')
        fail = data['payload'].get('invalidItemsCount', '')
        blob = data['payload'].get('targetBlobName', '')
        return success, fail, blob
    else:
        log_message(f"UPLOAD FAILED with status code {response.status_code}: {response.text}")


def read_feed(feed_id):
    log_message("Reading Feed Details")
    url = f"https://datafeeds.fefundinfo.com/api/v1/Feeds/{feed_id}"
    headers = {
        'accept': 'application/json',
        'Authorization': f'Bearer {token}',  # Replace YOUR_TOKEN with your actual token
        'Content-Type': 'application/json',
        'Cache-Control': 'no-cache',
        'Pragma': 'no-cache'
    }
    params = {'_': str(uuid.uuid4())}  # Generate a unique parameter to avoid caching
    response = requests.get(url, headers=headers, params=params)

    if response.status_code == 200:
        response_text = json.dumps(response.json(), indent=4)  # Format JSON output
        data = json.loads(response_text)
        return data

    else:
        log_message(f"Request failed with status code {response.status_code}: {response.text}")


def save_feed(payload):
    log_message("Saving Feed Details")
    url = f"https://datafeeds.fefundinfo.com/api/v1/Feeds/save"
    headers = {
        'accept': 'application/json',
        'Authorization': f'Bearer {token}',  # Replace YOUR_TOKEN with your actual token
        'Content-Type': 'application/json',
    }
    data = payload

    response = requests.post(url, headers=headers, json=data)
    if response.status_code == 200:
        response_text = json.dumps(response.json(), indent=4)  # Format JSON output
        data = json.loads(response_text)
        Save_Status = data["payload"]["passed"]
        print(f'Status: {Save_Status}')

    else:
        log_message(f"SAVE Request failed with status code {response.status_code}: {response.text}")


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
        log_message(f'Download Completed: {filename}')
    except Exception as e:
        logging.exception("An error occurred: %s", e)
        log_message(e)
        input("Hit Enter to Exit")
        exit(1)


def populate_urls_from_excel():
    global token, folder_paths, delete_folder_paths, user_path, BF, PROD, ARCHIVE, EXCEL, AIM, CT, PREP, \
        PREP_PREP_DCR_BANKDATA, PREP_PREP_DCR_CARNEGIE, PREP_PREP_DCR_DANSKEBANK, \
        PREP_PREP_DCR_SDC, PREP_PREP_FILES, PREP_PREP_Team_Trigger, \
        PROD_PROD_DCR_BANKDATA, PROD_PROD_DCR_CARNEGIE, PROD_PROD_DCR_DANSKEBANK, \
        PROD_PROD_DCR_SDC, PROD_PROD_FILES, PROD_PROD_Team_Trigger
    # Read the Excel file
    excel_path = EXCEL
    download_path = CT
    df = pd.read_excel(excel_path)
    log_message('CONNECTING TO EFS...')
    log_message("CONNECTING TO SWAGGER...")
    # Get the bearer token
    token = get_bearer_token()
    log_message("IDENTITY APPROVED - EFS-migration-for-support - WE CAN CONTINUE")
    time.sleep(2)
    log_message("Initialising Custom Table Downloads for 18 feeds (Default Timeout 30 Sec)")
    # Iterate over rows in the DataFrame
    try:
        for index, row in df.iterrows():
            file_count = index + 1
            log_message(str(file_count))
            log_message('-----------')
            feed_name = row['FeedName']
            api = row['API']
            log_message(f'Feed Name: {feed_name}')
            log_message(f'API: {api}')

            # Get Feed Blob and Container
            feed_url = f'https://datafeeds.fefundinfo.com/api/v1/Feeds/{api}'
            blob, container = get_blob(feed_url, token)
            log_message(f'Blob: {blob}')
            log_message(f'Container: {container}')

            # Download the files using the formatted URLs
            custom_file_url = f'https://datafeeds.fefundinfo.com/api/v1/CustomFiles/{blob}?permissionContainerId={container}'
            download_file(custom_file_url, token, api, download_path)
    except Exception as e:
        logging.exception("An error occurred: %s", e)
        log_message(e)
        input("Hit Enter to Exit")
        exit(1)


def clean_up():
    global folder_paths, delete_folder_paths, user_path, BF, PROD, ARCHIVE, EXCEL, AIM, \
        CT, PREP, \
        PREP_PREP_DCR_BANKDATA, PREP_PREP_DCR_CARNEGIE, PREP_PREP_DCR_DANSKEBANK, \
        PREP_PREP_DCR_SDC, PREP_PREP_FILES, PREP_PREP_Team_Trigger, \
        PROD_PROD_DCR_BANKDATA, PROD_PROD_DCR_CARNEGIE, PROD_PROD_DCR_DANSKEBANK, \
        PROD_PROD_DCR_SDC, PROD_PROD_FILES, PROD_PROD_Team_Trigger, TRINITY, DCR_file_list, DCR_Clients

    def get_file_for_day(path, day):
        # Format the date to match expected filename pattern
        date_str = day.strftime('%Y-%m-%d')
        # Find file with the matching date in its name
        files = glob.glob(os.path.join(path, f'*{date_str}*.csv'))
        return max(files, key=os.path.getmtime) if files else None

    # Get the current date and time
    current_date = datetime.datetime.now()
    # Check if today is Monday (0 is Monday, 6 is Sunday)
    is_monday = current_date.weekday() == calendar.MONDAY
    if is_monday:
        log_message('Cleaning Billing Feed - IS MONDAY')

        # Calculate the dates for Saturday, Sunday, and Monday
        saturday = current_date - datetime.timedelta(days=2)
        sunday = current_date - datetime.timedelta(days=1)
        monday = current_date

        # Get the file paths for the required days
        saturday_file = get_file_for_day(BF, saturday)
        sunday_file = get_file_for_day(BF, sunday)
        monday_file = get_file_for_day(BF, monday)

        # List to store valid ISINs from all three files
        valid_isins = set()

        # Process files for Saturday, Sunday, and Monday
        try:
            for file in [saturday_file, sunday_file, monday_file]:
                if file:
                    with open(file, newline='', mode='r') as csvfile:
                        reader = csv.DictReader(csvfile, delimiter='|')
                        for row in reader:
                            isin = row['ISIN'].strip()
                            if '_' not in isin and len(isin) == 12:
                                valid_isins.add(isin)

            # Write the valid ISINs to the output CSV
            output_csv_path = os.path.join(PREP, 'clean_bf.csv')
            with open(output_csv_path, 'w', newline='') as csvfile:
                writer = csv.writer(csvfile, delimiter='|')
                for isin in valid_isins:
                    writer.writerow([isin])
        except Exception as e:
            logging.exception("An error occurred: %s", e)
            log_message(e)
            input("Hit Enter to Exit")
            exit(1)

    log_message('Cleaning Billing Feed - NOT MONDAY')
    # Pick latest BF file from BF folder
    csv_files = glob.glob(os.path.join(BF, '*.csv'))
    # Find the latest file based on modification time
    latest_BF_file = max(csv_files, key=os.path.getmtime)
    valid_isins = set()
    output_csv_path = os.path.join(PREP, 'clean_bf.csv')
    try:
        with open(latest_BF_file, newline='', mode='r') as csvfile:
            reader = csv.DictReader(csvfile, delimiter='|')
            for row in reader:
                isin = row['ISIN'].strip()
                if '_' not in isin and len(isin) == 12:
                    valid_isins.add(isin)

        with open(output_csv_path, 'w', newline='') as outfile:
            writer = csv.writer(outfile)
            writer.writerow(['ISIN'])  # Write header
            for isin in valid_isins:
                writer.writerow([isin])  # Write each ISIN value

        # Iterate over all the entries in the target directory to copy the BF
        for entry in os.listdir(TRINITY):
            subdirectory_path = os.path.join(TRINITY, entry)

            # Check if the entry is a directory
            if os.path.isdir(subdirectory_path):
                # Construct the full destination file path
                destination_file_path = os.path.join(subdirectory_path, 'clean_bf.csv')

                # Copy the source file to the destination
                shutil.copy(output_csv_path, destination_file_path)
        log_message(f'Billing Feed Cleaned')

        # Iterate over all the entries in the target directory to move the CT
        excel_path = EXCEL
        df = pd.read_excel(excel_path)
        log_message("Writing to TRINITY")
        # Iterate over rows in the DataFrame
        AIM_list = []
        for entry in os.listdir(AIM):
            full_path = os.path.join(AIM, entry)
            if os.path.isfile(full_path):
                AIM_list.append(full_path)
        CT_list = []
        for entry in os.listdir(CT):
            full_path = os.path.join(CT, entry)
            if os.path.isfile(full_path):
                CT_list.append(entry)
        TRINITY_LIST = [name for name in os.listdir(TRINITY) if os.path.isdir(os.path.join(TRINITY, name))]
        for client in DCR_Clients:
            for index, row in df.iterrows():
                api = row['API']
                for CT in CT_list:
                    if api in CT.lower():
                        shutil.copy(CT, os.path.join(TRINITY, api, f'{api}_CT.csv'))
            for index, row in df.iterrows():
                api = row['API']
                feed_name = row['FeedName']
                for Trinity_name in TRINITY_LIST:
                    if api in Trinity_name:
                        for file in DCR_file_list:
                            if client.lower() in file.lower() and client.lower() in feed_name.lower():
                                for aim_file in AIM_list:
                                    if client.lower() in aim_file.lower():
                                        shutil.copy(aim_file, os.path.join(TRINITY, api, f'{client}_AWS_LAMBDA.csv'))
    except Exception as e:
        logging.exception("An error occurred: %s", e)
        log_message(e)
        input("Hit Enter to Exit")
        exit(1)


def compile_trinity():
    log_message('TRINITY IS POWERING UP')
    global folder_paths, delete_folder_paths, user_path, BF, PROD, ARCHIVE, EXCEL, AIM, \
        CT, PREP, \
        PREP_PREP_DCR_BANKDATA, PREP_PREP_DCR_CARNEGIE, PREP_PREP_DCR_DANSKEBANK, \
        PREP_PREP_DCR_SDC, PREP_PREP_FILES, PREP_PREP_Team_Trigger, \
        PROD_PROD_DCR_BANKDATA, PROD_PROD_DCR_CARNEGIE, PROD_PROD_DCR_DANSKEBANK, \
        PROD_PROD_DCR_SDC, PROD_PROD_FILES, PROD_PROD_Team_Trigger, TRINITY, DCR_file_list, DCR_Folder_Paths

    Trinity_folders = os.listdir(TRINITY)
    log_message('READING TRINITY ARCHITECTURE')
    try:
        for folder in Trinity_folders:
            flag = 0
            custom_table = ''
            billing_feed = ''
            dcr_report = ''
            ct_data = []
            added_isin = []
            removed_isin = []
            bf_data = []
            upload_path = ''
            folder_path = os.path.join(TRINITY, folder)
            entries = os.listdir(folder_path)
            file_count = len([entry for entry in entries if os.path.isfile(os.path.join(folder_path, entry))])
            if file_count == 3:
                for entry in entries:
                    if 'CT' in entry:
                        custom_table = os.path.join(folder_path, entry)
                    if 'LAMBDA' in entry:
                        dcr_report = os.path.join(folder_path, entry)
                    if 'clean' in entry:
                        billing_feed = os.path.join(folder_path, entry)
                with open(custom_table, mode='r', newline='') as ct_file:
                    csv_reader = csv.reader(ct_file)
                    next(csv_reader)
                    for row in csv_reader:
                        ct_data.append(row[0])
                with open(dcr_report, mode='r', newline='') as dcr_file:
                    csv_reader = csv.reader(dcr_file, delimiter='|')
                    next(csv_reader)
                    for row in csv_reader:
                        if row[0].strip():
                            added_isin.append(row[0])
                        removed_isin.append(row[1])
                    if added_isin:
                        flag = 1
                with open(billing_feed, mode='r', newline='') as bf_file:
                    csv_reader = csv.reader(bf_file)
                    next(csv_reader)
                    for row in csv_reader:
                        bf_data.append(row[0])
                # Combine data and remove duplicates
                combined_data = list(set(ct_data + added_isin + bf_data))

                # Remove items in removed_isins from combined_data
                upload_data_set = [isin for isin in combined_data if isin not in removed_isin]

                excel_path = EXCEL
                df = pd.read_excel(excel_path)
                for index, row in df.iterrows():
                    api = row['API']
                    feed_name = row['FeedName']
                    perm_cont = row['Perm_Cont']
                    if api == folder:
                        upload_path = os.path.join(PREP_PREP_FILES, f'{perm_cont}.csv')
                        if flag == 1:
                            for client in DCR_Clients:
                                if client.lower() in feed_name.lower():
                                    folder_list = os.listdir(PREP)
                                    for prep_folder in folder_list:
                                        if client.lower() in prep_folder.lower():
                                            # Get the current date
                                            current_date = datetime.datetime.now()
                                            # Format the date to dd-mm-yyyy
                                            formatted_date = current_date.strftime("%d-%m-%Y")
                                            shutil.copy(dcr_report, os.path.join(PREP, prep_folder,
                                                                                 f'{client}-DCR-{formatted_date}.csv'))

                # Save upload_data_set to a new file
                print(upload_path)
                with open(upload_path, mode='w', newline='') as file_out:
                    writer = csv.writer(file_out)
                    writer.writerow(['ISIN'])  # Write header
                    for isin in upload_data_set:
                        isin = isin.strip()
                        if '_' not in isin:
                            if len(isin) == 12:
                                writer.writerow([isin])  # Write each ISIN
                        else:
                            print(f'Excluded - ISIN is not valid: {isin}')

            if file_count == 2 and (
                    folder == '6265ee8c-9b36-4811-9cb7-b39e0757a779' or folder == 'fd1ff713-0119-4755-9fbb-cbae01343cf5' or folder == 'd74f1270-0018-4219-9bca-ba2798fb8e57' or folder == '033f3ec2-fdc0-43e5-9bf9-ece8577cf0c3'):
                for entry in entries:
                    if 'CT' in entry:
                        custom_table = os.path.join(folder_path, entry)
                    if 'clean' in entry:
                        billing_feed = os.path.join(folder_path, entry)
                with open(custom_table, mode='r', newline='') as ct_file:
                    csv_reader = csv.reader(ct_file)
                    next(csv_reader)
                    for row in csv_reader:
                        ct_data.append(row[0])
                with open(billing_feed, mode='r', newline='') as bf_file:
                    csv_reader = csv.reader(bf_file)
                    next(csv_reader)
                    for row in csv_reader:
                        bf_data.append(row[0])
                # Combine data and remove duplicates
                combined_data = list(set(ct_data + bf_data))

                # Remove items in removed_isins from combined_data
                upload_data_set = combined_data

                excel_path = EXCEL
                df = pd.read_excel(excel_path)
                for index, row in df.iterrows():
                    api = row['API']
                    feed_name = row['FeedName']
                    perm_cont = row['Perm_Cont']
                    if api == folder:
                        upload_path = os.path.join(PREP_PREP_FILES, f'{perm_cont}.csv')

                # Save upload_data_set to a new file
                print(upload_path)
                with open(upload_path, mode='w', newline='') as file_out:
                    writer = csv.writer(file_out)
                    writer.writerow(['ISIN'])  # Write header
                    for isin in upload_data_set:
                        isin = isin.strip()
                        if '_' not in isin:
                            if len(isin) == 12:
                                writer.writerow([isin])  # Write each ISIN
                        else:
                            print(f'Excluded - ISIN is not valid: {isin}')
            if file_count == 2 and (
                    folder == 'd8267ece-80dd-43da-a7b4-c22a0bb9585c' or folder == 'f6d6bff0-d3df-4cf1-a4e5-824ba227003d' or folder == 'e27caa39-09d9-4662-a6e2-58804bd7e9ba'):
                for entry in entries:
                    if 'clean' in entry:
                        billing_feed = os.path.join(folder_path, entry)
                with open(billing_feed, mode='r', newline='') as bf_file:
                    csv_reader = csv.reader(bf_file)
                    next(csv_reader)
                    for row in csv_reader:
                        bf_data.append(row[0])
                # Combine data and remove duplicates
                combined_data = list(set(bf_data))

                # Remove items in removed_isins from combined_data
                upload_data_set = combined_data

                excel_path = EXCEL
                df = pd.read_excel(excel_path)
                for index, row in df.iterrows():
                    api = row['API']
                    feed_name = row['FeedName']
                    perm_cont = row['Perm_Cont']
                    if api == folder:
                        upload_path = os.path.join(PREP_PREP_FILES, f'{perm_cont}.csv')

                # Save upload_data_set to a new file
                print(upload_path)
                with open(upload_path, mode='w', newline='') as file_out:
                    writer = csv.writer(file_out)
                    writer.writerow(['ISIN'])  # Write header
                    for isin in upload_data_set:
                        isin = isin.strip()
                        if '_' not in isin:
                            if len(isin) == 12:
                                writer.writerow([isin])  # Write each ISIN
                        else:
                            print(f'Excluded - ISIN is not valid: {isin}')
        # List all files in the source directory
        log_message('CONSTRUCTING ARCHIVE')
        current_date = datetime.datetime.now()
        # Format the date to dd-mm-yyyy
        formatted_date = current_date.strftime("%d-%m-%Y")
        files = os.listdir(PREP_PREP_FILES)
        os.chdir(ARCHIVE)
        os.mkdir(formatted_date)
        destination_dir = os.path.join(ARCHIVE, formatted_date)
        # Copy each file to the destination directory
        for file in files:
            src_file_path = os.path.join(PREP_PREP_FILES, file)
            dest_file_path = os.path.join(destination_dir, file)
            # Check if it's a file and not a directory
            if os.path.isfile(src_file_path):
                shutil.copy(src_file_path, dest_file_path)
        log_message('ARCHIVE IS SAVED')
    except Exception as e:
        logging.exception("An error occurred: %s", e)
        log_message(e)
        input("Hit Enter to Exit")
        exit(1)


def teams_trigger():
    log_message('POPULATING DATA ENTRY TABLE')
    global UPLOAD, folder_paths, delete_folder_paths, user_path, BF, PROD, ARCHIVE, EXCEL, AIM, \
        CT, PREP, \
        PREP_PREP_DCR_BANKDATA, PREP_PREP_DCR_CARNEGIE, PREP_PREP_DCR_DANSKEBANK, \
        PREP_PREP_DCR_SDC, PREP_PREP_FILES, PREP_PREP_Team_Trigger, \
        PROD_PROD_DCR_BANKDATA, PROD_PROD_DCR_CARNEGIE, PROD_PROD_DCR_DANSKEBANK, \
        PROD_PROD_DCR_SDC, PROD_PROD_FILES, PROD_PROD_Team_Trigger, TRINITY, DCR_file_list, DCR_Folder_Paths
    current_date = datetime.datetime.now()
    minus_1 = current_date - timedelta(days=1)
    minus_3 = current_date - timedelta(days=3)
    # Format the date to dd-mm-yyyy
    today = current_date.strftime("%d-%m-%Y")
    yesterday = minus_1.strftime("%d-%m-%Y")
    friday = minus_3.strftime("%d-%m-%Y")
    # Define the directories
    is_monday = current_date.weekday() == calendar.MONDAY
    try:
        if is_monday:
            dir1 = os.path.join(ARCHIVE, friday)
            dir2 = os.path.join(ARCHIVE, today)
        else:
            dir1 = os.path.join(ARCHIVE, yesterday)
            dir2 = os.path.join(ARCHIVE, today)

        # Get the list of CSV files in the first directory
        csv_files = [f for f in os.listdir(dir1) if f.endswith('.csv')]

        # Initialize HTML content with a table
        html_content = "<html><body><table border='1'>"
        html_content += "<tr><th>File</th><th>API</th><th>Added ISIN</th><th>Removed ISIN</th><th>CT Uploaded</th></tr>"
        # Loop through each CSV file
        for csv_file in csv_files:
            upload_ct_flag = 'YES'
            perm_id = os.path.splitext(csv_file)[0]
            cont_upload = 0
            success = 0
            fail = 0
            blob = ''
            # Construct file paths
            file_path1 = os.path.join(dir1, csv_file)
            file_path2 = os.path.join(dir2, csv_file)

            # Read the CSV files into pandas DataFrames
            df1 = pd.read_csv(file_path1)
            df2 = pd.read_csv(file_path2)

            # Extract the first column (assuming the column name is 'ISIN')
            isin1 = set(df1.iloc[:, 0].unique())
            isin2 = set(df2.iloc[:, 0].unique())

            # Compare the values
            added_isin = isin2 - isin1
            removed_isin = isin1 - isin2

            # Count of ISINs
            added_isin_count = len(added_isin)
            removed_isin_count = len(removed_isin)
            if added_isin_count == 0 and removed_isin_count == 0:
                upload_ct_flag = 'NO'
            else:
                log_message(f'Uploading feed for {perm_id}')
                upload_path = UPLOAD
                dfup = pd.read_excel(upload_path)
                for index, row in dfup.iterrows():
                    perm_cont = row['Perm_Cont']
                    feed_id = row['Feed_API']
                    if perm_id == perm_cont:
                        if cont_upload == 0:
                            success, fail, blob = upload_file(file_path2, perm_id)
                            cont_upload = 1
                        feed_data = read_feed(feed_id)
                        feed_data["payload"]["uploadedIdentifiersFileName"] = perm_cont
                        feed_data["payload"]["uploadedIdentifiersSuccessCount"] = success
                        feed_data["payload"]["uploadedIdentifiersInvalidCount"] = fail
                        feed_data["payload"]["isinsBlobName"] = blob
                        save_feed(feed_data['payload'])

            # Get the API
            excel_path = EXCEL
            df = pd.read_excel(excel_path)
            for index, row in df.iterrows():
                api = row['API']
                per_con = row['Perm_Cont']
                if csv_file == f'{per_con}.csv':
                    # Output the results
                    html_content += f"<tr><td>{csv_file}</td><td>{api}</td><td>{added_isin_count}</td><td>{removed_isin_count}</td><td>{upload_ct_flag}</td></tr>"
        html_content += "</table></body></html>"
        # Save the HTML content to a file
        with open(os.path.join(PREP_PREP_Team_Trigger, f'{today}.html'), 'w') as file:
            file.write(html_content)
        log_message('DATA ENTRY COMPLETED')
    except Exception as e:
        logging.exception("An error occurred: %s", e)
        log_message(e)
        input("Hit Enter to Exit")
        exit(1)


def move_to_prod():
    log_message('PUSH TO PRODUCTION')
    global folder_paths, delete_folder_paths, user_path, BF, PROD, ARCHIVE, EXCEL, AIM, \
        CT, PREP, \
        PREP_PREP_DCR_BANKDATA, PREP_PREP_DCR_CARNEGIE, PREP_PREP_DCR_DANSKEBANK, \
        PREP_PREP_DCR_SDC, PREP_PREP_FILES, PREP_PREP_Team_Trigger, \
        PROD_PROD_DCR_BANKDATA, PROD_PROD_DCR_CARNEGIE, PROD_PROD_DCR_DANSKEBANK, \
        PROD_PROD_DCR_SDC, PROD_PROD_FILES, PROD_PROD_Team_Trigger, TRINITY, DCR_file_list, DCR_Folder_Paths
    # Get the current date
    try:
        all_items = os.listdir(PREP)
        PREP_Folders = [item for item in all_items if os.path.isdir(os.path.join(PREP, item))]
        print(PREP_Folders)
        PROD_Folders = os.listdir(PROD)
        print(PROD_Folders)
        for prep_folder, prod_folder in zip(PREP_Folders, PROD_Folders):
            prep_folder = os.path.join(PREP, prep_folder)
            prod_folder = os.path.join(PROD, prod_folder)
            files = os.listdir(prep_folder)
            for file in files:
                src_file_path = os.path.join(prep_folder, file)
                dest_file_path = os.path.join(prod_folder, file)
                print(src_file_path, dest_file_path)
                # Check if it's a file and not a directory
                if os.path.isfile(src_file_path):
                    shutil.copy(src_file_path, dest_file_path)
    except Exception as e:
        logging.exception("An error occurred: %s", e)
        log_message(e)
        input("Hit Enter to Exit")
        exit(1)


# Usage:
print('Logging Enabled\n')
print('Welcome to Automation Engine for Routine Operations - AERO_BF\n')
print('Please follow the below steps.\n')
print()
get_user_path()
ready_folders()
download_aim()
file_checks()
populate_urls_from_excel()
clean_up()
compile_trinity()
# Show the message box
user_response_mtp = input("INITIATE UPLOAD?\n(Y/N):").strip().upper()
# Optional: Perform actions based on the response
if user_response_mtp in ('Y', 'y'):
    log_message("User chose 'Yes' for UPLOAD condition check")
    teams_trigger()
    move_to_prod()

else:
    log_message("User chose 'No' for MTP/UPLOAD condition check")
    print("Cannot continue without MOVE TO PROD")
    log_message("Abort!")

print(r'Logs saved to C:\Output.log')
print('AUTOMATION SCRIPT RUN SUCCESS')
input('HIT ENTER TO EXIT')

# Create a root window
root = tk.Tk()
# Hide the root window
root.withdraw()
# Destroy the root window if not needed
root.destroy()
