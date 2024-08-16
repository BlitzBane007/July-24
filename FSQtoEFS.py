import os
import requests
import json
import pandas as pd
import logging
import datetime
import sys

TIMEOUT_DURATION = 30

logging.basicConfig(filename='error.log', level=logging.ERROR, format='%(asctime)s:%(levelname)s:%(message)s')

user_path = r'C:\Users\Aditya.Apte\OneDrive - FE fundinfo\Desktop\Desktop Icons\Aditya Apte\FSQtoEFS'
EXCEL = ''

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


def get_user():
    global user_path, EXCEL
    user_path = input("Please enter the OneDrive Path to 'FSQtoEFS'\nUser Path:")
    print("Thank you!")
    EXCEL = os.path.join(user_path, 'Masterlist.xlsx')
    print(f'Excel path built: {EXCEL}')


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



def Create_Feeds():
    counter = int(input("Please enter the number of iterations:"))
    if not 1 <= counter <= 1000:
        print("Number must be between 1 and 1000.")
        input("Hit ENTER to Exit!")
        sys.exit(1)
    # Read the existing Excel file into a DataFrame
    df_existing = pd.read_excel(EXCEL, dtype=str)
    # Iterate over DataFrame rows as (index, Series) pairs
    for index, row in df_existing.iterrows():
        # Check if 'Status' is blank
        if pd.isna(row['Status']):
            # Increment counter
            counter += 1
            # Break loop after 10 iterations
            if counter == 10:
                break
            # Access data
            Sub_ID = row['Sub_ID']
            print("=============================")
            print(f"EXECUTING SUB_ID : {Sub_ID}")
            feed_type = row['Feed_Type']
            url = f"https://fsqtoefsmigrationtooleuwliv.azurewebsites.net/FSQMigration?subscriptionId={Sub_ID}&feedDataTypeToBeOnbaoarded={feed_type}&isUploadCustomUnits=false"
            headers = {"accept": "*/*"}
            print("Posting Request to Server")
            print("Waiting for Response")
            response = requests.post(url, headers=headers)
            # Parse the JSON response
            response_json = json.loads(response.text)
            print("Response received!")
            Feed_ID = response_json['payload']['FeedID']

            token = get_bearer_token()
            url = f'https://datafeeds.fefundinfo.com/api/v1/Feeds/{Feed_ID}'
            headers = {
                'Authorization': f'Bearer {token}',
                'accept': 'application/json'
            }
            try:
                response_EFS = requests.get(url, headers=headers, timeout=TIMEOUT_DURATION)

                json_response_EFS = response_EFS.json()
                # Parse the JSON response
                response_json_EFS = json.loads(response_EFS.text)
                selected_fields_count = len(json_response_EFS["payload"]["selectedFields"])
                EFS_unit_count = json_response_EFS["payload"]["uploadedIdentifiersSuccessCount"]+response_json_EFS["payload"]["uploadedIdentifiersInvalidCount"]
                if(EFS_unit_count == 0):
                    filters_used = json_response_EFS["payload"]["filters"]
                    # Define the URL
                    kusto_url = "https://datafeeds.fefundinfo.com/api/v1/Containers/KustoFeedFilter"

                    # Define the headers
                    kusto_headers = {
                        "accept": "application/json",
                        "Authorization": f"Bearer {token}",  # Add your token here
                        "Content-Type": "application/json"
                    }

                    # Define the JSON data
                    kusto_data = {
                        "isinBlobFileId": "",
                        "citicodeBlobFileId": "",
                        "feedConfigType": "FundData",
                        "filters": filters_used,
                        "isWholeOfMarket": False
                    }

                    # Make a POST request
                    kusto_response = requests.post(kusto_url, headers=kusto_headers, json=kusto_data)
                    kusto_response_json = kusto_response.json()
                    kusto_payload = kusto_response_json["payload"]

                    # Define the URL
                    citicode_count_url = "https://datafeeds.fefundinfo.com/api/v1/Containers/7e93b1dd-089f-4239-8307-b00752ee13f7/CitiCodesCount"

                    # Define the headers
                    citicode_count_headers = {
                        "accept": "application/json",
                        "Authorization": f"Bearer {token}",  # Add your token here
                        "Content-Type": "application/json"
                    }

                    # Define the JSON data
                    citicode_count_data = {
                        "isinBlobFileId": "",
                        "citicodeBlobFileId": "",
                        "feedConfigType": "FundData",
                        "filters": [],
                        "isWholeOfMarket": False,
                        "kustoFeedFilter": kusto_payload
                    }

                    # Make a POST request
                    citicode_count_response = requests.post(citicode_count_url, headers=citicode_count_headers, json=citicode_count_data)
                    citicode_count_response_json = citicode_count_response.json()
                    EFS_unit_count = citicode_count_response_json["payload"]


                # Create a dictionary with the required fields
                data = {
                    'Feed_ID': [response_json['payload']['FeedID']],
                    'Legal_Entity_ID': [response_json['payload']['PermissionContainer-LegalEntityId']],
                    'Invalid_OFST': [response_json['payload']['Fields With Invalid OFID Mapping']],
                    'Skipped_Fields': [response_json['payload']['Fields to be skipped']],
                    'Feed_Error_Message': [response_json['payload']['Feed Error Message']],
                    'Feed_Creation_Passed': [response_json['payload']['Feed Creation passed']],
                    'Permission_Container_ID': [response_json_EFS['payload']['permissionContainerId']],
                    'ISIN_Count_EFS': [EFS_unit_count],
                    'Field_Count_EFS': [selected_fields_count]

                }
                print("Writing to file")
                # Convert the dictionary to a DataFrame
                df_new = pd.DataFrame(data)

                # Replace None values in 'Feed_Error_Message' with 'null'
                df_new['Feed_Error_Message'] = df_new['Feed_Error_Message'].where(
                    df_new['Feed_Error_Message'].notnull(),
                    'null')

                index = df_existing.loc[df_existing['Sub_ID'] == str(Sub_ID)].index[0]
                excel_row = index + 2

                for column in df_new.columns:
                    new_value = df_new.at[0, column]
                    if isinstance(new_value, list):
                        new_value = ', '.join(map(str, new_value)) if new_value else 'null'

                    df_existing.at[index, column] = str(new_value)

                # Update the 'Status' column with the formula
                ISIN_Count_formula = f'=ABS(H{excel_row}-J{excel_row})'
                df_existing.at[index, 'ISIN_Count_Match'] = ISIN_Count_formula
                Field_Count_formula = f'=IF(I{excel_row}=K{excel_row},"YES","NO")'
                df_existing.at[index, 'Field_Count_Match'] = Field_Count_formula
                status_formula = f'=IF(AND(M{excel_row}="YES",N{excel_row}="null",O{excel_row}="null",P{excel_row}="null",Q{excel_row}="True"),"RFR","Failed")'
                df_existing.at[index, 'Status'] = status_formula

            except Exception as e:
                logging.exception("An error occurred: %s", e)
                print(e)
                input("Hit Enter to Exit")
                exit(1)
    # Write the updated DataFrame back to the Excel file
    df_existing.to_excel(EXCEL, index=False)
    print("Write Completed!")


get_user()
Create_Feeds()



