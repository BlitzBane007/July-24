import os
import requests
import json
import pandas as pd
import logging
import datetime

TIMEOUT_DURATION = 30

logging.basicConfig(filename='error.log', level=logging.ERROR, format='%(asctime)s:%(levelname)s:%(message)s')

user_path = r'C:\Users\Aditya.Apte\OneDrive - FE fundinfo\Desktop\Desktop Icons\Aditya Apte\FSQtoEFS'
EXCEL = ''
Sub_ID = 15033


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
    # user_path = input("Please enter the OneDrive Path to 'FSQtoEFS'\nUser Path:")
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



get_user()

url = "https://fsqtoefsmigrationtooleuwliv.azurewebsites.net/FSQMigration?subscriptionId=15033&feedDataTypeToBeOnbaoarded=4&isUploadCustomUnits=false"
headers = {"accept": "*/*"}
print("Posting Request to Server")
print("Waiting for Response")
response = requests.post(url, headers=headers)
# Parse the JSON response
response_json = json.loads(response.text)
print("Response received!")
# Create a dictionary with the required fields
data = {
    'Feed_ID': [response_json['payload']['FeedID']],
    'Legal_Entity_ID': [response_json['payload']['PermissionContainer-LegalEntityId']],
    'Invalid_OFST': [response_json['payload']['Fields With Invalid OFID Mapping']],
    'Skipped_Fields': [response_json['payload']['Fields to be skipped']],
    'Feed_Error_Message': [response_json['payload']['Feed Error Message']],
    'Feed_Creation_Passed': [response_json['payload']['Feed Creation passed']]
}
print("Writing to file")
# Convert the dictionary to a DataFrame
df_new = pd.DataFrame(data)

# Replace None values in 'Feed_Error_Message' with 'null'
df_new['Feed_Error_Message'] = df_new['Feed_Error_Message'].where(df_new['Feed_Error_Message'].notnull(), 'null')

# Read the existing Excel file into a DataFrame
df_existing = pd.read_excel(EXCEL, dtype=str)

index = df_existing.loc[df_existing['Sub_ID'] == str(Sub_ID)].index[0]
excel_row = index + 2

for column in df_new.columns:
    new_value = df_new.at[0, column]
    if isinstance(new_value, list):
        new_value = ', '.join(map(str, new_value)) if new_value else 'null'

    df_existing.at[index, column] = str(new_value)

# Update the 'Status' column with the formula
status_formula = f'=IF(AND(I{excel_row}="YES",J{excel_row}="YES",K{excel_row}="null",L{excel_row}="null",M{excel_row}="null",N{excel_row}="True"),"RFR","Failed")'
df_existing.at[index, 'Status'] = status_formula
# Write the updated DataFrame back to the Excel file
df_existing.to_excel(EXCEL, index=False)
print("Write Completed!")
