import os
import requests
import json
import pandas as pd
import logging
import datetime
import sys
import openpyxl

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
    EXCEL = os.path.join(user_path, 'INDEX.xlsx')
    print(f'Excel path built: {EXCEL}')

def get_EFS_bearer_token():
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


# print("Opening file")
# wb = openpyxl.load_workbook(EXCEL)
# ws = wb['Sheet1']
#
# header1 = "Name"
# header2 = "Product"
# header3 = "Price"
#
# col_idx_name = get_column_index_by_header(header1)
# col_idx_product = get_column_index_by_header(header2)
# col_idx_price = get_column_index_by_header(header3)
#
# # Check if any header was not found
# if col_idx_price is None or col_idx_product is None or col_idx_name is None:
#     print("Could not find one or more headers")
# else:
#     print("Headers Found")
#     # Iterate through each row and check the condition for the 'Name' column values
#     for row in range(2, ws.max_row + 1):
#         cell_price = ws.cell(row=row, column=col_idx_price)
#         cell_product = ws.cell(row=row, column=col_idx_product)
#         print(cell_product.value)
#
#         # Example condition: Check if the value in 'Name' column is 'ConditionValue'
#         if cell_product.value == 'Carrots':
#             print(f"Row {row}: {cell_product.value} found, updating price.")
#             cell_price.value = 'Aditya'  # Update value based on condition
#         else:
#             print(f"Row {row}: {cell_product.value} does not match 'ConditionValue'")
#
#     # Save the workbook
#     wb.save(EXCEL)

def Script_Run():
    run_count = 0
    counter = int(input("Please enter the number of iterations:"))
    if not 1 <= counter <= 1000:
        print("Number must be between 1 and 1000.")
        input("Hit ENTER to Exit!")
        sys.exit(1)
    print("Opening file")
    wb = openpyxl.load_workbook(EXCEL)
    ws = wb['Bridge']

    # Function to get the column index by header name
    def get_column_index_by_header(header):
        for col in ws.iter_cols(1, ws.max_column):
            if col[0].value == header:
                return col[0].column
        return None

    def Search_Feed(input_feed_type, input_searchterm):
        url = f"https://datafeeds.fefundinfo.com/api/v1/Feeds/search"
        headers = {
            'accept': 'application/json',
            'Authorization': f'Bearer {token}',  # Replace YOUR_TOKEN with your actual token
            'Content-Type': 'application/json',
        }
        data = {
            "page": 1,
            "pageSize": 9,
            "orderBy": "feedStatus",
            "orderAscending": False,
            "filter": [
                {"key": "FeedDataType", "value": input_feed_type},
                {"key": "EngineType", "value": "SelfServedFeed"}
            ],
            "searchTerm": input_searchterm
        }

        response = requests.post(url, headers=headers, json=data)

        if response.status_code == 200:
            response_text = json.dumps(response.json(), indent=4)  # Format JSON output
            print(response_text)
        else:
            print(f"Request failed with status code {response.status_code}: {response.text}")

    def Get_Feed_Details(input_feed_id):
        url = f"https://datafeeds.fefundinfo.com/api/v1/Feeds/{input_feed_id}"
        headers = {
            'accept': 'application/json',
            'Authorization': f'Bearer {token}',  # Replace YOUR_TOKEN with your actual token
            'Content-Type': 'application/json',
        }

        response = requests.get(url, headers=headers)

        if response.status_code == 200:
            response_text = json.dumps(response.json(), indent=4)  # Format JSON output
            print(response_text)
        else:
            print(f"Request failed with status code {response.status_code}: {response.text}")

    def Get_Diss_Details(input_feed_id):
        url = f"https://datafeeds.fefundinfo.com/api/v1/Feeds/{input_feed_id}/disseminationFeed"
        headers = {
            'accept': 'application/json',
            'Authorization': f'Bearer {token}',  # Replace YOUR_TOKEN with your actual token
            'Content-Type': 'application/json',
        }

        response = requests.get(url, headers=headers)

        if response.status_code == 200:
            response_text = json.dumps(response.json(), indent=4)  # Format JSON output
            print(response_text)
        else:
            print(f"Request failed with status code {response.status_code}: {response.text}")
    header_sub_id = "Sub_ID"
    header_feed_type = "Feed_Type"
    header_dump_type = "Dump_Type"
    header_perm_cont_id = "Perm_Cont_ID"
    header_feed_id = "Feed_ID"
    header_complexity = "Complexity"
    header_recpt_name = "Recpt_Name"
    header_recpt_creation_failed = "Recpt_Creation_Failed"
    header_recpt_status_code = "Recpt_Status_Code"
    header_diss_creation_passed = "Diss_Creation_Passed"
    header_sub_proj_creation_passed = "Sub_Proj_Creation_Passed"
    header_fsq_filename_syntax = "FSQ_FileName_Syntax"
    header_fsq_filename_substr = "FSQ_FileName_SubStr"
    header_fsq_filename_complex = "FSQ_FileName_Complex"
    header_encoding = "Encoding"
    header_delimiter = "Delimiter"
    header_file_type = "File_Type"
    header_delivery_type = "Delivery_Type"
    header_path = "Path"
    header_host = "Host"
    header_port = "Port"
    header_username = "Username"
    header_password = "Password"
    header_recpt_count = "Recpt_Count"
    header_compressed = "Compressed"
    header_schedules = "Schedules"
    header_time = "Time"
    header_alternative_email = "Alternative_Email"
    header_sender_email = "Sender_Email"
    header_include_attachment = "Include_Attachment"
    header_status = "Status"

    col_idx_sub_id = get_column_index_by_header(header_sub_id)
    col_idx_feed_type = get_column_index_by_header(header_feed_type)
    col_idx_dump_type = get_column_index_by_header(header_dump_type)
    col_idx_perm_cont_id = get_column_index_by_header(header_perm_cont_id)
    col_idx_feed_id = get_column_index_by_header(header_feed_id)
    col_idx_complexity = get_column_index_by_header(header_complexity)
    col_idx_recpt_name = get_column_index_by_header(header_recpt_name)
    col_idx_recpt_creation_failed = get_column_index_by_header(header_recpt_creation_failed)
    col_idx_recpt_status_code = get_column_index_by_header(header_recpt_status_code)
    col_idx_diss_creation_passed = get_column_index_by_header(header_diss_creation_passed)
    col_idx_sub_proj_creation_passed = get_column_index_by_header(header_sub_proj_creation_passed)
    col_idx_fsq_filename_syntax = get_column_index_by_header(header_fsq_filename_syntax)
    col_idx_fsq_filename_substr = get_column_index_by_header(header_fsq_filename_substr)
    col_idx_fsq_filename_complex = get_column_index_by_header(header_fsq_filename_complex)
    col_idx_encoding = get_column_index_by_header(header_encoding)
    col_idx_delimiter = get_column_index_by_header(header_delimiter)
    col_idx_file_type = get_column_index_by_header(header_file_type)
    col_idx_delivery_type = get_column_index_by_header(header_delivery_type)
    col_idx_path = get_column_index_by_header(header_path)
    col_idx_host = get_column_index_by_header(header_host)
    col_idx_port = get_column_index_by_header(header_port)
    col_idx_username = get_column_index_by_header(header_username)
    col_idx_password = get_column_index_by_header(header_password)
    col_idx_recpt_count = get_column_index_by_header(header_recpt_count)
    col_idx_compressed = get_column_index_by_header(header_compressed)
    col_idx_schedules = get_column_index_by_header(header_schedules)
    col_idx_time = get_column_index_by_header(header_time)
    col_idx_alternative_email = get_column_index_by_header(header_alternative_email)
    col_idx_sender_email = get_column_index_by_header(header_sender_email)
    col_idx_include_attachment = get_column_index_by_header(header_include_attachment)
    col_idx_status = get_column_index_by_header(header_status)


    for row in range(2, ws.max_row + 1):
        cell_status = ws.cell(row=row, column=col_idx_status)
        if cell_status.value is None or cell_status.value == '':
            if run_count == counter:
                break
            run_count += 1
            print(f'Run Count= {run_count}')
            cell_sub_id = ws.cell(row=row, column=col_idx_sub_id).value
            cell_feed_type = ws.cell(row=row, column=col_idx_feed_type).value
            cell_dump_type = ws.cell(row=row, column=col_idx_dump_type).value
            cell_perm_cont_id = ws.cell(row=row, column=col_idx_perm_cont_id).value
            cell_feed_id = ws.cell(row=row, column=col_idx_feed_id).value
            cell_complexity = ws.cell(row=row, column=col_idx_complexity).value
            cell_recpt_name = ws.cell(row=row, column=col_idx_recpt_name).value
            cell_recpt_creation_failed = ws.cell(row=row, column=col_idx_recpt_creation_failed).value
            cell_recpt_status_code = ws.cell(row=row, column=col_idx_recpt_status_code).value
            cell_diss_creation_passed = ws.cell(row=row, column=col_idx_diss_creation_passed).value
            cell_sub_proj_creation_passed = ws.cell(row=row, column=col_idx_sub_proj_creation_passed).value
            cell_fsq_filename_syntax = ws.cell(row=row, column=col_idx_fsq_filename_syntax).value
            cell_fsq_filename_substr = ws.cell(row=row, column=col_idx_fsq_filename_substr).value
            cell_fsq_filename_complex = ws.cell(row=row, column=col_idx_fsq_filename_complex).value
            cell_encoding = ws.cell(row=row, column=col_idx_encoding).value
            cell_delimiter = ws.cell(row=row, column=col_idx_delimiter).value
            cell_file_type = ws.cell(row=row, column=col_idx_file_type).value
            cell_delivery_type = ws.cell(row=row, column=col_idx_delivery_type).value
            cell_path = ws.cell(row=row, column=col_idx_path).value
            cell_host = ws.cell(row=row, column=col_idx_host).value
            cell_port = ws.cell(row=row, column=col_idx_port).value
            cell_username = ws.cell(row=row, column=col_idx_username).value
            cell_password = ws.cell(row=row, column=col_idx_password).value
            cell_recpt_count = ws.cell(row=row, column=col_idx_recpt_count).value
            cell_compressed = ws.cell(row=row, column=col_idx_compressed).value
            cell_schedules = ws.cell(row=row, column=col_idx_schedules).value
            cell_time = ws.cell(row=row, column=col_idx_time).value
            cell_alternative_email = ws.cell(row=row, column=col_idx_alternative_email).value
            cell_sender_email = ws.cell(row=row, column=col_idx_sender_email).value
            cell_include_attachment = ws.cell(row=row, column=col_idx_include_attachment).value

            Swag_sub_id = ''
            Swag_feed_type = ''
            Swag_dump_type = ''
            Swag_perm_cont_id = ''
            Swag_feed_id = ''
            Swag_complexity = ''
            Swag_recpt_name = ''
            Swag_recpt_creation_failed = ''
            Swag_recpt_status_code = ''
            Swag_diss_creation_passed = ''
            Swag_sub_proj_creation_passed = ''
            Swag_fsq_filename_syntax = ''
            Swag_fsq_filename_substr = ''
            Swag_fsq_filename_complex = ''
            Swag_encoding = ''
            Swag_delimiter = ''
            Swag_file_type = ''
            Swag_delivery_type = ''
            Swag_path = ''
            Swag_host = ''
            Swag_port = ''
            Swag_username = ''
            Swag_password = ''
            Swag_recpt_count = ''
            Swag_compressed = ''
            Swag_schedules = ''
            Swag_time = ''
            Swag_alternative_email = ''
            Swag_sender_email = ''
            Swag_include_attachment = ''

            token = get_EFS_bearer_token()
            search_term_build = f'FSQ-{cell_sub_id}'
            Search_Feed(cell_feed_type, search_term_build)
            # Get_Feed_Details('3be48bf9-b726-47f1-b116-673e32c736a2')
            # Get_Diss_Details('3be48bf9-b726-47f1-b116-673e32c736a2')


get_user()
Script_Run()






