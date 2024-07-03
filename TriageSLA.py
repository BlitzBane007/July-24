import pandas as pd
from datetime import datetime, timedelta

def calculate_difference(log_time, create_time):
    fmt = '%d/%m/%Y %H:%M'
    t1 = datetime.strptime(log_time, fmt)
    t2 = datetime.strptime(create_time, fmt)
    diff = t1 - t2
    return f"{int(diff.total_seconds() // 3600):02d}:{int((diff.total_seconds() % 3600) // 60):02d}"

def work_hours_and_weekday(create_time_str):
    dt = datetime.strptime(create_time_str, '%d/%m/%Y %H:%M')
    weekday = dt.weekday()
    within_work_hours = dt.time() >= datetime.strptime("10:00AM", "%I:%M%p").time() and dt.time() <= datetime.strptime("6:00PM", "%I:%M%p").time()
    return within_work_hours and weekday < 5

def str_to_time(time_str, fmt='%d/%m/%Y %H:%M', output_fmt='%I:%M%p'):
    return datetime.strptime(time_str, fmt).strftime(output_fmt)

def get_status(created_within_work_hours, diff_minutes):
    return "Not Breached" if not created_within_work_hours or diff_minutes < 30 else "Breached"

create_df = pd.read_csv('Createtime.csv')
log_df = pd.read_csv('Logtime.csv')

create_df['Edit Date'] = pd.to_datetime(create_df['Edit Date'], format='%d/%m/%Y %H:%M')
log_df['Edit Date'] = pd.to_datetime(log_df['Edit Date'], format='%d/%m/%Y %H:%M')

merged_df = pd.merge(log_df, create_df, on="Case Number", suffixes=('_log', '_create'))

merged_df['Day'] = merged_df['Edit Date_create'].apply(lambda x: x.strftime('%A'))
merged_df['Created Time'] = merged_df['Edit Date_create'].apply(lambda x: x.strftime('%I:%M%p'))
merged_df['Logged Time'] = merged_df['Edit Date_log'].apply(lambda x: x.strftime('%I:%M%p'))
merged_df['Difference'] = merged_df.apply(lambda x: calculate_difference(x['Edit Date_log'].strftime('%d/%m/%Y %H:%M'), x['Edit Date_create'].strftime('%d/%m/%Y %H:%M')), axis=1)
merged_df['Status'] = merged_df.apply(lambda x: get_status(work_hours_and_weekday(x['Edit Date_create'].strftime('%d/%m/%Y %H:%M')), (x['Edit Date_log'] - x['Edit Date_create']).seconds // 60), axis=1)

final_df = merged_df[['Case Number', 'Edited By', 'Day', 'Created Time', 'Logged Time', 'Difference', 'Status']]

final_df.to_excel('final_output.xlsx', index=False)

# Creating the Pivot Table.

# Step 1: Read the final_output.xlsx file into a DataFrame
final_df = pd.read_excel('final_output.xlsx')

# Step 2: Create a pivot table
pivot_table = final_df.pivot_table(index='Edited By', columns='Status', aggfunc='size', fill_value=0)

# Rename columns to match required headers
pivot_table.columns = ['Breached', 'Not Breached']

# Step 3: Calculate 'Grand Total'
pivot_table['Grand Total'] = pivot_table['Breached'] + pivot_table['Not Breached']

# Round off 'SLA %' to 2 decimal places
pivot_table['SLA %'] = ((pivot_table['Not Breached'] / pivot_table['Grand Total']) * 100).round(2)

# Reset index to move 'Edited By' to the headers
pivot_table.reset_index(inplace=True)

# Rename 'Edited By' to 'Analyst Name'
pivot_table.rename(columns={'Edited By': 'Analyst Name'}, inplace=True)

# Convert pivot table to HTML without the index
html_table = pivot_table.to_html(classes='table table-striped', index=False)

# Step 6: Write the HTML table to an HTML file
with open('pivot_table.html', 'w') as f:
    f.write(html_table)
