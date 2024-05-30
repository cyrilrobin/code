"""
Author : Kumar Aditya Kaushal
Edited by : Cyril Robin
Last Updated on :24May2024
"""

import pandas as pd
from datetime import datetime
import numpy as np
import requests
import json

def check_verification_status(acc_id):
    url = "http://10.83.37.91/loginservice/user/v2/%s/identifiers?phoneNumberFormat=E164" % acc_id

    payload = {}
    headers ={
        'Authorization': 'Bearer eyJhbGciOiJSUzI1NiJ9.eyJhdWQiOlsibG9naW4iLCJ1c2VyIl0sInNjb3BlIjpbInVzZXIuYWNjLnJlYWQiLCJ1c2VyLmNudC5yZWFkIiwidXNlci5hY2MuY29uZmlkZW50aWFsLnJlYWQiLCJsb2dpbi5pZGVudGlmaWVycy5yZWFkIiwidXNlci5hY2MuY29uZmlkZW50aWFsLndyaXRlIl0sImV4cCI6MTcxNzA5NjgzMiwiYXV0aG9yaXRpZXMiOlsiUk9MRV9DTElFTlQiXSwianRpIjoiY2U4ZTI0OGYtNGE1Ni00MjY5LWIwZTItMjlhMjJiNjQwMDI5IiwiY2xpZW50X2lkIjoiNWRmNzAwZjY0MWFhNDRiYmIyNDg5MTUzMWUxNzQwMzIifQ.MK90K7gDnhavHPBSQu8KIOnkhe84gw6AYoP6_fs9f5Nt226c1_1AmlAVi_A2_Y6H5xiqGlZEwLPMj_8QYIRaoKQ-r3m_WJtDyGq69hl82RKSUgy2BPRnSXgDD9Fci8b0jHLYolYbZBbWDzsoFbnvrDK-eA1HFaUTOmNAGEvBgzjukj9jy_jQdurzIzbTFEeu2fsMse4hag8Y7M8CJZbqTgrY7S9tXb57CAO1U9kQ7NfotLaPma3UGzcHuTFws1mxeO1osoT4VgtPeAsA4l_IMbHntivjGcXiKIlf-YJ-4EjsqR64qHA3YRopKrkf2jKQazEx7b36SMkmBfAXylUU2g'
    }
    response = requests.request("GET", url, headers=headers, data=payload)

    if response.text:
        response1 = json.loads(response.text.encode('utf8'))
        if response1 and "PHONE" in response1:
            phone_number_status = response1["PHONE"][0]["state"]
            print(acc_id, phone_number_status)
            return phone_number_status
        else:
            phone_number_status = "NA"
            return phone_number_status




# Read the Excel binary workbook
print("Reading the ct asset file.......")
ct_asset_file_path = r'FintechAssetMapping\Pendency_File_1CR_30_May_2024_new.xlsx'  #change the name of the ct file
sheet = 'Overall Raw'
ct_asset_file = pd.read_excel(ct_asset_file_path, sheet_name=sheet)
 
# Apply the filter to get the fintech incidents
print("Reading the fintech incident.......")
filter_ct_asset_df = ct_asset_file[(ct_asset_file['Problem Group'] == 'Fintech / Payments')].copy()

selected_columns = ['incident_id', 'customer_id', 'ssi', 'status_name', 'order_external_id', 'current_queue_name','Problem Statement','Final Flag']
filter_ct_asset_df = filter_ct_asset_df[selected_columns]

print("Saving the fintech incident in a file fintech_ct_asset_{today_date} .......")
# Save the filtered data to the filename finetech_filename
today_date = datetime.today().strftime('%d_%m_%y')
fintech_filename = fr"FintechAssetMapping/fintech_ct_asset_{today_date}.xlsx"
filter_ct_asset_df.to_excel(fintech_filename, index=False)
print(f"Saved the fintech incident in a file {fintech_filename} .......")



# Operating with Response-BNPL_Tech_Escalation_Form.xlsx -- https://docs.google.com/spreadsheets/d/1mFDajpLngcFmgRS1oa-eYE3HVL0UC47KIkllTL0XPtM/edit#gid=0
print("Reading the Response-BNPL_Tech_Escalation_Form.xlsx.......")
response_excel_path = r"FintechAssetMapping\Response-BNPL_Tech_Escalation_Form.xlsx"
tech_response_df = pd.read_excel(response_excel_path, sheet_name="Response")

# Sorting the Response-BNPL_Tech_Escalation_Form.xlsx on Esc Date (Z-A)
print(f"Sorting the {response_excel_path} on Esc Date (Z-A).......")
tech_response_df = tech_response_df.sort_values(by='Esc Date', ascending=False)

# Remove duplicate rows based on 'Incident ID' and keep the first occurrence
print("Deleting Duplicates........")
tech_response_df = tech_response_df.drop_duplicates(subset='Incident Id', keep='first')

filter_ct_asset_df = filter_ct_asset_df.reset_index(drop=True)
tech_response_df = tech_response_df.reset_index(drop=True)

print(f"Performing VLOOKUP for Exact Issue Type and Status with {response_excel_path}.......")
# Perform VLOOKUP for "Exact_Issue_Type"
merged_df = pd.merge(filter_ct_asset_df, tech_response_df[['Incident Id', 'Issue Type', 'STATUS']], left_on='incident_id', right_on='Incident Id', how='left')

# Deleting Status and Exact issue type if status is not Open and marking Open as Open at L4
print("Deleting Status and Exact issue type if status is not Open and marking Open as Open at L4......")
filter_ct_asset_df['Tech_Exact_Issue_Type'] = np.where(merged_df['STATUS'] == 'Open', merged_df['Issue Type'], '')
filter_ct_asset_df['Tech_Status'] = np.where(merged_df['STATUS'] == 'Open', 'Open at L4', '')

print(f"Saving the above data in {fintech_filename}.........")
filter_ct_asset_df.to_excel(fintech_filename, index=False)



# Operation with Fintech - L3 Form (Responses) ---  https://docs.google.com/spreadsheets/d/1t5UQ4O8mHH61xRAFeY5jtIHjrSJlXClatcoG8tlX_OQ/edit#gid=0
print("Reading file Fintech - L3 Form (Responses).......")
fintech_L3_scr_df_path = r'FintechAssetMapping/Fintech_L3_Form_Responses.xlsx'
fintech_L3_scr_df = pd.read_excel(fintech_L3_scr_df_path, sheet_name='Response')

# Sorting the fintech_L3_scr_df on Escalated Date (Z-A)
print(f"Sorting the {fintech_L3_scr_df_path} on Escalated Date (Z-A).......")
fintech_L3_scr_df = fintech_L3_scr_df.sort_values(by='Escalated Date', ascending=False)

# Remove duplicate rows based on 'Incident ID' and keep the first occurrence
print("Deleting Duplicates........")
fintech_L3_scr_df = fintech_L3_scr_df.drop_duplicates(subset='Incident ID', keep='first')

# Again Reading finetech_filename
print(f"Reading updated {fintech_filename} for vlookup.....")
updated_fintech_filename_post_Tech = pd.read_excel(fintech_filename)
updated_fintech_filename_post_Tech = updated_fintech_filename_post_Tech.reset_index(drop=True)
fintech_L3_scr_df = fintech_L3_scr_df.reset_index(drop=True)

# Performing VLOOKUP between filter_ct_asset_df and fintech_L3_scr_df to get Exact_Issue_Type and Final_Status
print("Performing VLOOKUP between filter_ct_asset_df and fintech_L3_scr_df to get Exact_Issue_Type and Final_Status")
scrub_merged_df = pd.merge(updated_fintech_filename_post_Tech,
                            fintech_L3_scr_df[['Incident ID', 'Exact Issue Type', 'Status']],
                            left_on='incident_id', right_on='Incident ID', how='left')

updated_fintech_filename_post_Tech['Scrub_Exact_Issue_Type'] = scrub_merged_df['Exact Issue Type']
updated_fintech_filename_post_Tech['Scrub_Final_Status'] = scrub_merged_df['Status']
updated_fintech_filename_post_Tech['Scrub_Inc'] = scrub_merged_df['incident_id']

print(f"Saving the above data in {fintech_filename}")

# Save the updated DataFrame to the same Excel file
updated_fintech_filename_post_Tech.to_excel(fintech_filename, index=False)


#Againg reading the fintech_filename
print(f"Reading updated {fintech_filename}......")
updated_fintech_filename_post_IDFC_Esc = pd.read_excel(fintech_filename)



# Operating with Response_IDFC_Esclation_Form.xlsx.xlsx ---- https://docs.google.com/spreadsheets/d/1n-r9b0z_JXDmsyeazwggu4R9JLq9wTk_Z6Fvr0DfZ6I/edit#gid=0
fintech_IDFC_Esc_df_path = r'FintechAssetMapping/Response_IDFC_Esclation_Form.xlsx'
print(f"Reading updated {fintech_IDFC_Esc_df_path}......")
fintech_IDFC_Esc_df = pd.read_excel(fintech_IDFC_Esc_df_path, sheet_name='IDFCEscalations')

# Sorting the Response_Fintech_L2_Handle_Form.xlsx on Time (Z-A)
print(f"Sorting the {fintech_IDFC_Esc_df_path} on Esc Date (Z-A).......")
fintech_IDFC_Esc_df = fintech_IDFC_Esc_df.sort_values(by='Timestamp', ascending=False)

# Remove duplicate rows based on 'Incident ID' and keep the first occurrence
print("Deleting Duplicates........")
fintech_IDFC_Esc_df = fintech_IDFC_Esc_df.drop_duplicates(subset='Incident Id', keep='first')

# Resetting the index of the DataFrame
fintech_IDFC_Esc_df = fintech_IDFC_Esc_df.reset_index(drop=True)

# Performing VLOOKUP between filter_ct_asset_df and fintech_L2_handle_df to get Exact_Issue_Type and Final_Status
print(f"Performing VLOOKUP between {fintech_filename} and Response_Fintech_L2_Handle_Form.xlsx to get Exact_Issue_Type")
idf_Esc_handle_merged_df = pd.merge(updated_fintech_filename_post_IDFC_Esc,
                               fintech_IDFC_Esc_df[['Incident Id', 'Issue Type','Status']],
                               left_on='incident_id', right_on='Incident Id', how='left')
updated_fintech_filename_post_IDFC_Esc['IDFC_Esc_Issue_Type'] = idf_Esc_handle_merged_df['Issue Type']
updated_fintech_filename_post_IDFC_Esc['IDFC_Esc_Status'] = idf_Esc_handle_merged_df['Status']

# Save the updated DataFrame to the same Excel file using the 'openpyxl' engine
updated_fintech_filename_post_IDFC_Esc.to_excel(fintech_filename, index=False, engine='openpyxl')


#Againg reading the fintech_filename
print(f"Reading updated {fintech_filename}......")
updated_fintech_filename_post_scrub = pd.read_excel(fintech_filename)


# Operating with Response_Fintech_L2_Handle_Form.xlsx ----- https://docs.google.com/spreadsheets/d/1WlcWr0_5nJPbYLGoXIcBwGAHtTTTrO1fpROUs0wF4so/edit?usp=sharing
fintech_L2_handle_df_path = r'FintechAssetMapping/Response_Fintech_L2_Handle_Form.xlsx'
print(f"Reading updated {fintech_L2_handle_df_path}......")
fintech_L2_handle_df = pd.read_excel(fintech_L2_handle_df_path, sheet_name='response')

# Sorting the Response_Fintech_L2_Handle_Form.xlsx on Time (Z-A)
print(f"Sorting the {fintech_L2_handle_df_path} on Esc Date (Z-A).......")
fintech_L2_handle_df = fintech_L2_handle_df.sort_values(by='Timestamp', ascending=False)

# Remove duplicate rows based on 'Incident ID' and keep the first occurrence
print("Deleting Duplicates........")
fintech_L2_handle_df = fintech_L2_handle_df.drop_duplicates(subset='Incident ID', keep='first')

# Resetting the index of the DataFrame
fintech_L2_handle_df = fintech_L2_handle_df.reset_index(drop=True)

# Performing VLOOKUP between filter_ct_asset_df and fintech_L2_handle_df to get Exact_Issue_Type and Final_Status
print(f"Performing VLOOKUP between {fintech_filename} and Response_Fintech_L2_Handle_Form.xlsx to get Exact_Issue_Type")
l2_handle_merged_df = pd.merge(updated_fintech_filename_post_scrub,
                               fintech_L2_handle_df[['Incident ID', 'Issue_Type']],
                               left_on='incident_id', right_on='Incident ID', how='left')
updated_fintech_filename_post_scrub['L2_Issue_Type'] = l2_handle_merged_df['Issue_Type']

# Save the updated DataFrame to the same Excel file using the 'openpyxl' engine
updated_fintech_filename_post_scrub.to_excel(fintech_filename, index=False, engine='openpyxl')



#Againg reading the fintech_filename
print(f"Reading updated {fintech_filename}......")
updated_fintech_filename_post_L2_handle = pd.read_excel(fintech_filename)

# Operating with Resposne_Fintech_L3_Handle_Form.xlsx ---- https://docs.google.com/spreadsheets/d/1PUg0iQ0E6MCD8ukrgUiRDyP7Zq-Zz03PgMev5YdovQM/edit#gid=0
fintech_L3_handle_df_path = r'FintechAssetMapping/Resposne_Fintech_L3_Handle_Form.xlsx'
print(f"Reading updated {fintech_L3_handle_df_path}......")
fintech_L3_handle_df = pd.read_excel(fintech_L3_handle_df_path, sheet_name='response')

# Sorting the Response_Fintech_L2_Handle_Form.xlsx on Time (Z-A)
print(f"Sorting the {fintech_L3_handle_df_path} on Esc Date (Z-A).......")
#fintech_L3_handle_df = fintech_L3_handle_df.sort_values(by='Time', ascending=False)

# Remove duplicate rows based on 'Incident ID' and keep the first occurrence
print("Deleting Duplicates........")
fintech_L3_handle_df = fintech_L3_handle_df.drop_duplicates(subset='incidentId', keep='first')

# Resetting the index of the DataFrame
fintech_L3_handle_df = fintech_L3_handle_df.reset_index(drop=True)

# Performing VLOOKUP between filter_ct_asset_df and fintech_L2_handle_df to get Exact_Issue_Type and Final_Status
print(f"Performing VLOOKUP between {fintech_filename} and Response_Fintech_L2_Handle_Form.xlsx to get Exact_Issue_Type")
l2_handle_merged_df = pd.merge(updated_fintech_filename_post_L2_handle,
                               fintech_L3_handle_df[['incidentId', 'issueType']],
                               left_on='incident_id', right_on='incidentId', how='left')
updated_fintech_filename_post_L2_handle['L3_Issue_Type'] = l2_handle_merged_df['issueType']

# Save the updated DataFrame to the same Excel file using the 'openpyxl' engine
updated_fintech_filename_post_L2_handle.to_excel(fintech_filename, index=False, engine='openpyxl')


print(f"Reading {fintech_filename} for deciding the dependency")
updated_fintech_filename_final = pd.read_excel(fintech_filename)
updated_fintech_filename_final['Number_Status'] = updated_fintech_filename_final['customer_id'].apply(check_verification_status)


#Deciding the Final issue Type
print("Deciding the Final issue Type......")
updated_fintech_filename_final.loc[
    (updated_fintech_filename_final['Tech_Exact_Issue_Type'].notnull()), 'Final_Issue_Type'] = updated_fintech_filename_final['Tech_Exact_Issue_Type']

updated_fintech_filename_final.loc[
    (updated_fintech_filename_final['Tech_Exact_Issue_Type'].isnull()) &
    (updated_fintech_filename_final['IDFC_Esc_Issue_Type'].notnull()) &
    (updated_fintech_filename_final['IDFC_Esc_Issue_Type'] == "Open"), 'Final_Issue_Type'] = updated_fintech_filename_final['IDFC_Esc_Issue_Type']

updated_fintech_filename_final.loc[
    (updated_fintech_filename_final['Tech_Exact_Issue_Type'].isnull()) &
    (updated_fintech_filename_final['IDFC_Esc_Issue_Type'] != "Open") &
    (updated_fintech_filename_final['Scrub_Exact_Issue_Type'].notnull()) &
    (updated_fintech_filename_final['Scrub_Final_Status'] == 'Open'), 'Final_Issue_Type'] = updated_fintech_filename_final['Scrub_Exact_Issue_Type']

updated_fintech_filename_final.loc[
    (updated_fintech_filename_final['Tech_Exact_Issue_Type'].isnull()) &
    (updated_fintech_filename_final['IDFC_Esc_Issue_Type'] != "Open") &
    (updated_fintech_filename_final['Scrub_Exact_Issue_Type'].notnull()) &
    (updated_fintech_filename_final['Scrub_Final_Status'] == 'Solved') &
    (updated_fintech_filename_final['L2_Issue_Type'].notnull()), 'Final_Issue_Type'] = updated_fintech_filename_final['L2_Issue_Type']

updated_fintech_filename_final.loc[
    (updated_fintech_filename_final['Tech_Exact_Issue_Type'].isnull()) &
    (updated_fintech_filename_final['IDFC_Esc_Issue_Type'] != "Open") &
    (updated_fintech_filename_final['Scrub_Exact_Issue_Type'].notnull()) &
    (updated_fintech_filename_final['Scrub_Final_Status'] == 'Solved') &
    (updated_fintech_filename_final['L2_Issue_Type'].isnull()) &
    (updated_fintech_filename_final['L3_Issue_Type'].notnull()), 'Final_Issue_Type'] = updated_fintech_filename_final['L3_Issue_Type']

updated_fintech_filename_final.loc[
    (updated_fintech_filename_final['Tech_Exact_Issue_Type'].isnull()) &
    (updated_fintech_filename_final['IDFC_Esc_Issue_Type'] != "Open") &
    (updated_fintech_filename_final['Scrub_Exact_Issue_Type'].isnull()) &
    (updated_fintech_filename_final['L2_Issue_Type'].notnull()), 'Final_Issue_Type'] = updated_fintech_filename_final['L2_Issue_Type']

updated_fintech_filename_final.loc[
    (updated_fintech_filename_final['Tech_Exact_Issue_Type'].isnull()) &
    (updated_fintech_filename_final['IDFC_Esc_Issue_Type'] != "Open") &
    (updated_fintech_filename_final['Scrub_Exact_Issue_Type'].isnull()) &
    (updated_fintech_filename_final['L2_Issue_Type'].isnull()) &
    (updated_fintech_filename_final['L3_Issue_Type'].notnull()),'Final_Issue_Type'] = updated_fintech_filename_final['L3_Issue_Type']

updated_fintech_filename_final.loc[
    (updated_fintech_filename_final['Tech_Exact_Issue_Type'].isnull()) &
    (updated_fintech_filename_final['IDFC_Esc_Issue_Type'] != "Open") &
    (updated_fintech_filename_final['Scrub_Exact_Issue_Type'].isnull()) &
    (updated_fintech_filename_final['L2_Issue_Type'].isnull()) &
    (updated_fintech_filename_final['L3_Issue_Type'].isnull()),'Final_Issue_Type'] = 'Not_Handled'



# Deciding the dependency
print("Deciding the dependency......")
updated_fintech_filename_final.loc[
    (updated_fintech_filename_final['Tech_Status'] == 'Open at L4'), 'Dependency'] = 'L4_Tech'

updated_fintech_filename_final.loc[
    (updated_fintech_filename_final['Tech_Status'] != 'Open at L4') &
    (updated_fintech_filename_final['IDFC_Esc_Status'] == 'Open'), 'Dependency'] = 'L3_IDFC_Team'

updated_fintech_filename_final.loc[
    (updated_fintech_filename_final['Tech_Status'].isnull()) &
    (updated_fintech_filename_final['IDFC_Esc_Status'] != 'Open') &
    (updated_fintech_filename_final['Scrub_Final_Status'] == 'Open') &
    (updated_fintech_filename_final['Scrub_Exact_Issue_Type'].notnull()), 'Dependency'] = 'L3_Scrub_Team_Scrubed'

updated_fintech_filename_final.loc[
    (updated_fintech_filename_final['Tech_Status'].isnull()) &
    (updated_fintech_filename_final['IDFC_Esc_Status'] != 'Open') &
    (updated_fintech_filename_final['Scrub_Final_Status'] == 'Open') &
    (updated_fintech_filename_final['Scrub_Exact_Issue_Type'].isnull()), 'Dependency'] = 'L3_Scrub_Team_Not_Scrubed'

updated_fintech_filename_final.loc[
    (updated_fintech_filename_final['Tech_Status'].isnull()) &
    (updated_fintech_filename_final['IDFC_Esc_Status'] != 'Open') &
    (updated_fintech_filename_final['Scrub_Final_Status'] == 'Solved'), 'Dependency'] = 'L2_TechM'

updated_fintech_filename_final.loc[
    (updated_fintech_filename_final['Tech_Status'].isnull()) &
    (updated_fintech_filename_final['IDFC_Esc_Status'] != 'Open') &
    (updated_fintech_filename_final['Scrub_Final_Status'].isnull()) &
    (updated_fintech_filename_final['L2_Issue_Type'].notnull()), 'Dependency'] = 'L2_TechM'

updated_fintech_filename_final.loc[
    (updated_fintech_filename_final['Tech_Status'].isnull()) &
    (updated_fintech_filename_final['IDFC_Esc_Status'] != 'Open') &
    (updated_fintech_filename_final['Scrub_Final_Status'].isnull()) &
    (updated_fintech_filename_final['L2_Issue_Type'].isnull()) &
    (updated_fintech_filename_final['L3_Issue_Type'].notnull()), 'Dependency'] = 'L3_Calling_Team'

updated_fintech_filename_final.loc[
    (updated_fintech_filename_final['Tech_Status'].isnull()) &
    (updated_fintech_filename_final['IDFC_Esc_Status'] != 'Open') &
    (updated_fintech_filename_final['Scrub_Final_Status'].isnull()) &
    (updated_fintech_filename_final['L2_Issue_Type'].isnull()) &
    (updated_fintech_filename_final['L3_Issue_Type'].isnull()), 'Dependency'] = 'Untouched'

print(f"Changing and saving the data to {fintech_filename}.....")
# Save the updated DataFrame to the same Excel file
updated_fintech_filename_final.to_excel(fintech_filename, index=False)
print(f"Data has been saved to {fintech_filename}.")
