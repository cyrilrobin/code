import csv
import requests
import json
import datetime
import pandas as pd
import os
import numpy as np

def calculate_bucket(age):
    if age <= 24:
        return 'a. 00-24 Hrs'
    elif age <= 48:
        return 'b. 25-48 Hrs'
    elif age <= 72:
        return 'c. 49-72 Hrs'
    elif age <= 120:
        return 'd. 73-120 Hrs'
    else:
        return 'e. 120+ Hrs'
    
def calculate_bucket_inDays(age):
    if age <= 1:
        return '00-01 Days'
    elif age <= 2:
        return '01-02 Days'
    elif age <= 4:
        return '03-04 Days'
    elif age <= 7:
        return '05-07 Days'
    elif age <= 10:
        return '08-10 Days'
    elif age <= 15:
        return '11-15 Days'
    elif age <= 20:
        return '16-20 Days'
    elif age <= 30:
        return '21-30 Days'
    else:
        return '30+ Days'

if __name__ == '__main__':
    try:
        incident_raw_df = pd.read_csv("OpenIncidentDashboard/IncidentDownload.csv", parse_dates=['allocation.addedToQueueAt', 'createdOn'], date_format='%b %d, %Y %H:%M:%S')

        print("Sorting on allocation.addedToQueueAt ...........")
        incident_raw_df.sort_values(by='allocation.addedToQueueAt', inplace=True)
        print("Removing duplicates on id ...........")
        incident_raw_df.drop_duplicates(subset='id', keep='first', inplace=True)
        print("Filtering the columns........")
        filtered_incident_raw_df = incident_raw_df[['allocation.addedToQueueAt','createdOn','customerId','id','issue.name','allocation.queue.name']]
        print("Saving this filtered incident data to file Out_uniqueuIncidentDump.csv.....")
        filtered_incident_raw_df.to_csv("OpenIncidentDashboard/Out_uniqueuIncidentDump.csv", index=False)

        unique_incident_df = pd.read_csv("OpenIncidentDashboard/Out_uniqueuIncidentDump.csv", parse_dates=['allocation.addedToQueueAt', 'createdOn'], date_format='%b %d, %Y %H:%M:%S')

        print("Data types of 'createdOn' and 'allocation.addedToQueueAt' columns:")
        print(unique_incident_df.dtypes[['createdOn', 'allocation.addedToQueueAt']])

        # Calculate the current datetime
        current_datetime = datetime.datetime.now()

        # Calculate the age of the incident in hours
        print("Calculating CTQ and ATQ ageing and buckets.............")
        unique_incident_df['CTQ_Ageing'] = (current_datetime - pd.to_datetime(unique_incident_df['createdOn'])).dt.total_seconds() / 3600
        unique_incident_df['ATQ_Ageing'] = (current_datetime - pd.to_datetime(unique_incident_df['allocation.addedToQueueAt'])).dt.total_seconds() / 3600

        # Calculate CTQ and ATQ buckets
        unique_incident_df['CTQ_Bucket'] = unique_incident_df['CTQ_Ageing'].apply(calculate_bucket)
        unique_incident_df['ATQ_Bucket'] = unique_incident_df['ATQ_Ageing'].apply(calculate_bucket)

        # Calculate CTQ and ATQ buckets
        unique_incident_df['CTQ_Bucket_inDays'] = (unique_incident_df['CTQ_Ageing']/24).apply(calculate_bucket_inDays)
        unique_incident_df['ATQ_Bucket_inDays'] = (unique_incident_df['ATQ_Ageing']/24).apply(calculate_bucket_inDays)

        # Print the DataFrame with age calculations and buckets
        print(unique_incident_df)
        print("Saving the data in Out_open_Dashboard.csv.........")
        unique_incident_df.to_csv("OpenIncidentDashboard/Out_open_Dashboard.csv", index=False)

        open_incident_dump_df = pd.read_csv("OpenIncidentDashboard/Out_open_Dashboard.csv")

        # Operating with Response-BNPL_Tech_Escalation_Form.xlsx -- https://docs.google.com/spreadsheets/d/1mFDajpLngcFmgRS1oa-eYE3HVL0UC47KIkllTL0XPtM/edit#gid=0
        print("Reading the Response-BNPL_Tech_Escalation_Form.xlsx.......")
        response_excel_path = r"OpenIncidentDashboard/Response-BNPL_Tech_Escalation_Form.xlsx"
        tech_response_df = pd.read_excel(response_excel_path, sheet_name="Response")

        # Sorting the Response-BNPL_Tech_Escalation_Form.xlsx on Esc Date (Z-A)
        print(f"Sorting the {response_excel_path} on Esc Date (Z-A).......")
        tech_response_df.sort_values(by='Esc Date', ascending=False, inplace=True)

        # Remove duplicate rows based on 'Incident ID' and keep the first occurrence
        print("Deleting Duplicates........")
        tech_response_df.drop_duplicates(subset='Incident Id', keep='first', inplace=True)
        
        if tech_response_df is not None:
            tech_response_df = tech_response_df[['Incident Id', 'Issue Type', 'STATUS']]

            print(f"Performing VLOOKUP for Exact Issue Type and Status with {response_excel_path}.......")
            # Perform VLOOKUP for "Exact_Issue_Type"
            merged_tech_df = pd.merge(open_incident_dump_df, tech_response_df, left_on='id', right_on='Incident Id', how='left')

            # Deleting Status and Exact issue type if status is not Open and marking Open as Open at L4
            print("Deleting Status and Exact issue type if status is not Open and marking Open as Open at L4......")
            open_incident_dump_df['Tech_Exact_Issue_Type'] = np.where(merged_tech_df['STATUS'] == 'Open', merged_tech_df['Issue Type'], '')
            open_incident_dump_df['Tech_Status'] = np.where(merged_tech_df['STATUS'] == 'Open', 'Open at L4', '')

            print(f"Saving the above data in openidDashboard.csv.........")
            open_incident_dump_df.to_csv("OpenIncidentDashboard/Out_open_Dashboard.csv", index=False)
        else:
            print("Tech response DataFrame is None. Please check if data was loaded properly.")
        
        # Operation with Fintech - L3 Form (Responses) ---  https://docs.google.com/spreadsheets/d/1t5UQ4O8mHH61xRAFeY5jtIHjrSJlXClatcoG8tlX_OQ/edit#gid=0
        print("Reading file Fintech - L3 Form (Responses).......")
        fintech_L3_scr_df_path = r'OpenIncidentDashboard/Fintech_L3_Form_Responses.xlsx'
        fintech_L3_scr_df = pd.read_excel(fintech_L3_scr_df_path, sheet_name='Response')

        # Sorting the fintech_L3_scr_df on Escalated Date (Z-A)
        print(f"Sorting the {fintech_L3_scr_df_path} on Escalated Date (Z-A).......")
        fintech_L3_scr_df = fintech_L3_scr_df.sort_values(by='Escalated Date', ascending=False)

        # Remove duplicate rows based on 'Incident ID' and keep the first occurrence
        print("Deleting Duplicates........")
        fintech_L3_scr_df = fintech_L3_scr_df.drop_duplicates(subset='Incident ID', keep='first')

        # Again Reading finetech_filename
        print(f"Reading updated Out_open_Dashboard.csv for vlookup.....")
        updated_fintech_filename_post_Tech = pd.read_csv("OpenIncidentDashboard/Out_open_Dashboard.csv")
        updated_fintech_filename_post_Tech = updated_fintech_filename_post_Tech.reset_index(drop=True)
        fintech_L3_scr_df = fintech_L3_scr_df.reset_index(drop=True)

        # Performing VLOOKUP between filter_ct_asset_df and fintech_L3_scr_df to get Exact_Issue_Type and Final_Status
        print("Performing VLOOKUP between filter_ct_asset_df and fintech_L3_scr_df to get Exact_Issue_Type and Final_Status")
        scrub_merged_df = pd.merge(updated_fintech_filename_post_Tech,
                                    fintech_L3_scr_df[['Incident ID', 'Exact Issue Type', 'Status']],
                                    left_on='id', right_on='Incident ID', how='left')

        updated_fintech_filename_post_Tech['Scrub_Exact_Issue_Type'] = scrub_merged_df['Exact Issue Type']
        updated_fintech_filename_post_Tech['Scrub_Final_Status'] = scrub_merged_df['Status']
        updated_fintech_filename_post_Tech['Scrub_Inc'] = scrub_merged_df['Incident ID']

        print(f"Saving the above data in Out_open_Dashboard.csv..........")

        # Save the updated DataFrame to the same Excel file
        updated_fintech_filename_post_Tech.to_csv("OpenIncidentDashboard/Out_open_Dashboard.csv", index=False)


        # Operating with Response_IDFC_Esclation_Form.xlsx.xlsx ---- https://docs.google.com/spreadsheets/d/1n-r9b0z_JXDmsyeazwggu4R9JLq9wTk_Z6Fvr0DfZ6I/edit#gid=0
        fintech_IDFC_Esc_df_path = r'OpenIncidentDashboard/Response_IDFC_Esclation_Form.xlsx'
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

        #Againg reading the fintech_filename
        print(f"Reading updated Out_open_Dashboard.csv......")
        updated_fintech_filename_post_IDFC_Esc = pd.read_csv("OpenIncidentDashboard/Out_open_Dashboard.csv")

        # Performing VLOOKUP between filter_ct_asset_df and fintech_L2_handle_df to get Exact_Issue_Type and Final_Status
        print(f"Performing VLOOKUP between Out_open_Dashboard.csv and Response_Fintech_L2_Handle_Form.xlsx to get Exact_Issue_Type")
        idf_Esc_handle_merged_df = pd.merge(updated_fintech_filename_post_IDFC_Esc,
                                    fintech_IDFC_Esc_df[['Incident Id', 'Issue Type','Status']],
                                    left_on='id', right_on='Incident Id', how='left')
        updated_fintech_filename_post_IDFC_Esc['IDFC_Esc_Issue_Type'] = idf_Esc_handle_merged_df['Issue Type']
        updated_fintech_filename_post_IDFC_Esc['IDFC_Esc_Status'] = idf_Esc_handle_merged_df['Status']

        print(f"Saving the above data in Out_open_Dashboard.csv..........")
        updated_fintech_filename_post_IDFC_Esc.to_csv("OpenIncidentDashboard/Out_open_Dashboard.csv", index=False)


        #Againg reading the fintech_filename
        print(f"Reading updated Out_open_Dashboard.csv file......")
        updated_fintech_filename_post_scrub = pd.read_csv("OpenIncidentDashboard/Out_open_Dashboard.csv")

        # Operating with Response_Fintech_L2_Handle_Form.xlsx ----- https://docs.google.com/spreadsheets/d/1WlcWr0_5nJPbYLGoXIcBwGAHtTTTrO1fpROUs0wF4so/edit?resourcekey#gid=248357108
        fintech_L2_handle_df_path = r'OpenIncidentDashboard/Response_Fintech_L2_Handle_Form.csv'
        print(f"Reading updated {fintech_L2_handle_df_path}......")
        fintech_L2_handle_df = pd.read_csv(fintech_L2_handle_df_path)

        # Sorting the Response_Fintech_L2_Handle_Form.xlsx on Time (Z-A)
        print(f"Sorting the {fintech_L2_handle_df_path} on Esc Date (Z-A).......")
        fintech_L2_handle_df = fintech_L2_handle_df.sort_values(by='Timestamp', ascending=False)

        # Remove duplicate rows based on 'Incident ID' and keep the first occurrence
        print("Deleting Duplicates........")
        fintech_L2_handle_df = fintech_L2_handle_df.drop_duplicates(subset='Incident ID', keep='first')

        # Resetting the index of the DataFrame
        fintech_L2_handle_df = fintech_L2_handle_df.reset_index(drop=True)

        # Performing VLOOKUP between filter_ct_asset_df and fintech_L2_handle_df to get Exact_Issue_Type and Final_Status
        print(f"Performing VLOOKUP between Out_open_Dashboard.csv and Response_Fintech_L2_Handle_Form.xlsx to get Exact_Issue_Type")
        l2_handle_merged_df = pd.merge(updated_fintech_filename_post_scrub,
                                    fintech_L2_handle_df[['Incident ID', 'Issue_Type']],
                                    left_on='id', right_on='Incident ID', how='left')
        updated_fintech_filename_post_scrub['L2_Issue_Type'] = l2_handle_merged_df['Issue_Type']

        print(f"Saving the above data in Out_open_Dashboard.csv..........")
        updated_fintech_filename_post_scrub.to_csv("OpenIncidentDashboard/Out_open_Dashboard.csv", index=False)

        #Againg reading the fintech_filename
        print(f"Reading updated Out_open_Dashboard.csv......")
        updated_fintech_filename_post_L2_handle = pd.read_csv("OpenIncidentDashboard/Out_open_Dashboard.csv")

        # Operating with Resposne_Fintech_L3_Handle_Form.xlsx ---- https://docs.google.com/spreadsheets/d/1PUg0iQ0E6MCD8ukrgUiRDyP7Zq-Zz03PgMev5YdovQM/edit#gid=0
        fintech_L3_handle_df_path = r'OpenIncidentDashboard/Resposne_Fintech_L3_Handle_Form.csv'
        print(f"Reading updated {fintech_L3_handle_df_path}......")
        fintech_L3_handle_df = pd.read_csv(fintech_L3_handle_df_path)

        # Sorting the Response_Fintech_L2_Handle_Form.xlsx on Time (Z-A)
        print(f"Sorting the {fintech_L3_handle_df_path} on Esc Date (Z-A).......")
        #fintech_L3_handle_df = fintech_L3_handle_df.sort_values(by='Time', ascending=False)

        # Remove duplicate rows based on 'Incident ID' and keep the first occurrence
        print("Deleting Duplicates........")
        fintech_L3_handle_df = fintech_L3_handle_df.drop_duplicates(subset='incidentId', keep='first')

        # Resetting the index of the DataFrame
        fintech_L3_handle_df = fintech_L3_handle_df.reset_index(drop=True)

        # Performing VLOOKUP between filter_ct_asset_df and fintech_L2_handle_df to get Exact_Issue_Type and Final_Status
        print(f"Performing VLOOKUP between Out_open_Dashboard.csv and Response_Fintech_L2_Handle_Form.xlsx to get Exact_Issue_Type")
        l2_handle_merged_df = pd.merge(updated_fintech_filename_post_L2_handle,
                                    fintech_L3_handle_df[['incidentId', 'issueType']],
                                    left_on='id', right_on='incidentId', how='left')
        updated_fintech_filename_post_L2_handle['L3_Issue_Type'] = l2_handle_merged_df['issueType']

        print(f"Saving the above data in Out_open_Dashboard.csv..........")
        updated_fintech_filename_post_L2_handle.to_csv("OpenIncidentDashboard/Out_open_Dashboard.csv", index=False)

        print(f"Reading Out_open_Dashboard.csv for deciding the dependency")
        updated_fintech_filename_final = pd.read_csv("OpenIncidentDashboard/Out_open_Dashboard.csv")



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

        print(f"Changing and saving the data to Out_open_Dashboard.csv.....")
        # Save the updated DataFrame to the same Excel file
        updated_fintech_filename_final.to_csv("OpenIncidentDashboard/Out_open_Dashboard.csv", index=False)
        print(f"Data has been saved to Out_open_Dashboard.csv.")


        # Read the CSV file
        print("Reading Out_open_Dashboard.csv............")
        final_open_dashboard_df = pd.read_csv("OpenIncidentDashboard/Out_open_Dashboard.csv")

        # Define conditions
        condition_1 = final_open_dashboard_df['issue.name'] == 'Mobile Recharge Escalation Issue'
        condition_2 = final_open_dashboard_df['issue.name'].isin(['Mobile Recharge & Bill Payment Enquiry', 'Mobile Recharge Entrypoint Issue'])

        # Update 'Final_Issue_Type' column based on conditions
        print("Replacing values for Mobile and recharge cases............")
        final_open_dashboard_df.loc[condition_1, 'Final_Issue_Type'] = 'Mobile Recharge Escalation'
        final_open_dashboard_df.loc[condition_1, 'Dependency'] = 'Bill_Desk'
        final_open_dashboard_df.loc[condition_2, 'Final_Issue_Type'] = 'Mobile Recharge & Bill Payment'
        final_open_dashboard_df.loc[condition_2, 'Dependency'] = 'CT_Solvable'

        # Write the updated dataframe back to CSV
        print("Saving the data in Out_open_Dashboard.csv.........")
        final_open_dashboard_df.to_csv("OpenIncidentDashboard/Out_open_Dashboard.csv", index=False)
        print(f"Data has been saved to Out_open_Dashboard.csv.")

        
        # Update 'Priority' column based on conditions
        print("Making priorities............")
        final_open_dashboard_df.loc[(final_open_dashboard_df['Final_Issue_Type'] == 'Mobile Recharge Escalation') & (final_open_dashboard_df['CTQ_Ageing'] >= 0),
                                    'Priority'] = 'P2'
        final_open_dashboard_df.loc[(final_open_dashboard_df['Final_Issue_Type'] == 'Mobile Recharge Escalation') & (final_open_dashboard_df['CTQ_Ageing'] >= 100),
                                    'Priority'] = 'P1'
        final_open_dashboard_df.loc[(final_open_dashboard_df['Final_Issue_Type'] == 'Mobile Recharge Escalation') & (final_open_dashboard_df['CTQ_Ageing'] >= 150),
                                    'Priority'] = 'P0'
        final_open_dashboard_df.loc[(final_open_dashboard_df['Final_Issue_Type'] == 'Mobile Recharge Escalation') & (final_open_dashboard_df['CTQ_Ageing'] >= 190),
                                    'Priority'] = 'P-1'
        final_open_dashboard_df.loc[(final_open_dashboard_df['Final_Issue_Type'] == 'Mobile Recharge Escalation') & (final_open_dashboard_df['CTQ_Ageing'] >= 200),
                                    'Priority'] = 'P-2'
        final_open_dashboard_df.loc[(final_open_dashboard_df['Final_Issue_Type'] != 'Mobile Recharge Escalation') & (final_open_dashboard_df['CTQ_Ageing'] >= 0),
                                    'Priority'] = 'P2'
        final_open_dashboard_df.loc[(final_open_dashboard_df['Final_Issue_Type'] != 'Mobile Recharge Escalation') & (final_open_dashboard_df['CTQ_Ageing'] >= 12),
                                    'Priority'] = 'P1'
        final_open_dashboard_df.loc[(final_open_dashboard_df['Final_Issue_Type'] != 'Mobile Recharge Escalation') & (final_open_dashboard_df['CTQ_Ageing'] >= 24),
                                    'Priority'] = 'P0'
        final_open_dashboard_df.loc[(final_open_dashboard_df['Final_Issue_Type'] != 'Mobile Recharge Escalation') & (final_open_dashboard_df['CTQ_Ageing'] >= 48),
                                    'Priority'] = 'P-1'
        final_open_dashboard_df.loc[(final_open_dashboard_df['Final_Issue_Type'] != 'Mobile Recharge Escalation') & (final_open_dashboard_df['CTQ_Ageing'] >= 72),
                                    'Priority'] = 'P-2'

        print("Saving the data in Out_open_Dashboard.csv.........")
        final_open_dashboard_df.to_csv("OpenIncidentDashboard/Out_open_Dashboard.csv", index=False)
        print(f"Data has been saved to Out_open_Dashboard.csv.")

    except Exception as e:
        print('Something went wrong! :', e)
