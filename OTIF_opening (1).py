import pandas as pd
import requests
from datetime import datetime
import numpy as np

def fetch_incident_data(incident_id):
    try:
        url = "http://10.83.32.101/incidents/%s/workflow" % incident_id

        payload = {}
        headers = {
            'Content-Type': 'application/json',
            'X_IMS_CLIENT_ID': 'cs',
            'X_IMS_USERNAME': 'vcosi.tvc3287',
            'X_IMS_TENANT': 'cs'
        }

        res = requests.request("GET", url, headers=headers, data=payload)

        if res.status_code == 200:
            data = res.json()
            if 'statusResponse' in data['workflowEntity']['data']:
                status = data['workflowEntity']['data']['statusResponse'].get('status', 'NA')
            else:
                status = "Open"
            last_solved_by = data['workflowEntity']['data'].get("updatedBy", "")
            last_solved = data['workflowEntity']['data'].get("updatedOn","")
            last_solved_on = datetime.strptime(last_solved, "%Y-%m-%dT%H:%M:%S.%f%z")  # Using datetime.strptime to parse the datetime string

            # Format the datetime object
            last_solved_on = last_solved_on.strftime("%d-%m-%Y %I:%M:%S %p")
            print(f"incident_id:{incident_id},status:{status},last_solved_by:{last_solved_by},last_solved_on:{last_solved_on}")

            return status, last_solved_by, last_solved_on
        else:
            return f"status_code : {res.status_code}", "Incident_Id is invalid", "Incident_Id is invalid"

    except Exception as e:
        return f'status_code : {res.status_code}', str(e), str(e)


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



dashboard_date = '26-May-2024'
print("Reading the asset file.......")
asset_df = pd.read_excel(r"C:\Users\vcosi.tvc3287\Desktop\New_folder\Ek_Floder\Pendency_File_1CR_26th_May_2024_new.xlsx", sheet_name='Overall Raw')
print("Count by Problem Group")
count_df = asset_df.groupby(['Problem Group'])['incident_id'].count().reset_index()
print(count_df)
print("Count by final flag")
count_df = asset_df.groupby(['Final Flag'])['incident_id'].count().reset_index()
print(count_df)

print("Filtering for fintech asset......")
fintech_asset_df = asset_df[asset_df['Problem Group'] == 'Fintech / Payments']
print("Count by Problem Statement")
count_df = fintech_asset_df.groupby(['Problem Statement'])['incident_id'].count().reset_index()
print(count_df)
print("Count by final flag")
count_df = fintech_asset_df.groupby(['Final Flag'])['incident_id'].count().reset_index()
print(count_df)

# Fetching incident data for each incident ID in fintech_asset_df
print("Fetching incident data for each incident ID in fintech_asset_df.....")
for i, incident_id in enumerate(fintech_asset_df['incident_id']):
    status, last_solved_by, last_solved_on = fetch_incident_data(incident_id)
    fintech_asset_df.loc[fintech_asset_df.index[i], 'Current_status'] = status
    fintech_asset_df.loc[fintech_asset_df.index[i], 'Last_Solved_By'] = last_solved_by
    fintech_asset_df.loc[fintech_asset_df.index[i], 'Last_Solved_On'] = last_solved_on

# Apply the conditions to create the new column
print("Appling the conditions to create the Status Group.....")
fintech_asset_df['Status_Group'] = fintech_asset_df['Current_status'].apply(lambda status: 'Solved' if status in ['Solved', 'Solved NRR','Closed'] else ('Open' if status in ['Waiting for Internal Updates', 'Updated', 'Waiting for Customer Updates', 'Unresolved'] else 'Not_Found'))
# Saving the modified DataFrame to a new CSV file
print("Saving the data in file Out_Fintech_Asset_Data.csv")
fintech_asset_df.to_csv(r"C:\Users\vcosi.tvc3287\Desktop\New_folder\Ek_Floder\Out_Fintech_Asset_Data.csv", index=False)
print("Saved.")

fintech_asset_df = pd.read_csv(r"C:\Users\vcosi.tvc3287\Desktop\New_folder\Ek_Floder\Out_Fintech_Asset_Data.csv")

# Apply the conditions using lambda functions to create the 'ExactStatus' column
print("Calculating Exact Status...........")
fintech_asset_df['ExactStatus'] = fintech_asset_df.apply(lambda row: 
    "ReOpenByL2" if (row['Current_status'] in ['Waiting for Internal Updates', 'Updated', 'Waiting for Customer Updates', 'Unresolved']) and ((str(row['Last_Solved_By']).startswith("fapl.")) or (row['Last_Solved_By'] in 
                                                                                                                                                                                                   ['cog.cek31049',
                                                                                                                                                                                                    'hgs.42748',
                                                                                                                                                                                                    'hgs.42750',
                                                                                                                                                                                                    'ieblr.t247790',
                                                                                                                                                                                                    'ieblr.t247862',
                                                                                                                                                                                                    'ieblr.t247863',
                                                                                                                                                                                                    'ieblr.t247725',
                                                                                                                                                                                                    'ieblr.247543',
                                                                                                                                                                                                    'vcosi.tvc3236',
                                                                                                                                                                                                    'vcosi.tvc3242',
                                                                                                                                                                                                    'vcosi.tvc3243',
                                                                                                                                                                                                    'vcosi.tvc3284',
                                                                                                                                                                                                    'vcosi.tvc3253',
                                                                                                                                                                                                    'vcosi.tvc3254',
                                                                                                                                                                                                    'vcosi.tvc3283',
                                                                                                                                                                                                    'vcosi.tvc3246',
                                                                                                                                                                                                    'vcosi.tvc3287',])) 
    else "ReOpen" if (row['Current_status'] in ['Waiting for Internal Updates', 'Updated', 'Waiting for Customer Updates', 'Unresolved']) and pd.notna(row['Last_Solved_By']) 
    else "Open" if (row['Current_status'] in ['Waiting for Internal Updates', 'Updated', 'Waiting for Customer Updates', 'Unresolved']) and pd.isna(row['Last_Solved_By']) 
    else "Solved" if row['Current_status'] in ['Solved', 'Solved NRR', 'Closed']
    else "Not_Found", axis=1)

# Convert 'Last_Solved_On' and 'incident_creation_time' columns to datetime format
print("Converting 'Last_Solved_On' and 'incident_creation_time' columns to datetime format.......")
fintech_asset_df['Last_Solved_On'] = pd.to_datetime(fintech_asset_df['Last_Solved_On'], format="%d-%m-%Y %I:%M:%S %p")
fintech_asset_df['incident_creation_time'] = pd.to_datetime(fintech_asset_df['incident_creation_time'], format="%Y-%m-%d %H:%M:%S")
fintech_asset_df['assigned_to_queue_at'] = pd.to_datetime(fintech_asset_df['assigned_to_queue_at'], format="%Y-%m-%d %H:%M:%S")
fintech_asset_df['incident_first_solved_time'] = pd.to_datetime(fintech_asset_df['incident_first_solved_time'], format="%Y-%m-%d %H:%M:%S")

# Apply the conditions to calculate 'Ageing Or Q2R' based on created and assigned to queue date
print("Apply the conditions to calculate 'Ageing Or Q2R' based on created and assigned to queue date......")


fintech_asset_df['Ageing_OR_Q2R'] = fintech_asset_df.apply(lambda row: 
    ((row['Last_Solved_On'] - row['assigned_to_queue_at']).total_seconds() / 3600) if (row['Status_Group'] == 'Solved' and row['incident_first_solved_time'] < row['assigned_to_queue_at'])
    else (((row['Last_Solved_On'] - row['incident_creation_time']).total_seconds() / 3600) if (row['Status_Group'] == 'Solved' and (row['incident_first_solved_time'] >= row['assigned_to_queue_at'] or pd.isnull(row['incident_first_solved_time'])))
    else (((pd.Timestamp.now() - row['assigned_to_queue_at']).total_seconds() / 3600) if (row['Status_Group'] == 'Open' and row['incident_first_solved_time'] < row['assigned_to_queue_at'])
    else ((pd.Timestamp.now() - row['incident_creation_time']).total_seconds() / 3600))), axis=1)



# Saving the modified DataFrame to a CSV file
print("Saving the data to Out_Q2R_Asset_Fintech.csv.........")
fintech_asset_df.to_csv(r"C:\Users\vcosi.tvc3287\Desktop\New_folder\Ek_Floder\Out_Q2R_Ageing_Asset_Fintech.csv", index=False)
print("Saved.")

print("Reading file Out_Q2R_Asset_Fintech.csv")
fintech_Q2R_Ageing_Asset_df = pd.read_csv(r"C:\Users\vcosi.tvc3287\Desktop\New_folder\Ek_Floder\Out_Q2R_Ageing_Asset_Fintech.csv")
# Calculate Ageing, CTQ and ATQ buckets
print("Calculating Ageing, CTQ and ATQ bucket.......")
fintech_Q2R_Ageing_Asset_df['Ageing_OR_Q2R_Bucket'] = fintech_Q2R_Ageing_Asset_df['Ageing_OR_Q2R'].apply(calculate_bucket)
print("Saving Above data in file Out_Bucket_Q2R_Ageing_Asset_Fintech.csv")
fintech_Q2R_Ageing_Asset_df.to_csv(r"C:\Users\vcosi.tvc3287\Desktop\New_folder\Ek_Floder\Out_Bucket_Q2R_Ageing_Asset_Fintech.csv", index=False)
print("Saved.")

print("Reading the solved incident file.......")
solved_incident_df = pd.read_csv(r"C:\Users\vcosi.tvc3287\Desktop\New_folder\Ek_Floder\CT_Solved_Incidents.csv")
print("Sorting as per the solved date")
solved_incident_df.sort_values(by='Date', ascending=False, inplace=True)
print("Deleting the dulpicate incidents..........")
solved_incident_df.drop_duplicates(subset='IncidentID', inplace=True)
solved_incident_df = solved_incident_df[['IncidentID','Exact Sub Issue']]

print("Reading the Out_Bucket_Q2R_Ageing_Asset_Fintech.csv file......")
fintech_Q2R_Ageing_Asset_data_df = pd.read_csv(r"C:\Users\vcosi.tvc3287\Desktop\New_folder\Ek_Floder\Out_Bucket_Q2R_Ageing_Asset_Fintech.csv")

print("Merging the Exact Issue Type........")
exact_ssi_merge_df = pd.merge(fintech_Q2R_Ageing_Asset_data_df, solved_incident_df, left_on='incident_id', right_on='IncidentID', how='left')

print("Saving the merge data in File Out_SSI_merged.csv")
exact_ssi_merge_df.to_csv(r"C:\Users\vcosi.tvc3287\Desktop\New_folder\Ek_Floder\Out_SSI_merged.csv", index=False)
print("Saved.")

print("Reading the file Out_SSI_merged.csv.........")
exact_ssi_df = pd.read_csv(r"C:\Users\vcosi.tvc3287\Desktop\New_folder\Ek_Floder\Out_SSI_merged.csv")

print("Group SSI file........")
group_ssi_df = pd.read_csv(r"C:\Users\vcosi.tvc3287\Desktop\New_folder\Ek_Floder\GroupSSI.csv")

print("Merging the Group SSI and fav/unfav.........")
group_ssi_merge_df = pd.merge(exact_ssi_df, group_ssi_df, left_on='Exact Sub Issue', right_on='Exact_Issue_Type', how='left')
print("Saving the mergre data in file Out_fav_unfav.csv.....")
filename = f"C:\\Users\\vcosi.tvc3287\\Desktop\\New_folder\\Ek_Floder\\Out_fav_unfav_{dashboard_date}.csv"
group_ssi_merge_df.to_csv(filename, index=False)
print("Saved.")

print("Grouping for the report perpose.........")
report_df = group_ssi_merge_df.groupby(['Exact_Issue_Type','Group_SSI','Status_Group','ExactStatus','Ageing_OR_Q2R_Bucket','is_Fav'])['incident_id'].count().reset_index()
print("Saving this report in file called otif_report.csv")
report_df['dashboard_date'] = dashboard_date
report_df.to_csv(r"C:\Users\vcosi.tvc3287\Desktop\New_folder\Ek_Floder\Out_otif_report.csv", index=False)
print("Saved.")




