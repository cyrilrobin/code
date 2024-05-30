import csv
import requests
import json
import datetime
import pandas as pd
from dateutil import parser

def custom_parse_date(date_string):
    if isinstance(date_string, str):
        try:
            return parser.parse(date_string)
        except ValueError:
            return pd.NaT
    else:
        return pd.NaT
    

def last_solved_details(incident_Id):
    try:
        url = f"http://10.83.32.101/incidents/{incident_Id}/workflow"

        payload = {}
        headers = {
            'Content-Type': 'application/json',
            'X_IMS_CLIENT_ID': 'cs',
            'X_IMS_USERNAME': 'swati.gautam',
            'X_IMS_TENANT': 'cs'
        }

        res = requests.request("GET", url, headers=headers, data=payload)

        if res.status_code == 200:
            data = res.json()  # Use res.json() directly
            workflowStatus = data.get('workflowStatus', "")
            if workflowStatus == "COMPLETED":
                current_status = "Solved"
            else:
                current_status = "Open"
            # Check if the necessary fields are present
            lastSolvedBy = data['workflowEntity']['data'].get("lastSolvedBy", "NA")
            lastSolved = data['workflowEntity']['data'].get("lastSolvedOn", "NA")
            # Convert lastSolvedOn if it exists
            if lastSolved != "NA":
                lastSolvedOn = datetime.datetime.strptime(lastSolved, "%Y-%m-%dT%H:%M:%S.%f%z").strftime("%d-%m-%Y %I:%M:%S %p")
            else:
                lastSolvedOn = "NA"

            print(f"Incident ID: {incident_Id}, Last Solved By: {lastSolvedBy}, Last Solved On: {lastSolvedOn}, Current Status: {current_status}")

            return lastSolvedBy, lastSolvedOn, current_status

        else:
            print(f"Unable to fetch data for incident ID {incident_Id}. Status code: {res.status_code}")
            return 'NA', 'NA', 'NA'  # Return default values

    except Exception as e:
        print(f"Unable to fetch data from incident ID {incident_Id}: {e}")
        return 'NA', 'NA', 'NA'  # Return default values



def get_lob(lastSolvedBy):
    if lastSolvedBy in ['cog.cek31049', 'hgs.42748', 'hgs.42750', 'HGS.42807', 'HGS.42810', 'HGS.42811',
                        'ieblr.247543', 'ieblr.t232397', 'ieblr.t247725', 'ieblr.t247790', 'ieblr.t247862',
                        'ieblr.t247863', 'ieblr.T248367', 'madhusudhan.m', 'vcosi.tvc3337', 'kumar.kaushal',
                        'vcosi.tvc3236', 'vcosi.tvc3242', 'vcosi.tvc3243', 'vcosi.tvc3246', 'vcosi.tvc3253',
                        'vcosi.tvc3254', 'vcosi.tvc3283', 'nadeem.ahamed']:
        return 'L3'
    elif lastSolvedBy in ['vcosi.tvc3287','vcosi.tvc3284']:
        return 'CT'
    elif lastSolvedBy.startswith('fapl.'):
        return 'L2'
    elif lastSolvedBy.startswith('rcbp-'):
        return 'rcbp'
    else:
        return 'other'


if __name__ == '__main__':
    try:
        print("Reading solved incidents file closing dashboard file ............")
        closing_dashboard_df = pd.read_csv("SLAndOTIF_OpeningDashboard/Closing_Dashboard_11March241.csv", parse_dates=['Created_At', 'Added2Queue'], date_format='%y/%m/%d %H:%M:%S')
        print("Reading group SSI file......")
        group_ssi_df = pd.read_csv("SLAndOTIF_OpeningDashboard/GroupSSI.csv")

        print("Mapping GSSI .............")
        gssi_merge_df = pd.merge(closing_dashboard_df, group_ssi_df, left_on='Final_Issue_Type', right_on='Exact_Issue_Type', how='left')
        print("Saving the mapped data in Out_Closing_Dashboard_GSSI.csv.......")
        gssi_merge_df.to_csv("SLAndOTIF_OpeningDashboard/Out_Closing_Dashboard_GSSI.csv", index=False)

        input_file_path = 'SLAndOTIF_OpeningDashboard/Out_Closing_Dashboard_GSSI.csv'
        output_file_path = 'SLAndOTIF_OpeningDashboard/Out_Closing_Dashboard_solvedBy.csv'

        print("Fetching solved details............")
        # Open the input CSV file for reading
        with open(input_file_path, mode='r') as infile:
            reader = csv.DictReader(infile)
            fieldnames = reader.fieldnames + ['lastSolvedBy', 'lastSolvedOn', 'current_status']

            # Open the output CSV file for writing
            with open(output_file_path, mode='w', newline='') as outfile:
                writer = csv.DictWriter(outfile, fieldnames=fieldnames)
                writer.writeheader()

                for row in reader:
                    incident_Id = row['incidentId']
                    lastSolvedBy, lastSolvedOn, current_status = last_solved_details(incident_Id)

                    # Update the row with new data
                    row['lastSolvedBy'] = lastSolvedBy
                    row['lastSolvedOn'] = lastSolvedOn
                    row['current_status'] = current_status

                    # Write the updated row to the output CSV file
                    writer.writerow(row)
        print("Solved By data saved on Out_Closing_Dashboard_solvedBy.csv.")

        input_Fintech_solved_details_data_path = 'SLAndOTIF_OpeningDashboard/Out_Closing_Dashboard_solvedBy.csv'
        output_Fintech_solved_details_data_path = 'SLAndOTIF_OpeningDashboard/Out_Closing_Dashboard_LOB.csv'

        print("Fetching LOB details............")
        # Open the input CSV file for reading
        with open(input_Fintech_solved_details_data_path, mode='r') as infile:
            reader = csv.DictReader(infile)
            fieldnames = reader.fieldnames + ['LOB']

            # Open the output CSV file for writing
            with open(output_Fintech_solved_details_data_path, mode='w', newline='') as outfile:
                writer = csv.DictWriter(outfile, fieldnames=fieldnames)
                writer.writeheader()

                for row in reader:
                    lastSolvedBy = row['lastSolvedBy']
                    lob = get_lob(lastSolvedBy)

                    # Update the row with the new LOB value
                    row['LOB'] = lob

                    # Write the updated row to the output CSV file
                    print(row)
                    writer.writerow(row)
        print("LOB data saved in Out_Closing_Dashboard_LOB.csv.")

        print("Reading Out_Closing_Dashboard_LOB.csv .........")
        final_closing_dashboard_df = pd.read_csv('SLAndOTIF_OpeningDashboard/Out_Closing_Dashboard_LOB.csv')
        print("Reading finetch mapping file .........")
        problem_statement_df = pd.read_csv("SLAndOTIF_OpeningDashboard/FintechMapping.csv")
        print("Merging the problem stament as per  the SSI......")
        mergre_prb_statment_df = pd.merge(final_closing_dashboard_df,problem_statement_df, left_on='subSubIssue', right_on='SSI', how='left')
        print("Saving merge data in Out_final_closing_dashboard.csv ......")
        mergre_prb_statment_df.to_csv("SLAndOTIF_OpeningDashboard/Out_final_closing_dashboard.csv", index=False)
        print("Saved to Out_final_closing_dashboard.csv.")

        print("Reading Out_final_closing_dashboard.csv .........")
        report_closing_dashboard_df = pd.read_csv("SLAndOTIF_OpeningDashboard/Out_final_closing_dashboard.csv")
        print("Filtering columns .........")
        report_closing_dashboard_df = report_closing_dashboard_df[['incidentId','queue','subSubIssue','Group_SSI','13M X 40K Influencers File.Column2','is_Fav','lastSolvedOn','Created_At','Added2Queue','Problem_Statement','current_status']]
        print("Saving report data ..........")
        report_closing_dashboard_df.to_csv("SLAndOTIF_OpeningDashboard/Out_Report_closing_dashboard.csv", index=False)
        print("Saved to Out_Report_closing_dashboard.csv.")

    except Exception as e:
        print('Something went wrong! :', e)
