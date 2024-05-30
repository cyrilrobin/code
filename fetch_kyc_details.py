# Python script to fetch the kyc details.
import csv
import time
import datetime
import requests

# Please change the following parameter
# api_host : url of the environment
# input_file_path : path of the file that contains the accountID
# output_file_path : path at which you want to store the accontIds, applicationIds, Status
api_host = "10.47.100.53:80"
input_file_path = 'Consent Required.csv'
output_file_path = 'kyc_out.csv'


def get_status():
    output_list = []

    output_list.append(['Account ID','KYC Type','First Name','Middle Name','Last Name','DOB','Mobile number','Masked Aadhaar Number','Masked Pan number','Pan Name','Pan Number'])
    with open(input_file_path) as csv_file:
        csv_reader = csv.reader(csv_file, delimiter=',')
        for row in csv_reader:
            url = f"http://10.83.36.214:80/fintech-us/v1/user/{row[0]}/kyc-details"
            headers = {
                'X-Client-ID': 'pinaka',
                'Content-Type': 'application/json'
            }

            response = requests.get(url, headers=headers)
            if response.status_code == 200:
                kyc_type = response.json()["kyc_method"]
                customer_info = response.json()["customer_info"]
                dob_list = customer_info["dob"]
                dob = ''
                for i in dob_list:
                    dob = dob + str(i) + '/'

                output_data = [row[0], kyc_type, customer_info["first_name"], customer_info["middle_name"], customer_info["last_name"],
                               dob[:-1], customer_info["phone"], customer_info["masked_aadhaar_number"],
                               customer_info["masked_pan_number"], customer_info["pan_name"], customer_info["pan_number"]]

                print(output_data)
                output_list.append(output_data)
            else:
                print(f'failed to fetch kyc for acc - {row[0]}')
                output_list.append([row[0], "No data available"])

    write_data_in_file(output_list)




def write_data_in_file(output_list=None):
    with open(output_file_path, mode='w') as output_file:
        output_writer = csv.writer(output_file, quoting=csv.QUOTE_MINIMAL)
        output_writer.writerows(output_list)


if __name__ == '__main__':
    get_status()

#Account ID,KYC Type,First Name,Middle Name,Last Name,DOB,Mobile number,Masked Aadhaar Number,Masked Pan number,Pan Name,Pan Number
