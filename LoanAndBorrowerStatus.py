import requests
import json
import csv
import sys

out_file = open("CombinedReport_1.csv", "w")


def get_acc_status(acc_id):
    Tijori_url = "http://10.83.32.255/bnpl-tijori/v2/loanacc/%s/borrower?productCode=FLIPKART_ADVANZ" % acc_id
    payload = {}
    headers = {
        "X-User-Id": acc_id,
        "X-Merchant-Id": "mp_flipkart"
    }

    tijori_response = requests.request("GET", Tijori_url, headers=headers, data=payload)
    if tijori_response.status_code == 200:
        if json.loads(tijori_response.text)["data"]:
            acc_state = json.loads(tijori_response.text)["data"][0]["account_state"]
            borrower_state = json.loads(tijori_response.text)["data"][0]["borrower"]["borrower_state"]
        else:
            acc_state = "NA"
            borrower_state = "NA"
    else:
        acc_state = "NA"
        borrower_state = "NA"

    out_file.write(str(acc_id) + "," + acc_state + "," + borrower_state + "\n")


with open("acc_id.csv", "r") as csvfile:
    reader = csv.reader(csvfile)
    count = 0
    out_file.write("acc_id,Loan_state,Borrower_state" + "\n")
    for row in reader:
        get_acc_status(row[0])
        count = count + 1
        print(count)