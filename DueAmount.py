import requests
import json
import csv
import sys

out_file = open("SkylerDue1.csv", "w")


def get_acc_status(acc_id):
    skyler_url = "http://10.83.34.243/fintech-skyler/1/discovery/FLIPKART_ADVANZ/accountSummary"
    payload = {}
    headers = {
        "X-User-Id": acc_id,
        "X-Merchant-Id": "mp_flipkart"
    }
    skyler_header = {
        'X-TENANT-ID': 'ARMSTRONG',
        'X-Client-ID': 'a',
        'X-Request-ID': 'a',
        'X-Trace-ID': 'a',
        'X-User-Id': acc_id,
        'X-Customer-Id': acc_id
    }
    skyler_response = requests.request("GET", skyler_url, headers=skyler_header)
    if skyler_response.status_code == 200:
        if "minimumDueAmount" in json.loads(skyler_response.text):
            skyler_minimumDueAmount = json.loads(skyler_response.text)["minimumDueAmount"]
        else:
            skyler_minimumDueAmount = "NA"
        if 'stmtDueDate' in json.loads(skyler_response.text):
            skyler_statementDueDate = json.loads(skyler_response.text)["stmtDueDate"]
        else:
            skyler_statementDueDate = "NA"
        if 'stmtAmount' in json.loads(skyler_response.text):
            skyler_stmtAmount = json.loads(skyler_response.text)["stmtAmount"]
        else:
            skyler_stmtAmount = "NA"
    else:
        skyler_minimumDueAmount = "NA"
        skyler_statementDueDate = "NA"
        skyler_stmtAmount = "NA"

    out_file.write(str(acc_id) + "," + str(skyler_minimumDueAmount) + "," + str(skyler_statementDueDate) + "," + str(
        skyler_stmtAmount) + "\n")


with open("dueamountinput.csv", "r") as csvfile:
    reader = csv.reader(csvfile)
    count = 0
    out_file.write("acc_id,skyler_minimumDueAmount,skyler_statementDueDate,skyler_stmtAmount" + "\n")
    for row in reader:
        get_acc_status(row[0])
        count = count + 1
        print(count)