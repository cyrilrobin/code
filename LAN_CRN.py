import requests
import json
import csv

out_file = open("LAN_report.csv", "w", newline='')  # Use newline='' to avoid extra empty lines in CSV

def get_acc_status(acc_id):
    Tijori_url = "http://10.83.32.255/bnpl-tijori/v2/loanacc/%s/borrower?productCode=FLIPKART_ADVANZ" % acc_id
    payload = {}
    headers = {
        "X-User-Id": acc_id,
        "X-Merchant-Id": "mp_flipkart"
    }

    tijori_response = requests.get(Tijori_url, headers=headers, data=payload)  # Use requests.get for GET requests
    if tijori_response.status_code == 200:
        data = json.loads(tijori_response.text).get("data", [])
        if data:
            lan = data[0].get("lan", "NA")
            crn = data[0].get("crn", "NA")
        else:
            lan = "NA"
            crn = "NA"
    else:
        lan = "NA"
        crn = "NA"

    out_file.write(str(acc_id) + "," + lan + "," + crn + "\n")

with open("LANCRN.csv", "r") as csvfile:
    reader = csv.reader(csvfile)
    next(reader, None)  # Skip the header row if it exists
    out_file.write("acc_id,lan,crn\n")
    count = 0
    for row in reader:
        get_acc_status(row[0])
        count += 1
        print(count)
