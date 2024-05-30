import requests
import json
import csv
import sys

out_file = open("CurrentUsage.csv","w")

def get_acc_status(acc_id):
 Khaata_url = "http://10.83.33.1/bnpl-khaata/1/account/FLIPKART_ADVANZ/customer/%s/account_summary" % acc_id
 payload={}
 headers = {
     "X-User-Id" : acc_id,
     "X-Merchant-Id" : "mp_flipkart"
 }
 khaata_response = requests.request("GET", Khaata_url)
 if khaata_response.status_code == 200:
     if 'currentUsage' in json.loads(khaata_response.text):
         currentUsage= json.loads(khaata_response.text)["currentUsage"]
     else:
         currentUsage = "NA"  
 else:
     currentUsage = "NA"
    
 out_file.write(str(acc_id)+","+str(currentUsage)+"\n")

with open("input.csv", "r") as csvfile:
  reader = csv.reader(csvfile)
  count=0
  out_file.write("acc_id,currentUsage" + "\n")
  for row in reader:
    get_acc_status(row[0])
    count=count+1
    print(count)