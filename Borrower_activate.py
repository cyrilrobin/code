import json
import csv
import requests

tijori_api = "http://10.83.32.255/bnpl-tijori/v1/borrower/%s"


logSuccess = open("account_success.txt", "w")
logError = open("account_error.txt", "w")
logException = open("account_exception.txt", "w")


def updateBorrowerState():
    with open("borrower_payload.csv", "r") as csvfile:
        reader = csv.reader(csvfile)
        for row in reader:
            accountId = row[0]
            tijori_update = tijori_api % accountId
            headers = {"Content-Type": 'application/json', "X-Authorization": 'bXBfZmxpcGthcnQ6dHJha3BpbGZfcG0=',
            "X-Merchant-Id": 'mp_flipkart', "X-User-Id": accountId,'X-Client-Id':'khaata'}
            data = {"borrower_state": "ACTIVE"}
            response = requests.put(tijori_update, data=json.dumps(data), headers=headers)
            #print (response.status_code
            count=0
            if (response.status_code / 100 == 2):
                count=count+1
                print(count)
                #datas = json.loads(response.content)
                #logSuccess.write(str(accountId +"\n"))
            else:
                logError.write(accountId+"\n")
                #print("Failed -->" + accountId)


updateBorrowerState()

logSuccess.close()
logError.close()
logException.close()