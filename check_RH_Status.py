import json
import csv
import requests

rh_api = "http://10.83.32.134/1/fintech/%s/getStatus?fetchBalance=true"
headers={}
log = open("RH_Result.txt","w")
#logError = open("discard_error.txt","wb")
#logException = open("discard_exception.txt","wb")


def get_rh_status():
    with open("check_RH_Status_input.csv","r") as csvfile:
        reader = csv.reader(csvfile)
        for row in reader:
            accountId = row[0]
            rh_status = rh_api%accountId
            data = {}
            response = requests.get(rh_status,data=json.dumps(data) ,headers=headers)
            data=json.loads(response.text)
            if(response.status_code/100 == 2):
                #print (data)
                if data["productTypeFintechMyAccountMap"].get("FLIPKART_ADVANZ"):
                    status = str(data["productTypeFintechMyAccountMap"].get("FLIPKART_ADVANZ")['status'])
                    print("Yes")
                    log.write(accountId + ",FLIPKART_ADVANZ," + status + "\n")
                else:
                    print("no")
                    log.write(accountId + ",FLIPKART_ADVANZ,Not_Eligible"+"\n" )


get_rh_status()


# logSuccess.close()
# logError.close()
# logException.close()