import csv
import requests
import  json

def getCollectableId(acc):
    url = "http://10.83.32.138/bnpl-collections/1/collectables/unpaid/FK_ADVANZ_STATEMENT/"+acc

    payload = {}
    headers = {
      'Content-Type': 'application/json',
      'X-User-Id': acc
    }

    response = requests.request("GET", url, headers=headers, data=payload)

    x =json.loads(response.text)
    if len(x)!=0 and x[0]['state'] != 'PAID':
        return x[0]['collectableId']
    return None


res= []
with open('input.csv') as csv_file:
    csv_reader = csv.reader(csv_file, delimiter=',')
    for row in csv_reader:
            collectable_id = getCollectableId(row[0])
            if collectable_id is not None:
                res.append(row[0] +","+"https://finapi.flipkart.com/1/fintech/shylock/blacklisted/initiate?collectable_id="+collectable_id+"\n")


f = open("out.csv", "w")
for x in res:
  f.write(x)
f.close()
