import csv
import requests
import json
import datetime

out_file = open("CurrentStatusOutput.csv", "+a", newline="")
csv_writer = csv.writer(out_file)
csv_writer.writerow(['order_Id','order_created_at','internalId','itemType','odStatus','item_fromStatus','item_toStatus','item_eventContext','item_comments','ItemUnitId','unitType','unit_fromStatus','unit_toStatus','to_time','to_createdAt','marketplace'])

def get_incident_data(order_Id):
    try:
        url = "http://10.83.37.123/oms/3.0/orders/" + order_Id

        payload = {}
        headers = {
            'X_OMS_CLIENT_ID': 'CHORE'
        }

        res = requests.request("GET", url, headers=headers, data=payload)
        if res.status_code == 200:
            od = json.loads(res.text)["order"]["orderItems"]
            for odItem in od.keys():
                order_created_at = datetime.datetime.fromtimestamp(json.loads(res.text)["order"]["createdAt"]/ 1000).strftime('%Y-%m-%d %H:%M:%S')
                itemType = od[odItem]["itemType"]
                odStatus = od[odItem]["status"]
                #marketplace = 'NA' #od[odItem]["marketplace"]
                internalId = od[odItem]["internalId"]
                item_statusHistories = od[odItem].get("statusHistories",[])
                if item_statusHistories:
                   item_last_status_history = item_statusHistories[-1]
                   item_fromStatus = item_last_status_history.get("fromStatus", "")
                   item_toStatus = item_last_status_history.get("toStatus", "")
                   item_eventContext = item_last_status_history.get("eventContext", "")
                   item_comments = item_last_status_history.get("comments", "")
                else:
                    item_fromStatus = "NA"
                    item_toStatus = "NA"
                    item_eventContext = "NA"
                    item_comments = "NA"

                data = json.loads(res.text)["order"]["orderItems"][odItem]["orderItemUnits"]
                for odUnitId in data.keys():
                    ItemUnitId = data[odUnitId]["id"]
                    unitType = data[odUnitId]["unitType"]
                    unit_statusHistories =  data[odUnitId].get("statusHistories",[])
                    if unit_statusHistories:
                        unit_last_status_history = unit_statusHistories[-1]
                        unit_fromStatus = unit_last_status_history.get("fromStatus","")
                        unit_toStatus = unit_last_status_history.get("toStatus", "")
                        unit_to_time = unit_last_status_history.get("statusTime", "")
                        unit_to_createdAt = unit_last_status_history.get("createdAt", "")
                        unit_eventContext = unit_last_status_history.get("eventContext", "")
                        if unit_to_time == "":
                            to_time = ""
                        else:
                            to_time = datetime.datetime.fromtimestamp(unit_to_time/ 1000).strftime('%Y-%m-%d %H:%M:%S')
                        if unit_to_createdAt == "":
                            to_createdAt = ""
                        else:
                            to_createdAt = datetime.datetime.fromtimestamp(unit_to_createdAt/ 1000).strftime('%Y-%m-%d %H:%M:%S')



                        print(f"order_Id: {order_Id},order_created_at:{order_created_at}, orderItemId: {internalId}, ItemType: {itemType},orderItemStatus: {odStatus},item_fromStatus: {item_fromStatus}, item_toStatus: {item_toStatus}, item_eventContext: {item_eventContext}, item_comments: {item_comments} ,ItemUnitId: {ItemUnitId},ItemUnitType: {unitType}, unit_fromStatus: {unit_fromStatus}, unit_toStatus: {unit_toStatus}, unit_to_time: {to_time},to_createdAt: {to_createdAt}, unit_eventContext: {unit_eventContext}, marketplace: ")
                        out_file.write(f"{order_Id},{order_created_at},{internalId},{itemType},{odStatus},{item_fromStatus},{item_toStatus},{item_eventContext},{item_comments},{ItemUnitId},{unitType},{unit_fromStatus},{unit_toStatus},{to_time},{to_createdAt}\n")


        else:
            print(f"Unable to fetch data from order_Id: {order_Id}")

    except Exception as e:
        print("Unable to fetch data from order_Id:", str(e))

        # Print to the terminal
        print(f"Order_Id: {order_Id} 1st, Status Code: {res.status_code}, Error: {e}")

        # Save to CSV
        out_file.write(f"{order_Id},{str(f'status_code : {res.status_code}')},{str(e)}\n")

if __name__ == '__main__':
    try:
        fileName = 'input.csv'
        print("Data processing ....")
        with open(fileName, "r") as csvfile:
            reader = csv.reader(csvfile)
            for row in reader:
                print("Processing row:", row)  # Add this line for debugging
                get_incident_data(row[0])
        print("Data processed successfully")
    except Exception as e:
        print('Something went wrong! :', e)

