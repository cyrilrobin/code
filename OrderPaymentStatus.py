import csv
import requests
import json
import datetime

out_file = open("orderPaymentStatusOutput.csv", "+a", newline="")
csv_writer = csv.writer(out_file)
csv_writer.writerow(['order_Id','pzt_id','status','invalidate','paymentCategory','merchant_id'])

def get_incident_data(order_Id):
    try:
        url_oms = "http://10.83.37.123/oms/3.0/orders/" + order_Id

        payload_oms = {}
        headers_oms = {
            'X_OMS_CLIENT_ID': 'CHORE'
        }

        res_oms = requests.request("GET", url_oms, headers=headers_oms, data=payload_oms)
        if res_oms.status_code == 200:
            payments = json.loads(res_oms.text)["order"]["payments"]
            for pzt in payments.keys():
                pzt_id = pzt
                status = payments[pzt]['status']
                invalidate = payments[pzt]['invalidate']
                paymentCategory = payments[pzt].get("paymentCategory","NA")
                if 'paymentMessage' in payments[pzt]:
                    merchant_id = payments[pzt]['paymentMessage'].get("merchant_id",'NA')
                else:
                    merchant_id = 'NA'
                    
                print(f"orderId: {order_Id}, pzt_Id: {pzt_id},status:{status},invalidate:{invalidate},paymentCategory:{paymentCategory},merchant_id:{merchant_id}")
                out_file.write(f"{order_Id},{pzt_id},{status},{invalidate},{paymentCategory},{merchant_id}\n")


        else:
            print(f"Unable to fetch data from order_Id: {order_Id}")

    except Exception as e:
        print("Unable to fetch data from order_Id:", str(e))

        # Print to the terminal
        print(f"Order_Id: {order_Id} 1st, Status Code: {res_oms.status_code}, Error: {e}")

        # Save to CSV
        out_file.write(f"{order_Id},{str(f'status_code : {res_oms.status_code}')},{str(e)}\n")

if __name__ == '__main__':
    try:
        fileName = 'input.csv'
        print("Data processing ....")
        with open(fileName, "r") as csvfile:
            reader = csv.reader(csvfile)
            for row in reader:
                get_incident_data(row[0])
        print("Data processed successfully")
    except Exception as e:
        print('Something went wrong! :', e)
