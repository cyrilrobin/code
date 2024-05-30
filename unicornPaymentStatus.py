import csv
import requests
import json
import datetime

out_file1 = open("forwardPaymentStatusOutput.csv", "w", newline="")
csv_writer1 = csv.writer(out_file1)
csv_writer1.writerow(['parent_pzt','pzt_id','you_pay_amount','txn_time','merchant_id','txn_type','status','merchant_status','response_code','response_message','payment_instrument'])

out_file2 = open("RefundPaymentStatusOutput.csv", "w", newline="")
csv_writer2 = csv.writer(out_file2)
csv_writer2.writerow(['parent_pzt','ref_pzt_id','ref_amount','ref_txn_time','ref_merchant_id','ref_status','ref_response_code','ref_response_message','ref_track_id','ref_txn_id','ref_arn','ref_gateway_status','ref_gateway_response_code','payment_instrument'])


def get_incident_data(parent_pzt):
    try:
        url_oms = "http://10.83.37.162/unicorn/1.0/payments/timeline/%s/build" % parent_pzt

        payload = {}
        headers = {}

        res = requests.request("GET", url_oms, headers=headers, data=payload)
        if res.status_code == 200:
            unicorn = json.loads(res.text)
            payments = unicorn['sale_transactions']
            for pzt in payments.keys():
                pzt_id = payments[pzt]['sale_transaction_details']['id']
                you_pay_amount = payments[pzt]['sale_transaction_details']['you_pay_amount']
                txn_time = datetime.datetime.fromtimestamp(payments[pzt]['sale_transaction_details']['txn_time'] / 1000).strftime('%Y-%m-%d %H:%M:%S')
                merchant_id = payments[pzt]['sale_transaction_details']['merchant_id']
                txn_type = payments[pzt]['sale_transaction_details']['txn_type']
                status = payments[pzt]['sale_state_summary']['status']
                merchant_status = payments[pzt]['sale_state_summary']['merchant_status']
                response_code = payments[pzt]['sale_state_summary']['response_code']
                #response_message = payments[pzt]['sale_state_summary'].get("response_message","NA")
                response_message = "NA"
                payment_instrument = payments[pzt]['sale_transaction_details'].get("payment_instrument","NA")
     
                print(f"parent_pzt: {parent_pzt}, pzt_Id: {pzt_id},you_pay_amount:{you_pay_amount},txn_time:{txn_time},merchant_id:{merchant_id},txn_type:{txn_type},status:{status},merchant_status:{merchant_status},response_code:{response_code},response_message:{response_message},payment_instrument:{payment_instrument}")
                out_file1.write(f"{parent_pzt},{pzt_id},{you_pay_amount},{txn_time},{merchant_id},{txn_type},{status},{merchant_status},{response_code},{response_message},{payment_instrument}\n")

            if 'refund_transactions' in unicorn:
                refunds = unicorn['refund_transactions']
                for ref_pzt in refunds.keys():
                    ref_pzt_id = refunds[ref_pzt]['transaction_details']['id']
                    ref_amount = refunds[ref_pzt]['transaction_details']['amount']
                    ref_txn_time = datetime.datetime.fromtimestamp(refunds[ref_pzt]['transaction_details']['txn_time'] / 1000).strftime('%Y-%m-%d %H:%M:%S')
                    ref_merchant_id = refunds[ref_pzt]['transaction_details']['merchant_id']
                    ref_status = refunds[ref_pzt]['state_summary']['status']
                    ref_response_code = refunds[ref_pzt]['state_summary']['response_code']
                    #ref_response_message = refunds[ref_pzt]['state_summary']['response_message']
                    ref_response_message = "NA"
                    ref_track_id = refunds[ref_pzt]['gateway_response']['track_id']
                    ref_txn_id = refunds[ref_pzt]['gateway_response']['txn_id']
                    ref_arn = refunds[ref_pzt]['gateway_response'].get("reference_id2","NA")
                    ref_gateway_status = refunds[ref_pzt]['gateway_response']['status']
                    ref_gateway_response_code = refunds[ref_pzt]['gateway_response']['response_code']
                    payment_instrument = refunds[ref_pzt]['transaction_details']['payment_instrument']
                    print(f"parent_pzt: {parent_pzt},ref_pzt_id:{ref_pzt_id},ref_amount:{ref_amount},ref_txn_time:{ref_txn_time},ref_merchant_id:{ref_merchant_id},ref_status:{ref_status},ref_response_code:{ref_response_code},ref_response_message:{ref_response_message},ref_track_id:{ref_track_id},ref_txn_id:{ref_txn_id},ref_arn:{ref_arn},ref_gateway_status:{ref_gateway_status},ref_gateway_response_code:{ref_gateway_response_code},payment_instrument:{payment_instrument}")
                    out_file2.write(f"{parent_pzt},{ref_pzt_id},{ref_amount},{ref_txn_time},{ref_merchant_id},{ref_status},{ref_response_code},{ref_response_message},{ref_track_id},{ref_txn_id},{ref_arn},{ref_gateway_status},{ref_gateway_response_code},{payment_instrument}\n")
            else:
                print(f"{parent_pzt}: No refund found")
        else:
            print(f"Unable to fetch data from order_Id: {parent_pzt}")

    except Exception as e:
        print("Unable to fetch data from order_Id:", str(e))

        # Print to the terminal
        print(f"parent_pzt: {parent_pzt} 1st, Status Code: {res.status_code}, Error: {e}")

        # Save to CSV
        out_file1.write(f"{parent_pzt},{str(f'status_code : {res.status_code}')},{str(e)}\n")

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
