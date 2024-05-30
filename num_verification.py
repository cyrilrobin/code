import requests
import csv
import json

status_log = open("phone_number_output.csv", "a+")


def check_verification_status(acc_id):
    url = "http://10.83.37.91/loginservice/user/v2/%s/identifiers?phoneNumberFormat=E164" % acc_id

    payload = {}
    headers ={
        'Authorization': 'Bearer eyJhbGciOiJSUzI1NiJ9.eyJhdWQiOlsibG9naW4iLCJ1c2VyIl0sInNjb3BlIjpbInVzZXIuYWNjLnJlYWQiLCJ1c2VyLmNudC5yZWFkIiwidXNlci5hY2MuY29uZmlkZW50aWFsLnJlYWQiLCJsb2dpbi5pZGVudGlmaWVycy5yZWFkIiwidXNlci5hY2MuY29uZmlkZW50aWFsLndyaXRlIl0sImV4cCI6MTcxNzA5NjgzMiwiYXV0aG9yaXRpZXMiOlsiUk9MRV9DTElFTlQiXSwianRpIjoiY2U4ZTI0OGYtNGE1Ni00MjY5LWIwZTItMjlhMjJiNjQwMDI5IiwiY2xpZW50X2lkIjoiNWRmNzAwZjY0MWFhNDRiYmIyNDg5MTUzMWUxNzQwMzIifQ.MK90K7gDnhavHPBSQu8KIOnkhe84gw6AYoP6_fs9f5Nt226c1_1AmlAVi_A2_Y6H5xiqGlZEwLPMj_8QYIRaoKQ-r3m_WJtDyGq69hl82RKSUgy2BPRnSXgDD9Fci8b0jHLYolYbZBbWDzsoFbnvrDK-eA1HFaUTOmNAGEvBgzjukj9jy_jQdurzIzbTFEeu2fsMse4hag8Y7M8CJZbqTgrY7S9tXb57CAO1U9kQ7NfotLaPma3UGzcHuTFws1mxeO1osoT4VgtPeAsA4l_IMbHntivjGcXiKIlf-YJ-4EjsqR64qHA3YRopKrkf2jKQazEx7b36SMkmBfAXylUU2g'
    }
    response = requests.request("GET", url, headers=headers, data=payload)

    if response.text:
        response1 = json.loads(response.text.encode('utf8'))
        if response1 and "PHONE" in response1:
            phone_number_status = response1["PHONE"][0]["state"]
            print(acc_id, phone_number_status)
            status_log.write(acc_id +"," + phone_number_status + "\n")


file_name = "input.csv"
with open(file_name, "r") as csvfile:
    reader = csv.reader(csvfile)
    accountIdList = []
    for row in reader:
        acc_id = row[0]
        check_verification_status(acc_id)