import requests
import csv
import json

status_log = open("phone_number_output.csv", "a+") 


def check_verification_status(acc_id):
    url = "http://10.83.37.91/loginservice/user/v2/%s/identifiers?phoneNumberFormat=E164" % acc_id

    payload = {}
    headers ={
        'Authorization': 'Bearer    eyJhbGciOiJSUzI1NiJ9.eyJhdWQiOlsibG9naW4iLCJ1c2VyIl0sInNjb3BlIjpbInVzZXIuYWNjLnJlYWQiLCJ1c2VyLmNudC5yZWFkIiwidXNlci5hY2MuY29uZmlkZW50aWFsLnJlYWQiLCJsb2dpbi5pZGVudGlmaWVycy5yZWFkIiwidXNlci5hY2MuY29uZmlkZW50aWFsLndyaXRlIl0sImV4cCI6MTcxNjc1MTIyMywiYXV0aG9yaXRpZXMiOlsiUk9MRV9DTElFTlQiXSwianRpIjoiOWFiMDE3ZDgtOGI3NS00ODYwLWIzN2MtOGNlODUyNmFjOGE2IiwiY2xpZW50X2lkIjoiNWRmNzAwZjY0MWFhNDRiYmIyNDg5MTUzMWUxNzQwMzIifQ.Bg-cIkXrk3GuTl_vJoRgBNQBMDIr23-Xn9dPqa6431aN0sWYD4N5ZKrIoisILIq2I8UuVyO3mD67Tok31uIk7kHFbPmI2QGaEV8tDVCOEhbHED2GECaDB3OxxVVdytspFg5R5eHM17H-eah5jybMkrUM4t2Dvl-GPEAbwwSxiLko7LfJG8cV2ftXqiEQwI9FcJ4GCA3CkaY5SKX5PeE32zd44kJeuY8Kh4xJ5KoBxm6rbeSVgVH9DiUkJ3SOg_RsTWQ9N3sTSMMSY-2HUHpiBCN1lhT9IyfhDNWlDoT1mHTaFWBoHvBFfHixBlQhUDQzS3wdlMtUqZtMs0ljfivIcA'
        }    
    
    response = requests.request("GET", url, headers=headers, data=payload)

    if response.text:
        response1 = json.loads(response.text.encode('utf8'))
        if response1 and "PHONE" in response1:
            phone_number_status = response1["PHONE"][0]["state"]
            phone_number = response1["PHONE"][0]["user_login_id"]
            print(acc_id, phone_number_status,phone_number)
            status_log.write(acc_id +"," + phone_number_status +","+phone_number+ "\n")


file_name = "input.csv"
with open(file_name, "r") as csvfile:
    reader = csv.reader(csvfile)
    accountIdList = []
    for row in reader:
        acc_id = row[0]
        check_verification_status(acc_id)