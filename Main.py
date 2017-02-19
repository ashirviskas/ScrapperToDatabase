import json
import time
import requests
WAIT_TIME = 2

def GetJsonFromRequest(request_type):
    #request = requests.get("http://46.101.173.126:5000/"+request_type)
    #request_id = json.loads(request.content).get("result")
    #print(request_id)
    request_id = "09fa0f3c-acc2-436d-8f7b-31ebd05c17c7"
    request = requests.get("http://46.101.173.126:5000/" + request_type +"/"+request_id)
    while (request.content == "Loading"):
        time.sleep(WAIT_TIME)
        request = requests.get("http://46.101.173.126:5000/" + request_type + "/" + request_id)
    data = json.loads(request.content)
    return data


#r = requests.get("http://46.101.173.126:5000/cpu/c7b219df-6919-4747-8ef3-6623b77c4870")
#data = json.loads(r.content)
#print(data[1])
data = GetJsonFromRequest("cpu")
print(data[1])