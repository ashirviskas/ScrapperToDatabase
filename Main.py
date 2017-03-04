import json
import time
import requests
import pymongo
import os
import datetime
import pprint

from pymongo import MongoClient
client = MongoClient('localhost', 27017)
WAIT_TIME = 2
LOADING_MESSAGE = "Loading"
file = open("address.txt", "r")
ADDRESS = file.readline()
print(ADDRESS)


class Part:
    def __init__(self, name, last_requested = time.time(), was_requested = False):
        self.last_requested = last_requested
        self.name = name
        self.was_requested = was_requested

    def __str__(self):
        return str(self.name, " Last requested: ", self.last_requested, " ago")


def GetJsonFromRequest(request_type):
    #request = requests.get("ADDRESS"+request_type)
    #request_id = json.loads(request.content).get("result")
    #print(request_id)
    request_id = "09fa0f3c-acc2-436d-8f7b-31ebd05c17c7"
    request = requests.get(ADDRESS + request_type +"/"+request_id)
    while (request.content == LOADING_MESSAGE):
        time.sleep(WAIT_TIME)
        request = requests.get(ADDRESS + request_type + "/" + request_id)
    data = json.loads(request.content)
    return data


def GenerateParts():
    Parts = []
    request = requests.get(ADDRESS)
    data = json.loads(request.content)
    endpoints = data.get('endpoints')
    for endpoint in endpoints:
        Parts.append(str(data['endpoints'][endpoint]))
    return Parts

def UpdateDatabaseTime(database, parts):
    twelve_hours = 864000   #12 hours in seconds
    for part in parts:
        if time.time() - part.last_requested > twelve_hours:
            

#def SendJsonToDatabase(json_data, database, ):




#Parts = GenerateParts()
#for part in Parts:
#    print(part)

client = MongoClient('localhost', 27017)
db = client.project
cpus = db.cpus
test_obj = {"sky_id": "1254",
            "part_name": "A decent cpu",
            "hardware_stuffs": ["LGA1151", "test"],
            "Speed": "3.4GHz",
            "date": datetime.datetime.utcnow()}
result = cpus.insert_one(test_obj)
print(result.inserted_id)
pprint.pprint(cpus.find_one({"sky_id": "1254"}))
