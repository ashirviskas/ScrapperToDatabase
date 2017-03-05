import json
import time
import requests
import os
import datetime
import pprint
import pymongo
from pymongo import MongoClient


client = MongoClient('localhost', 27017)
WAIT_TIME = 2 #time to wait for the worker on scrapper server to finish
LOADING_MESSAGE = "Loading"
file = open("address.txt", "r")
ADDRESS = file.readline()
print(ADDRESS)
Parts = {}

class Part:
    def __init__(self, name, collection, was_requested = False, last_requested = time.time()):
        self.last_requested = last_requested
        self.name = name
        self.collection = collection
        self.was_requested = was_requested

    def __str__(self):
        return str(self.name, " Last requested: ", self.last_requested)

    def LoadToDatabase(self, data):
        self.collection.data.insert(data, check_keys=False)

def GetJsonFromRequest(request_type):
    request = requests.get("ADDRESS"+request_type) #gets request id from the server
    request_id = json.loads(request.content).get("result")
    #print(request_id)
    request = requests.get(ADDRESS + request_type +"/"+request_id)
    while request.content == LOADING_MESSAGE:
        time.sleep(WAIT_TIME)
        request = requests.get(ADDRESS + request_type + "/" + request_id)
    data = json.loads(request.content)
    return data


def GeneratePartsDefault():
    global client
    Parts = {}
    Parts['cpu'] = Part('cpu', client.cpu)
    Parts['motherboard'] = Part('motherboard', client.motherboard)
    Parts['cooler'] = Part('cooler', client.cooler)
    Parts['casecooler'] = Part('casecooler', client.casecooler)
    Parts['ram'] = Part('ram', client.ram)
    Parts['hdd'] = Part('hdd', client.hdd)
    Parts['ssd'] = Part('ssd', client.ssd)
    Parts['gpu'] = Part('gpu', client.gpu)
    Parts['case'] = Part('case', client.case)
    Parts['psu'] = Part('psu', client.psu)
    Parts['dvd'] = Part('dvd', client.dvd)
    return Parts

def UpdateDatabase(parts, forced):
    twelve_hours = 864000   #12 hours in seconds
    for part in parts:
        if time.time() - part.last_requested > twelve_hours:
            print()
            
def UpdatePart(partname):
    global Parts
    part = Parts[partname]
    data = GetJsonFromRequest(partname)
    part.LoadToDatabase(data)



#def SendJsonToDatabase(json_data, database, ):
def Initialize():
    global Parts
    Parts = GeneratePartsDefault()

Initialize()
### TESTING FOLLOWS: ###
file = open("data_dvd_testing", "r", encoding="utf8") #
json_data = json.loads(file.read())
Parts['dvd'].LoadToDatabase(json_data)
pprint.pprint(client.dvd.data.find_one({"contents":"5"}))
"""test_obj = {"sky_id": "1254",
            "part_name": "A decent cpu",
            "hardware_stuffs": ["LGA1151", "test"],
            "Speed": "3.4GHz",
            "date": datetime.datetime.utcnow()}
print(result.inserted_id)
pprint.pprint(cpus.find_one({"sky_id": "1254"}))
"""
###TESTING END###