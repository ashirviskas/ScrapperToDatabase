import json
import time
import requests
import os
import datetime
import pprint
import pymongo
from datetime import timedelta
from pymongo import MongoClient


client = MongoClient('localhost', 27017)
WAIT_TIME = 2 #time to wait for the worker on scrapper server to finish
LOADING_MESSAGE = '"Loading"\n'
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
        result_del = ""
        result_ins = ""
        if data is not []:
            result_del = self.collection.data.delete_many({})
            result_ins = self.collection.data.insert(data, check_keys=False)
        print("deleted: ", result_del.deleted_count)
        print("inserted: ", len(result_ins))
        self.collection.log.insert_one({"date": datetime.datetime.utcnow(),
                                        "deleted":result_del.deleted_count,
                                        "inserted":len(result_ins)})
    def LastUpdated(self):
        stuffs = self.collection.log.find({}, {'date': 1, "_id": False}).sort("date",-1).limit(1)
        for stuff in stuffs:
            return stuff

def GetJsonFromRequest(request_type):
    global ADDRESS
    request = requests.get(ADDRESS+request_type) #gets request id from the server
    request_id = json.loads(request.content).get("result")
    #print(request_id)
    request = requests.get(ADDRESS + request_type +"/"+request_id)
    while request.text == LOADING_MESSAGE:
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

def UpdateDatabase(parts, forced = False, sleeptime = 60*60): # parts dictionary, forced - is forced, sleeptime - time to sleep, so won't ddos skytech
    twelve_hours = timedelta(hours = 12)   #12 hours in seconds
    for key in parts:
        if parts[key].LastUpdated() is not None:
            if datetime.time() - parts[key].LastUpdated() > twelve_hours or forced:
                UpdatePart(key)
                #parts[key].LoadToDatabase(GetJsonFromRequest(parts[key].name))
                time.sleep(sleeptime) #updates different parts every hour
        else:
            UpdatePart(key)
            #parts[key].LoadToDatabase(GetJsonFromRequest(parts[key].name))
            if not forced:
                time.sleep(sleeptime)  # updates different p


def UpdatePart(partname):
    global Parts
    part = Parts[partname]
    data = GetJsonFromRequest(partname)
    part.LoadToDatabase(data)

def EternalUpdating():
    global Parts
    while True:
        UpdateDatabase(Parts)

#def SendJsonToDatabase(json_data, database, ):
def Initialize():
    global Parts
    Parts = GeneratePartsDefault()

Initialize()
UpdateDatabase(Parts, True)
#EternalUpdating()



### TESTING FOLLOWS: ###
# file = open("data_dvd_testing", "r", encoding="utf8") #
# json_data = json.loads(file.read())
# Parts['dvd'].LoadToDatabase(json_data)
# pprint.pprint(Parts['dvd'].LastUpdated())
# """for dvd in client.dvd.data.find():
#     pprint.pprint(dvd)"""
# pprint.pprint(client.dvd.data.find({"contents":"5"}))
"""test_obj = {"sky_id": "1254",
            "part_name": "A decent cpu",
            "hardware_stuffs": ["LGA1151", "test"],
            "Speed": "3.4GHz",
            "date": datetime.datetime.utcnow()}
print(result.inserted_id)
pprint.pprint(cpus.find_one({"sky_id": "1254"}))
"""
###TESTING END###