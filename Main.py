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
            result_del = self.collection.delete_many({})
            result_ins = self.collection.insert(data, check_keys=False)
        print("deleted: ", result_del.deleted_count)
        print("inserted: ", len(result_ins))
        self.collection.insert_one({"name": self.name,
                                    "date": datetime.datetime.utcnow(),
                                    "deleted": result_del.deleted_count,
                                    "inserted": len(result_ins)})
    def LastUpdated(self):
        stuffs = self.collection.find({"name": self.name}, {'date': 1, "_id": False}).sort("date",-1).limit(1)
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
    Parts['cpu'] = Part('cpu', client.Scrapper_Project.cpu)
    Parts['motherboard'] = Part('motherboard', client.motherboard)
    Parts['cooler'] = Part('cooler', client.cooler)
    Parts['casecooler'] = Part('casecooler', client.casecooler)
    Parts['ram'] = Part('ram', client.Scrapper_Project.ram)
    Parts['hdd'] = Part('hdd', client.Scrapper_Project.hdd)
    Parts['sdd'] = Part('sdd', client.Scrapper_Project.ssd)
    Parts['gpu'] = Part('gpu', client.Scrapper_Project.gpu)
    Parts['case'] = Part('case', client.Scrapper_Project.case)
    Parts['psu'] = Part('psu', client.Scrapper_Project.psu)
    Parts['dvd'] = Part('dvd', client.Scrapper_Project.dvd)
    return Parts

def UpdateDatabase(parts, forced = False, every_few_hours = 12, sleeptime = 60*60): # parts dictionary, forced - is forced, sleeptime - time to sleep, so won't ddos skytech
    twelve_hours = timedelta(hours = every_few_hours)   #12 hours in seconds
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

def EternalUpdating(every_few_hours = 12, sleeptime_seconds = 60*60):
    global Parts
    while True:
        UpdateDatabase(Parts, False, every_few_hours, sleeptime_seconds)

#def SendJsonToDatabase(json_data, database, ):
def Initialize():
    global Parts
    Parts = GeneratePartsDefault()

Initialize()
EternalUpdating()
# print("updating ssd")
# UpdatePart("sdd")
# time.sleep(10)
# print("updating ram")
# UpdatePart("ram")
# time.sleep(10)
# print("updating hdd")
# UpdatePart("hdd")
# time.sleep(10)
# print("updating gpu")
# UpdatePart("gpu")
# time.sleep(10)
# print("updating case")
# UpdatePart("case")
# time.sleep(10)
# print("updating psu")
# UpdatePart("psu")
print("FINISHED")



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