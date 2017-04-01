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
WAIT_TIME = 15 #time to wait for the worker on scrapper server to finish
LOADING_MESSAGE = '"Loading"\n'
file = open("address.txt", "r")
ADDRESS = file.readline()
print(ADDRESS)
Parts = {}
mandatory_fields = []


class Part:
    def __init__(self, name, collection, database = None, was_requested = False, last_requested = time.time(), parttype = None):
        self.last_requested = last_requested
        self.name = name
        self.collection = collection
        self.was_requested = was_requested
        self.parttype = parttype
        self.database = database

    def __str__(self):
        return str(self.name, " Last requested: ", self.last_requested)

    def load_to_database(self, data):
        result_del = ""
        result_ins = ""
        if data is not []:
            for part in data:
                part = self.parttype.filter_out(part)
            result_del = self.collection.delete_many({})
            result_ins = self.collection.insert(data, check_keys=False)
        print("deleted: ", result_del.deleted_count)
        print("inserted: ", len(result_ins))
        self.database.log.insert_one({"name": self.name,
                                    "date": datetime.datetime.utcnow(),
                                    "deleted": result_del.deleted_count,
                                    "inserted": len(result_ins)})
    def last_updated(self):
        stuffs = self.collection.find({"name": self.name}, {'date': 1, "_id": False}).sort("date",-1).limit(1)
        for stuff in stuffs:
            return stuff
    def set_parttype(self, parttype):
        self.parttype = parttype


class PartType:
    def __init__(self, partname, dictionary, values_needed):
        self.partname = partname
        self.dictionary = dictionary
        self.values_needed = values_needed

    def filter_out(self, obj):
        # obj = json.loads(json_data)
        new_json = {}
        try:
            new_json['price'] = obj['price']['eu']
        except:
            return
        try:
            new_json['contents'] = obj['contents']
        except:
            return
        try:
            new_json['name'] = obj['name']
        except:
            return
        try:
            new_json['model'] = obj['model']
        except:
            return
        try:
            new_json['url'] = obj['url']
        except:
            return
        for attribute, value in obj.items():
            if attribute in self.values_needed:
                new_json[attribute] = value
        for value in self.values_needed:
            if value not in new_json:
                return False
        return new_json


def generate_parttypes():
    # Ram part generating
    part_types = {}
    values_needed = []
    values_needed.append("Atminties talpa")
    values_needed.append("Atminties rūšis")
    values_needed.append("Atminčių skaičius rinkinyje")
    values_needed.append("Radiatorius")
    values_needed.append("Atminties tipas")
    values_needed.append("CAS Latency (CL) atidėjimas")
    part_types['ram'] = PartType("ram", None, list(values_needed))
    # Psu part
    values_needed.clear()
    values_needed.append("Maitinimo šaltinio galia")
    values_needed.append("Aukštis")
    values_needed.append("Plotis")
    values_needed.append("Formatas")
    values_needed.append("Gylis")
    values_needed.append("Maitinimo šaltinio standardas (ATX)")
    values_needed.append("Maitinimo kištukų 6-pin (PCI-E) kiekis")
    part_types["psu"] = PartType("psu", None, list(values_needed))
    # Case part
    values_needed.clear()
    values_needed.append("Aukštis")
    values_needed.append("Ilgis")
    values_needed.append("Plotis")
    values_needed.append("Įmontuotas maitinimo blokas(-ai)")
    values_needed.append("Maitinimo šaltinio galia")
    values_needed.append("Korpuso rūšis (pagrindinės plokštės tipas)")
    values_needed.append("Midi Tower")
    part_types["case"] = PartType("case", None, list(values_needed))
    # Cooler part
    values_needed.clear()
    values_needed.append("Procesoriaus lizdo tipas")
    values_needed.append("Radiatoriaus aukštis")
    values_needed.append("Radiatoriaus plotis")
    values_needed.append("Ventiliatoriaus plotis")
    values_needed.append("Ventiliatoriaus aukštis")
    values_needed.append("Ventiliatoriaus apsisukimų greitis")
    values_needed.append("Ilgaamžiškumas")
    part_types["cooler"] = PartType("cpu", None, list(values_needed))
    # Cpu part
    values_needed.clear()
    values_needed.append("Procesoriaus branduolių skaičius")
    values_needed.append("Procesoriaus tipas")
    values_needed.append("Maksimalus TDP")
    values_needed.append("Pridėtas ventiliatorius")
    values_needed.append("Integruota grafinė sistema")
    values_needed.append("Procesoriaus  taktavimo dažnis")
    values_needed.append("Procesoriaus lizdo tipas")
    values_needed.append("Procesoriaus tipas")
    part_types["cpu"] = PartType("cpu", None, list(values_needed))
    # dvd part
    values_needed.clear()
    values_needed.append("Plotis")
    values_needed.append("Gylis")
    values_needed.append("Aukštis")
    values_needed.append("DVD±R, DVD±RW, DVD±R DL, DVD-RAM, DVD-ROM, DVD-ROM DL, DVD-Video")
    part_types["dvd"] = PartType("dvd", None, list(values_needed))
    # gpu part
    values_needed.clear()
    values_needed.append("Chipset tipas")
    values_needed.append("Atminties magistralė")
    values_needed.append("Atminties dažnis (efektyvus)")
    values_needed.append("Instaliuota vaizdo atmintis")
    values_needed.append("Aukštis")
    values_needed.append("Chipset gamintojas")
    part_types["gpu"] = PartType("gpu", None, list(values_needed))
    # hdd part
    values_needed.clear()
    values_needed.append("HDD Capacity")
    values_needed.append("Rotation speed")
    values_needed.append("Form Factor")
    part_types["hdd"] = PartType("hdd", None, list(values_needed))
    # motherboard part
    values_needed.clear()
    values_needed.append("Chipset tipas")
    values_needed.append("Maksimalus atminties dydis")
    values_needed.append("Procesoriaus lizdo tipas")
    part_types["motherboard"] = PartType("motherboard", None, list(values_needed))
    # ssd part
    values_needed.clear()
    values_needed.append("Įrašymo greitis")
    values_needed.append("Nuskaitymo greitis")
    values_needed.append("Disko talpa")
    values_needed.append("Plotis")
    values_needed.append("Sąsaja")
    part_types["ssd"] = PartType("ssd", None, list(values_needed))

    return part_types

def get_json_from_request(request_type):
    global ADDRESS
    request = requests.get(ADDRESS+request_type)  # gets request id from the server
    request_id = json.loads(request.content).get("result")
    # print(request_id)
    request = requests.get(ADDRESS + request_type + "/" +request_id)
    while request.text == LOADING_MESSAGE:
        time.sleep(WAIT_TIME)
        request = requests.get(ADDRESS + request_type + "/" + request_id)
    data = json.loads(request.content)
    return data


def generate_parts_default():
    global client
    parts_l = {}
    parts_l['cpu'] = Part('cpu', client.Scrapper_Project.cpu, client.Scrapper_Project)
    parts_l['motherboard'] = Part('motherboard', client.motherboard, client.Scrapper_Project)
    parts_l['cooler'] = Part('cooler', client.cooler, client.Scrapper_Project)
    # parts_l['casecooler'] = Part('casecooler', client.casecooler, client.Scrapper_Project)
    parts_l['ram'] = Part('ram', client.Scrapper_Project.ram, client.Scrapper_Project)
    parts_l['hdd'] = Part('hdd', client.Scrapper_Project.hdd, client.Scrapper_Project)
    parts_l['ssd'] = Part('ssd', client.Scrapper_Project.ssd, client.Scrapper_Project)
    parts_l['gpu'] = Part('gpu', client.Scrapper_Project.gpu, client.Scrapper_Project)
    parts_l['case'] = Part('case', client.Scrapper_Project.case, client.Scrapper_Project)
    parts_l['psu'] = Part('psu', client.Scrapper_Project.psu, client.Scrapper_Project)
    parts_l['dvd'] = Part('dvd', client.Scrapper_Project.dvd, client.Scrapper_Project)
    return parts_l

def add_parttypes_to_parts(parts, parttypes):
    for key, part in parts.items():
        part.set_parttype(parttypes[part.name])


def initialize_mandatory_fields():
    global mandatory_fields
    mandatory_fields.append("Price")
    mandatory_fields.append("Name")
    mandatory_fields.append("Model")
    mandatory_fields.append("Units remaining")
    return True


def update_database(parts, forced = False, every_few_hours = 12, sleeptime =60 * 60):  # parts dictionary, forced - is forced, sleeptime - time to sleep, so won't ddos skytech
    twelve_hours = timedelta(hours = every_few_hours)   #12 hours in seconds
    for key in parts:
        if parts[key].last_updated() is not None:
            if datetime.time() - parts[key].last_updated() > twelve_hours or forced:
                update_part(key)
                #parts[key].LoadToDatabase(GetJsonFromRequest(parts[key].name))
                time.sleep(sleeptime) #updates different parts every hour
        else:
            update_part(key)
            #parts[key].LoadToDatabase(GetJsonFromRequest(parts[key].name))
            if not forced:
                time.sleep(sleeptime)  # updates different p


def update_part(partname):
    global Parts
    part = Parts[partname]
    data = get_json_from_request(partname)
    part.load_to_database(data)

def eternal_updating(every_few_hours = 12, sleeptime_seconds =60 * 60):
    global Parts
    while True:
        update_database(Parts, False, every_few_hours, sleeptime_seconds)

#def SendJsonToDatabase(json_data, database, ):
def initialize():
    global Parts
    parttypes = generate_parttypes()
    Parts = generate_parts_default()
    add_parttypes_to_parts(Parts, parttypes)

initialize()
#update_part("ram")
eternal_updating()
# print("updating ssd")
# update_part("ssd")
# time.sleep(10)
# print("updating ram")
# update_part("ram")
# time.sleep(10)
# print("updating hdd")
# update_part("hdd")
# time.sleep(10)
# print("updating gpu")
# update_part("gpu")
# time.sleep(10)
# print("updating case")
# update_part("case")
# time.sleep(10)
# print("updating psu")
# update_part("psu")
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