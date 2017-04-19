# encoding=utf-8
import json
import time
import requests
import os
import datetime
import pprint
import pymongo
from datetime import timedelta
from pymongo import MongoClient

uri = open("database.txt", "r").readline()
client = MongoClient(uri)
LOG_COL = client.Scrapper_Project.log
WAIT_TIME = 15 #time to wait for the worker on scrapper server to finish
LOADING_MESSAGE = '"Loading"\n'
file = open("address.txt", "r")
ADDRESS = file.readline()
print(ADDRESS)
Parts = {}


class Part:
    def __init__(self, name, collection, log_collection = None, was_requested = False, last_requested = time.time(), parttype = None):
        self.last_requested = last_requested
        self.name = name
        self.collection = collection
        self.was_requested = was_requested
        self.parttype = parttype
        self.log_collection = log_collection

    def __str__(self):
        return str(self.name, " Last requested: ", self.last_requested)

    def load_to_database(self, data):
        result_del = ""
        result_ins = ""
        data_filtered = []
        if is_database_online():
            if data is not [] or data is not False:
                for part in data:
                    d = self.parttype.filter_out(part)
                    if d is not False:
                        data_filtered.append(d)
                if self.collection.count() > 0:
                    result_del = self.collection.delete_many({}).deleted_count
                else:
                    result_del = 0
                    # result_del.deleted_count = 0
                result_ins = self.collection.insert(data_filtered, check_keys=False)
        else:
            announce_an_error("Loading to database unsuccessful, nothing loaded")
            return
        print("Part: ", self.name)
        print("deleted: ", 0)
        print("inserted: ", len(result_ins))
        self.log_collection.insert_one({"name": self.name,
                                    "date": datetime.datetime.utcnow(),
                                    "deleted": result_del,
                                    "inserted": len(result_ins)})

    def last_updated(self):
        stuffs = self.log_collection.find({"name": self.name}, {'date': 1, "_id": False}).sort("date",-1).limit(1)
        for stuff in stuffs:
            return stuff
        return None

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
            return False
        try:
            new_json['contents'] = obj['contents']
        except:
            return False
        try:
            new_json['name'] = obj['name']
        except:
            return False
        try:
            new_json['model'] = obj['model']
        except:
            return False
        try:
            new_json['url'] = obj['url']
        except:
            return False
        for attribute, value in obj['attributes'].items():
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
    values_needed.append("Korpuso tipas")
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
    part_types["cooler"] = PartType("cooler", None, list(values_needed))
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
    values_needed.append("Nuskaitomi diskų formatai (DVD)")
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
    # casecooler part
    values_needed.clear()
    values_needed.append("Ilgaamžiškumas")
    values_needed.append("Ventiliatoriaus apsisukimų greitis")
    values_needed.append("Aukštis")
    values_needed.append("Plotis")
    values_needed.append("Storis")
    values_needed.append("Oro srautas")
    values_needed.append("Akustinis triukšmas")
    values_needed.append("Maitinimo jungties tipas")
    part_types["casecooler"] = PartType("casecooler", None, list(values_needed))
    return part_types


def get_json_from_request(request_type):
    global ADDRESS
    request = requests.get(ADDRESS+request_type)  # gets request id from the server
    #try:
    request_id = json.loads(request.content).get("result")
    #except:
    #    announce_an_error("Cannot decode JSON of: " + ADDRESS+request_type)
    #    return False
    # print(request_id)
    request = requests.get(ADDRESS + request_type + "/" +request_id)
    while request.text == LOADING_MESSAGE:
        time.sleep(WAIT_TIME)
        request = requests.get(ADDRESS + request_type + "/" + request_id)
    data = json.loads(request.content)
    return data


def is_database_online():
    global client
    try:
        client.server_info()
    except:
        announce_an_error("PyMongo server is offline or not accessible")
        return False
    return True


def generate_parts_default():
    global client
    parts_l = {}
    parts_l['cpu'] = Part('cpu', client.Scrapper_Project.cpu, client.Scrapper_Project.log)
    parts_l['motherboard'] = Part('motherboard', client.Scrapper_Project.motherboard, client.Scrapper_Project.log)
    parts_l['cooler'] = Part('cooler', client.Scrapper_Project.cooler, client.Scrapper_Project.log)
    parts_l['casecooler'] = Part('casecooler', client.Scrapper_Project.casecooler, client.Scrapper_Project.log)
    parts_l['ram'] = Part('ram', client.Scrapper_Project.ram, client.Scrapper_Project.log)
    parts_l['hdd'] = Part('hdd', client.Scrapper_Project.hdd, client.Scrapper_Project.log)
    parts_l['ssd'] = Part('ssd', client.Scrapper_Project.ssd, client.Scrapper_Project.log)
    parts_l['gpu'] = Part('gpu', client.Scrapper_Project.gpu, client.Scrapper_Project.log)
    parts_l['case'] = Part('case', client.Scrapper_Project.case, client.Scrapper_Project.log)
    parts_l['psu'] = Part('psu', client.Scrapper_Project.psu, client.Scrapper_Project.log)
    parts_l['dvd'] = Part('dvd', client.Scrapper_Project.dvd, client.Scrapper_Project.log)
    return parts_l

def add_parttypes_to_parts(parts, parttypes):
    for key, part in parts.items():
        part.set_parttype(parttypes[part.name])


def update_database(forced=False, every_few_hours=12, sleeptime=60 * 60):  # parts dictionary, forced - is forced,
    # sleeptime - time to sleep between parts, so won't ddos skytech
    twelve_hours = timedelta(hours = every_few_hours)   #12 hours in seconds
    for key in Parts:
        if Parts[key].last_updated() is not None: # if part was recently updated
            if datetime.datetime.utcnow() - Parts[key].last_updated()['date'] > twelve_hours or forced: # if updated more than twelve hours ago or forced
                update_part(key)
                time.sleep(sleeptime)
        else:
            update_part(key)
            if not forced:
                time.sleep(sleeptime)  # updates d


def update_part(partname):
    global Parts
    try:
        part = Parts[partname]
    except KeyError:
        announce_an_error("Error, no such partname in Parts: " + partname)
        return
    data = get_json_from_request(partname)
    if data is not False:
        part.load_to_database(data)


def announce_an_error(error_mes):
    global LOG_COL
    print(error_mes)
    LOG_COL.insert_one({"Error": error_mes,
                                    "date": datetime.datetime.utcnow()})

def eternal_updating(every_few_hours = 12, sleeptime_seconds =60 * 60):
    global Parts
    while True:
        update_database(False, every_few_hours, sleeptime_seconds)


def initialize():
    global Parts
    parttypes = generate_parttypes()
    Parts = generate_parts_default()
    add_parttypes_to_parts(Parts, parttypes)

initialize()
update_database(True, 12, 60)
#eternal_updating()
# print("updating ssd")
# update_part("ssd")
# time.sleep(10)
# print("updating ram")
# update_part("ram")
# time.sleep(10)
# print("updating hdd")
#update_part("dvd")
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
