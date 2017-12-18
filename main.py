from handle import *
from datetime import datetime
from functions import *
import json

def main():
    #a = create_type((("nom","string"),("date","time")))
    #print(a)
    #v = """{"id":"12","Adresse":{"num":"44", "rue":"rue de Paris"},"formation":{"ecole":{"nom":"paris-9","adresse":{"num":"11","rue":"avenue de france"}},"niveau":"master"}}"""
    #va = json.loads(v)
    #print(type(va))
    #print (va["Adresse"])
    #print(v["id"])
    #print(type(json.loads(v)))
    #print (to_json(v)["Adresse"]['num'])
    #print(type(to_json(v)))
    create_dataframe()
    #date="12-03-2017 10:00:00"
    #print(to_date(date,"%d-%m-%Y %H:%M:%S"))


main()