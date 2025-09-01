from pymongo import MongoClient
import pandas
import json
import os

# Connecting to mongodb
client = MongoClient("mongodb://localhost:27017/")
sales_db = client.sales_db

# Loading data

path = "/home/sohrab/datasci/etl-project/data/"
files = os.listdir(path)

for file in files:
    # creating collection based on name of file
    collection_name = file[:-5]
    file_path = path + file
    with open(file_path) as file:
        file_data = json.load(file)
    collection = sales_db[f'{collection_name}']
    collection.insert_many(file_data)




