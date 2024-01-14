#!/usr/bin/env python3

import pymongo
import os
from dotenv import load_dotenv
import json

# Load environment variables from .env file
load_dotenv()
connection_string = os.getenv("MONGO_CONNECTION_STRING")
database_name = os.getenv("MONGO_DB_NAME")
collection_name = os.getenv("MONGO_PLACES_COLLECTION_NAME")

# Query the place coordinates
client = pymongo.MongoClient(connection_string)
collection = client[database_name][collection_name]
documents = collection.find({}, {'place_id': 1, 'lat': 1, 'lon': 1})
place_coordinates = {document['place_id']:{'lat':float(document['lat'][:-1]), 'lon':float(document['lon'][:-1])} for document in documents}

# Write data to a file
script_dir = os.path.dirname(os.path.abspath(__file__))
file_path = os.path.join(script_dir, '../data/place_coordinates.json')
with open(file_path, "w") as f:
    f.write(json.dumps(place_coordinates, indent=4))
print(f"Successfully extracted coordinate data of {len(place_coordinates)} places")