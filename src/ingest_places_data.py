#!/usr/bin/env python3

import requests
import pymongo
import os
from dotenv import load_dotenv
import json
from fuzzywuzzy import fuzz
from pprint import pprint
import time
import traceback

# Load environment variables
load_dotenv()

# Load place names
script_dir = os.path.dirname(os.path.abspath(__file__))
file_path = os.path.join(script_dir, "../data/place_names.json")
with open(file_path, "r") as f:
    place_names = json.load(f)



def get_places_data(place_names):
    url = "https://ai-weather-by-meteosource.p.rapidapi.com/find_places"
    headers = {
        "X-RapidAPI-Key": os.getenv("PLACES_API_KEY"),
        "X-RapidAPI-Host": "ai-weather-by-meteosource.p.rapidapi.com"
    }
    places_data = {}
    print(f"\nGetting places data for {len(place_names)} places...")
    
    for index, place in enumerate(place_names):
        en_name = place["en"]
        vi_name = place["vi"]
        places_data[en_name] = "Not found"
        querystring = {"text":en_name,"language":"en"}
        response = requests.get(url, headers=headers, params=querystring).json()
        
        # Check the type of the response
        # If it is a dictionary, the request is not successful
        if type(response) == dict:
            print(f"The request is not successful.\nError message: {response['message']}\n")
            break
        # If the response is None, there is no match for the place
        elif response == None:
            continue
        # If it is a list, the place is FOUND
        elif type(response) == list:
            if len(response) == 1:
                places_data[en_name] = response[0]
            else:
                max_similarity = 0
                most_similar_place = None
                for result in response:
                    if result["country"] != "Socialist Republic of Vietnam":
                        continue
                    en_similarity = fuzz.ratio(en_name.lower(), result["name"].lower())
                    vi_similarity = fuzz.ratio(vi_name.lower(), result["name"].lower())
                    if en_similarity == 100:
                        most_similar_place = result
                        break
                    more_similar = max(vi_similarity, en_similarity)
                    if more_similar > max_similarity:
                        max_similarity = more_similar
                        most_similar_place = result
                places_data[en_name] = most_similar_place
        # Check if the place data is found
        if places_data[en_name] == "Not found":
            print(f"{index + 1}. {en_name} was NOT FOUND")
        else:
            print(f"{index + 1}. {en_name} was FOUND")
        # Wait 1s before continuing with the next place
        time.sleep(3)
    
    return places_data



def insert_places_data(places_data):
    # Get values of environment variables for MongoDB
    connection_string = os.getenv("MONGO_CONNECTION_STRING")
    database_name = os.getenv("MONGO_DB_NAME")
    collection_name = os.getenv("MONGO_PLACES_COLLECTION_NAME")    
    # Check if the data is empty
    if len(places_data) == 0:
        print("\nNo data to be inserted")
        return
    # Restructure data
    documents = [data for place, data in places_data.items()]
    # Connect to MongoDB
    client = pymongo.MongoClient(connection_string)
    try:
        client.server_info()
        print("Successfully connected to MongoDB")
        collection = client[database_name][collection_name]
        # Insert data
        try:
            print(f"Start inserting {len(documents)} documents into '{database_name}.{collection_name}' collection")
            collection.insert_many(documents, ordered=False)
            print(f"Successfully inserted all documents into '{database_name}.{collection_name}' collection")
        except Exception as insert_error:
            print(f"Failed to insert places data into '{database_name}.{collection_name}' collection\nError message:")
            pprint(insert_error)
    except pymongo.errors.ConnectionFailure as error:
        print(f"Failed to connect to MongoDB.\nError message: \"{error}\"\n{traceback}")
        with open(os.path.join(script_dir, '../data/places_data.json'),"w") as f:
            f.write(json.dumps(places_data, indent=4))



# Get places data and insert to MongoDB
if __name__ == "__main__":
    places_data = get_places_data(place_names)
    insert_places_data(places_data)