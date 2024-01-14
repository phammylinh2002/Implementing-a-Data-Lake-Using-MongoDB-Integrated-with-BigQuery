#!/usr/bin/env python3

import requests
import pymongo
import json
import os
import logging
import traceback
import time
import boto3
from dotenv import load_dotenv
from datetime import datetime, timedelta



def format_traceback():
    return traceback.format_exc().replace('\n', ' | ')



def notify_error(session, message):
    sns_client = session.client("sns")
    try:
        sns_client.publish(
            TopicArn = os.getenv("INGESTION_TOPIC_ARN"),
            Subject = "Failure occured while ingesting weather data to MongoDB",
            Message = message
        )
        logger.info("Email notification sent successfully.")
    except Exception as e:
        logger.error(f"Failed to send email notification. Error message: {e}. {format_traceback()}.")



def save_to_s3(session, saved_data):
    data = {index:data for index, data in enumerate(saved_data)}
    file_name = 'failed_inserts_' + ''.join(character for character in str(expected_last_updated) if character not in ["-", " ", ":"]) + '.json'
    try:
        s3 = session.resource('s3')
        s3.Object(os.getenv("BUCKET_NAME"), file_name).put(Body=json.dumps(data, indent=4))
        logger.info(f"Sucessfully saved {file_name} to S3")
    except Exception as e:
        message = f"An error occurred while trying to save data to S3: {e}. {format_traceback()}."
        logger.error(message)
        notify_error(session, message)



def get_weather_data(place_coordinates):
    url = "https://weatherapi-com.p.rapidapi.com/current.json"
    headers = {
        "X-RapidAPI-Key": os.getenv("WEATHER_API_KEY"),
        "X-RapidAPI-Host": "weatherapi-com.p.rapidapi.com"
    }
    weather_data = []
    logger.info(f"Start getting weather data at {expected_last_updated} for {len(place_coordinates)} places")
    # Start a loop to try getting weather data for each place in up to 3 times
    for i in range(3):
        failed_places = {}
        for place_id, coordinate in place_coordinates.items():
            try:
                querystring = {"q":f"{coordinate['lat']},{coordinate['lon']}"}
                response = requests.get(url, headers=headers, params=querystring).json()
                if datetime.strptime(response["current"]["last_updated"]+":00", "%Y-%m-%d %H:%M:%S") == expected_last_updated:
                    response["place_id"] = place_id
                    weather_data.append(response)
                    logger.info(f"Got {place_id}")
                else:
                    logger.warning(f"Failed to get weather data for '{place_id}'. Expect ['current']['last_updated'] as {expected_last_updated}, got {response['current']['last_updated']}")
                    failed_places[place_id] = coordinate
                    continue
            except Exception as e:
                logger.warning(f"Failed to get weather data for {place_id}. Error: {e}")
                failed_places[place_id] = coordinate
                continue
        if len(failed_places) == 0:
            break
        else:
            if i in range(2):
                logger.info(f"Retrying to get weather data for {len(failed_places)} place(s)")
                place_coordinates = failed_places
                time.sleep(60)
            else:
                break
    # Log and return the weather data
    if len(failed_places) == 0:
        logger.info(f"Successfully got weather data at {expected_last_updated} for all places")
    else:
        logger.info(f"Successfully got data for {len(weather_data)} places at {expected_last_updated}, failed for {len(failed_places)} place(s) {list(failed_places.keys())}.")
    return weather_data



def insert_weather_data(weather_data=None):
    # Create a session for later use (if necessary)
    session = boto3.Session(
        aws_access_key_id = os.getenv("AWS_ACCESS_KEY"),
        aws_secret_access_key = os.getenv("AWS_SECRET_ACCESS_KEY"),
        region_name = os.getenv("REGION_NAME"))
    
    # Get values of environment variables for MongoDB
    connection_string = os.getenv("MONGO_CONNECTION_STRING")
    database_name = os.getenv("MONGO_DB_NAME")
    collection_name = os.getenv("MONGO_WEATHER_COLLECTION_NAME")
    
    # Connect to MongoDB
    client = pymongo.MongoClient(connection_string)
    try:
        client.server_info()
        logger.info("Successfully connected to MongoDB")
        collection = client[database_name][collection_name]
        # Insert data into MongoDB
        logger.info(f"Start inserting {len(weather_data)} documents")
        inserted_documents = weather_data
        try:
            collection.insert_many(inserted_documents, ordered=False)
            logger.info(f"Successfully inserted all weather data")
        except Exception as e:
            message = f"Failed to insert weather data. An unexpected error occurred: \"{e}\"."
            logger.error(f"{message} {format_traceback()}.")
            notify_error(session, message)
            save_to_s3(session, weather_data)
    except pymongo.errors.ServerSelectionTimeoutError as timeout_e:
        message = f"Failed to connect to MongoDB.\nError message: \"{timeout_e}\"."
        logger.error(f"{message} {format_traceback()}.")
        notify_error(session, message)
        save_to_s3(session, weather_data)
    except Exception as e:
        message = f"Something is wrong. Error message: \"{e}\"."
        logger.error(f"{message} {format_traceback()}.")
        notify_error(session, message)
        save_to_s3(session, weather_data)



def handler(event=None, context=None):
    # Load environment variables
    load_dotenv()

    # Assign expected_last_updated value for later use
    global expected_last_updated
    expected_last_updated = (datetime.utcnow() + timedelta(hours=7)).replace(minute=0, second=0, microsecond=0)

    # Load data for place_coordinates variable
    script_dir = os.path.dirname(os.path.abspath(__file__))
    all_files_and_directories = os.listdir(script_dir)
    directories = [name for name in all_files_and_directories if os.path.isdir(os.path.join(script_dir, name))]
    if "data" in directories:
        file_path = os.path.join(script_dir, 'data/place_coordinates.json')
    else:
        file_path = os.path.join(script_dir, '../data/place_coordinates.json')
    with open(file_path, 'r') as f:
        place_coordinates = json.load(f)

    # Set up logging
    global logger
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    
    # Call the functions
    weather_data = get_weather_data(place_coordinates)
    insert_weather_data(weather_data)