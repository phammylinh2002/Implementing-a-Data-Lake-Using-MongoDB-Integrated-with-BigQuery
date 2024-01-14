#!/usr/bin/env python3

import pymongo
from google.cloud import bigquery
from bson.objectid import ObjectId
import os
import logging
import traceback
from datetime import timedelta, datetime



# Set up BigQuery client
dataset_id = os.getenv("BIGQUERY_DATASET_ID")
places_table_id = os.getenv("BIGQUERY_PLACES_TABLE_ID")
weather_table_id = os.getenv("BIGQUERY_WEATHER_TABLE_ID")
key_file_name = os.getenv("GCP_SERVICE_ACCOUNT_KEY_FILE_NAME")
script_dir = os.path.dirname(os.path.abspath(__file__))
file_path = os.path.join(script_dir, f"../data/{key_file_name}")
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = file_path
bigquery_client = bigquery.Client()

# Set up MongoDB client
mongo_client = pymongo.MongoClient(os.getenv('MONGO_CONNECTION_STRING'))
db = mongo_client[os.getenv('MONGO_DB_NAME')]
collection = db[os.getenv('MONGO_WEATHER_COLLECTION_NAME')]

# Set up logging
# Create a custom Formatter class inheriting from logging.Formatter to get the GMT+7 timestamp
class GMTPlus7Formatter(logging.Formatter):
    def formatTime(self, record, datefmt=None):
        dt = datetime.utcfromtimestamp(record.created) + timedelta(hours=7)
        t = dt.strftime(self.default_time_format)
        return t
# Create a handler and set its formatter
handler = logging.FileHandler('../log/process_insert_update_weather_data.log', mode='a')
handler.setFormatter(GMTPlus7Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
# Add the handler to the root logger
logging.getLogger().addHandler(handler)
logging.getLogger().setLevel(logging.INFO)



def format_traceback():
    return traceback.format_exc().replace('\n', ' | ')



def check_row_existence(document_id):
    query = f"""
    SELECT 1 
    FROM `{dataset_id}.{weather_table_id}` 
    WHERE id = '{document_id}'
    """
    query_job = bigquery_client.query(query)
    rows = list(query_job.result())
    if len(rows) == 0:
        logging.info(f"Row '{document_id}' DOES NOT EXIST in '{dataset_id}.{weather_table_id}' table")
        return False
    else:
        logging.info(f"Row '{document_id}' EXISTS in '{dataset_id}.{weather_table_id}' table")
        return True



def check_foreign_key(place_id, document_id):
    query = f"""
    SELECT 1 
    FROM `{dataset_id}.{places_table_id}` 
    WHERE place_id = '{place_id}'
    """
    query_job = bigquery_client.query(query)
    rows = list(query_job.result())
    if len(rows) != 0:
        logging.info(f"Place ID '{place_id}' of document '{document_id}' is VALID")
        return True
    else:
        logging.info(f"Place ID '{place_id}' of document '{document_id}' is INVALID")
        return False



def process_document(document):
    document["id"] = str(document["_id"])
    del document["_id"]
    del document["location"]
    for key in document["current"]:
        document[key] = document["current"][key]
    del document["current"]
    document["condition"] = document["condition"]["text"]
    document["is_day"] = bool(document["is_day"])
    logging.info(f"Processed document '{document['id']}'")
    return document



def insert_row(document, skip_existence_check=False):
    # Check if the document already exists in BigQuery
    if skip_existence_check == False:
        if check_row_existence(str(document["_id"])) == True:
            logging.info(f"Skipped document '{str(document['_id'])}'")
            return
    # Check if the place_id of the document is valid
    if check_foreign_key(document["place_id"], str(document["_id"])) == False:
        logging.info(f"Skipped document '{str(document['_id'])}'")
        return
    # Process the document
    processed_document = process_document(document)
    # Insert the processed document into BigQuery
    columns = ', '.join(processed_document.keys())
    values = ', '.join(
        f"TIMESTAMP '{value}:00'" if key == 'last_updated' else 
        f"'{value}'" if isinstance(value, str) else
        f"{str(value).lower()}" if isinstance(value, bool) else
        f"{value}" for key, value in processed_document.items()
    )
    insert_statement = f"""
    INSERT INTO `{dataset_id}.{weather_table_id}` ({columns})
    VALUES ({values})
    """
    logging.info(f"Start inserting row '{processed_document['id']}' into BigQuery")
    try:
        insert_job = bigquery_client.query(insert_statement)
        insert_job.result()
        logging.info(f"Successfully inserted row '{processed_document['id']}'")
    except Exception as e:
        logging.warning(f"Failed to insert row '{processed_document['id']}'. {format_traceback()}.")



def update_row(document_id):
    # Check if the document already exists in BigQuery
    if check_row_existence(document_id) == False:
        document = collection.find_one(document_id)
        insert_row(document, skip_existence_check=True)
        return
    # Get the document from MongoDB
    document = collection.find_one({"_id": ObjectId(document_id)})
    # Check if the place_id of the document is valid
    if check_foreign_key(document["place_id"], str(document["_id"])) == False:
        logging.info(f"Skipped document '{str(document['_id'])}'")
        return
    # Process the document
    processed_document = process_document(document)
    # Update the document in BigQuery
    set_clause = ', '.join(
        f"{key} = TIMESTAMP('{value}:00')" if key == 'last_updated' else 
        f"{key} = '{value}'" if isinstance(value, str) else 
        f"{key} = {value}" for key, value in processed_document.items()
    )
    update_statement = f"""
    UPDATE `{dataset_id}.{weather_table_id}`
    SET {set_clause}
    WHERE id = '{document_id}'
    """
    logging.info(f"Start updating row '{document_id}'")
    try:
        update_job = bigquery_client.query(update_statement)
        update_job.result()
        logging.info(f"Successfully updated row '{document_id}'")
    except Exception as e:
        logging.warning(f"Failed to update row '{document_id}'. {format_traceback()}.")



# Watch the MongoDB collection for changes and call suitable functions
unexpected_operation_message = ""
with collection.watch() as stream:
    logging.info("Watching for changes...")
    for change in stream:
        operation_type = change["operationType"]
        coll_id = "'" + change["ns"]["db"] + "." + change["ns"]["coll"] + "'"
        if operation_type == "insert":
            full_document = change['fullDocument']
            logging.info(f"Document '{str(full_document['_id'])}' was INSERTED into {coll_id}")
            insert_row(full_document)
        elif operation_type == "update":
            document_key = str(change['documentKey']['_id'])
            logging.info(f"Document '{document_key}' was UPDATED in {coll_id}")
            update_row(document_key)
        elif operation_type == "delete":
            document_key = str(change['documentKey']['_id'])
            logging.info(f"Document '{document_key}' was DELETED from {coll_id}")
        else:
            unexpected_operation_message = f"An unexpected operation was performed (operationType:{operation_type}). Change details: {change}"
            logging.error(unexpected_operation_message)
            os._exit(1)