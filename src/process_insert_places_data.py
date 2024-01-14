#!/usr/bin/env python3

import pymongo
from google.cloud import bigquery
import os
from dotenv import load_dotenv

# Set up credentials
load_dotenv()
dataset_id = os.getenv("BIGQUERY_DATASET_ID") 
table_id = os.getenv("BIGQUERY_PLACES_TABLE_ID")
key_file_name = os.getenv("GCP_SERVICE_ACCOUNT_KEY_FILE_NAME")
script_dir = os.path.dirname(os.path.abspath(__file__))
file_path = os.path.join(script_dir, f"../data/{key_file_name}")
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = file_path



# Define a function to get places data from MongoDB
def get_data():
    client = pymongo.MongoClient(os.getenv("MONGO_CONNECTION_STRING"))
    collection = client[os.getenv("MONGO_DB_NAME")][os.getenv("MONGO_PLACES_COLLECTION_NAME")]
    places_data = [{k:v for k, v in document.items() if k != "_id"} for document in collection.find({})]
    print("Successfully got places data")
    return places_data



# Define a function to check for duplicates before loading data to BigQuery
def check_duplicates(data):
    client = bigquery.Client()
    # Get existing place_ids from places table on BigQuery
    query_job = client.query(f"SELECT place_id FROM {dataset_id}.{table_id}")
    place_ids = [dict(row)["place_id"] for row in query_job]
    # Check for duplicates before loading
    unique_ids = set(place_ids)
    if len(unique_ids) == len(place_ids):
        unique_documents = []
        for document in data:
            place_id = document["place_id"]
            if place_id not in unique_ids:
                unique_ids.add(place_id)
                unique_documents.append(document)
            else:
                print(f"Duplicate value found: {place_id}. The document will not be loaded to BigQuery.")
        return unique_documents
    else:
        duplicate_ids = [id for id in unique_ids if place_ids.count(id) > 1]
        print(f"Duplicate values found in existing data of {dataset_id}.{table_id} table.\nDuplicate values: {duplicate_ids}")



def generate_insert_statement(rows):
    # Check if the unique_documents is empty
    if not rows_to_insert:
        print("No new rows to insert")
        return
    # Get the column names from the keys of the first row
    columns = ', '.join(rows[0].keys())
    # Generate a list of value strings
    values = []
    for row in rows:
        value_string = ', '.join(f"'{row[column]}'" for column in list(rows[0].keys()))
        values.append(f"    ({value_string})\n")

    # Join all value strings into a single string
    values_string = ', '.join(values)

    # Generate the final INSERT statement
    insert_statement = f"INSERT INTO `{dataset_id}.{table_id}` ({columns})\nVALUES\n{values_string};"

    return insert_statement



# Define a function to handle inserting data to BigQuery
def insert_data(insert_statement):
    bigquery_client = bigquery.Client()
    # Insert rows into BigQuery
    insert_job = bigquery_client.query(insert_statement)
    insert_job.result()
    print(f"Successfullt inserted {insert_job.num_dml_affected_rows} rows into '{dataset_id}.{table_id}' table")



if __name__ == "__main__":
    places_data = get_data()
    rows_to_insert = check_duplicates(places_data)
    insert_statement = generate_insert_statement(rows_to_insert)
    insert_data(insert_statement)