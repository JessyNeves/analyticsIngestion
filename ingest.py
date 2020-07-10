from os import path
import os
import sys
import pandas as pd
from google.cloud.bigquery.client import Client
from control import ingestControl, validateDate
from bigQuery import querying
from preprocessing import preprocess

"""Pipeline PARAMETERS"""
parameterFirstDate = input("Enter the first date for the query: ")
parameterLastDate = input("Enter the end date for the query:")
overwrite = False

"""Checks if input Date is valid"""
validateDate(parameterFirstDate)
validateDate(parameterLastDate)
print("Data between " + parameterFirstDate + " and " + parameterLastDate + " will be imported.")

"""Checks if data was already imported"""
if ingestControl(parameterFirstDate, parameterLastDate, overwrite):
    print("Date was not imported before. Program will proceed")
else:
    print("Exiting, there is an overlap between the desired dates and previsouly imported data. Set overwrite to true"
          " to bypas")
    sys.exit(0)

"""Explicitly SET credentials path as Environment Variable and create BigQuery Client"""
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'credentials.json'
bq_client = Client()

"""Fetch back the results from querying function"""
results = querying(bq_client, parameterFirstDate, parameterLastDate)

"""If querying functions fails, it returns a boolean False."""
if(isinstance(results, bool)):
    print("No new data was fetched, exiting.")
    sys.exit(0)

results = preprocess(results)
"""Concat fetched data to general"""
if not (path.exists("imported-data/sessions.parquet")):
    results.to_parquet("imported-data/sessions.parquet")
else:
    """"Concat new data"""
    pd.concat([results, pd.read_parquet("imported-data/sessions.parquet")]).to_parquet("imported-data/sessions.parquet")
