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
overwrite = False

"""Checks if input Date is valid"""
validateDate(parameterFirstDate)

"""Checks if data was already imported"""
if ingestControl(parameterFirstDate, overwrite):
    print("Date was not imported before. Program will proceed")
else:
    print("Exiting, there is an overlap between the desired dates and previsouly imported data. Set overwrite to true"
          " to bypas")
    sys.exit(0)

"""Explicitly SET credentials path as Environment Variable and create BigQuery Client"""
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'credentials.json'
bq_client = Client()

"""Fetch back the results from querying function"""
results = querying(bq_client, parameterFirstDate)

"""If querying functions fails, it returns a boolean False."""
if(isinstance(results, bool)):
    print("No new data was fetched, exiting.")
    sys.exit(0)

results = preprocess(results)

"""Load control table"""
controlTable = pd.read_csv("controlTable.csv")

"""Concat fetched data to general"""
if not (path.exists("sessions.parquet")):
    results.to_parquet("sessions.parquet")
    controlTable.loc[controlTable["ImportDate"] == int(parameterFirstDate), ["ParquetWritten"]] = "OK"
    controlTable.to_csv("controlTable.csv", index=False)
else:
    """"Concat new data"""
    pd.concat([results, pd.read_parquet("sessions.parquet")]).to_parquet("sessions.parquet")
    controlTable.loc[controlTable["ImportDate"] == int(parameterFirstDate), ["ParquetWritten"]] = "OK"
    controlTable.to_csv("controlTable.csv", index=False)
