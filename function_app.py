import logging
import azure.functions as func
from ringcentral import SDK
from datetime import datetime, timedelta
import pytz
import os,sys,json
from pandas import json_normalize
import pandas as pd
from azure.storage.blob import BlobServiceClient
from io import StringIO


def initialize_blob_client():
   connection_string = os.getenv("BlobConnectionString")
   blob_service_client = BlobServiceClient.from_connection_string(connection_string)
   return blob_service_client

def append_data_to_blob(df):
   blob_service_client = initialize_blob_client()
   container_name = "rcdata"
   blob_name = "RCData.csv"

   blob_client= blob_service_client.get_blob_client(container=container_name, blob=blob_name)
   
   try: 
      blob_data=blob_client.download_blob().readall()
      existing_df= pd.read_csv(StringIO(blob_data.decode('utf-8')))
      print("Successfully read container!")
   except Exception as e:
      logging.error(f"Error reading from blob: {e}")
      existing_df = pd.DataFrame()

   combined_df = pd.concat([existing_df, df], ignore_index=True)

   csv_data= combined_df.to_csv(index=False)

   try: 
      blob_client.upload_blob(csv_data, blob_type="BlockBlob", overwrite=True)
      print("Data successfully appended and uploaded to the blob.")
   except Exception as e:
      logging.error(f"Error writing to blob: {e}")

app = func.FunctionApp()


@app.schedule(schedule="0 * * * *", arg_name="myTimer", run_on_startup=True,
              use_monitor=False) 
def timer1hr(myTimer: func.TimerRequest) -> None:
    if myTimer.past_due:
        logging.info('The timer is past due!')

    logging.info('Python timer trigger function executed.')

    def read_analytics_aggregate_data():
      try:
         time_zone= pytz.timezone("America/Denver")
         time_to= datetime.now(time_zone)
         time_from= time_to - timedelta(hours=1)
         time_from_iso=time_from.isoformat()
         time_to_iso= time_to.isoformat()

         bodyParams = {
      "grouping": {
         "groupByMembers": "Queue",
         "keys": [
            "991668048"
         ]
      },
      "timeSettings": {
         "timeRange" : {
            "timeFrom": time_from_iso,
            "timeTo": time_to_iso
         },
         "timeZone": "America/Los_Angeles"
      },
      "responseOptions": {
         "counters": {
            "allCalls": {
               "aggregationType": "Sum"
            },
            "callsByDirection": {
               "aggregationType": "Sum"
            },
            "callsByResponse": {
               "aggregationType": "Sum"
            },
            "callsByType": {
               "aggregationType": "Sum"
            }
         }
      }
    }
         queryParams = {
            'perPage': 200
         }

         endpoint = '/analytics/calls/v1/accounts/~/aggregation/fetch'
         resp = platform.post(endpoint, bodyParams, queryParams)
         jsonObj = resp.json_dict()
         print(json.dumps(jsonObj, indent=2, sort_keys=True))
         return jsonObj
      except Exception as err:
         sys.exit("Unable to read analytics aggregation ", err)
    
    rcsdk = SDK(os.getenv('RC_CLIENT_ID'),
                os.getenv('RC_CLIENT_SECRET'),
                os.getenv('RC_SERVER_URL') )
    platform = rcsdk.platform()

    def login():
       try:
          platform.login( jwt=os.getenv('RC_JWT') )
          timeline_data = read_analytics_aggregate_data()
          return timeline_data
       except Exception as e:
          sys.exit("Unable to authenticate to platform. Check credentials." + str(e))

    json_data = login()

    df = pd.json_normalize(json_data["data"],
                                 sep="_",
                                 record_path="records")
    
    current_time=datetime.now()
    df["Timestamp"] = current_time

    append_data_to_blob(df)

    logging.info("Timer Function ran and completed")