import base64
from apiclient.discovery import build
from httplib2 import Http
from oauth2client import file, client, tools
import pandas as pd
import json
from google.cloud import pubsub_v1,bigquery
import time

def gsheet2df(gsheet):
    """ Converts Google sheet data to a Pandas DataFrame.  
    Note: This script assumes that your data contains a header file on the first row!
    Also note that the Google API returns 'none' from empty cells - in order for the code
    below to work, you'll need to make sure your sheet doesn't contain empty cells,
    or update the code to account for such instances. 
    """
    header = gsheet.get('values', [])[0]   # Assumes first line is header!
    values = gsheet.get('values', [])[1:]  # Everything else is data.
    if not values:
        print('No data found.')
    else:
        all_data = []
        for col_id, col_name in enumerate(header):
            column_data = []
            for row in values:
                column_data.append(row[col_id])
            ds = pd.Series(data=column_data, name=col_name)
            all_data.append(ds)
        df = pd.concat(all_data, axis=1)
        return df

def hello_pubsub(event, context):
    """Triggered from a message on a Cloud Pub/Sub topic.
    Args:
         event (dict): Event payload.
         context (google.cloud.functions.Context): Metadata for the event.
    """
    SPREADSHEET_ID = '1v0IPZ3C72br5kFdB0SY_xQZHGM80NnNLO_qesPOYSKo'
    RANGE_NAME = 'Sheet1'
    project_id = 'nc-gtrends'
    topic_id = 'trends_parameters'

    client = bigquery.Client(project = project_id)
    service = build('sheets', 'v4')
    gsheet = service.spreadsheets().values().get(spreadsheetId=SPREADSHEET_ID, range=RANGE_NAME).execute()
    df = gsheet2df(gsheet)
    
    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(project_id, topic_id)
    
    ds_list = list([x.dataset_id for x in client.list_datasets()])
    for i in df.to_dict(orient='records'):
        if i.get("dataset") not in ds_list:
            dataset = client.create_dataset(project_id + "."  + i.get("dataset"))  # Make an API request.
            print("Created dataset {}.{}".format(client.project, dataset.dataset_id))
            time.sleep(5)
            ds_list = list([x.dataset_id for x in client.list_datasets()])
        print(str(i))
        print(json.dumps(i))
        publisher.publish(topic_path, json.dumps(i).encode("utf-8"))
        
    
