'''
@Module: Producer module for AWS
         kinesis.
@Language: Python 3.7
@Author: Suryadeep(schatt37@asu.edu)
@Version: 1.0
@Author: Suryadeep(schatt37@asu.edu)
@Version: 1.2
@Author: Suryadeep(schatt37@asu.edu)
@Version: 1.4
@Author: Suryadeep(schatt37@asu.edu)
@Version: 1.8
'''
import json
import boto3
import logging
#from botocore.vendored import requests
import requests
import time

def lambda_handler(event, context):
    # Kinesis stream name
    my_stream_name = 'surya-python-trial-stream' # Replace the stream name here
    logging.basicConfig(level = logging.INFO)
    logger = logging.getLogger()
    # Import the kinesis client
    # module from boto3.
    kinesis_client = boto3.client('kinesis', region_name='us-west-2')
    
    # Function to fetch data
    # from the slack API.
    def request_from_slack():
        itr = 0
        # Data fectch status identifier
        fetch_status = {
            'status' : True,
            'message' : 'Data fetched successfully'
        }
        # Request data from
        # slack api.
        #URL = 'https://api.slack.com/audit/v1/logs?limit=1500&pretty=1'
        URL = 'https://api.slack.com/audit/v1/logs?pretty=1'
        HEADERS = {
            'Accept': 'application/json',
            'Authorization': '' # Add the slack auth token
        }
        r = requests.get(url = URL, headers = HEADERS)
        # Handle error for index range.
        try:
            # Get response in json
            payload = r.json()
            # Call the funcition to put 
            # data to Kinesis data stream.
            return put_to_stream(payload)

        except ValueError:
            # Form the error respone
            fetch_status['status'] = False
            fetch_status['message'] = "No data fetched from slack API"
            return fetch_status

    # Function to send some
    # data to kinesis stream.
    def put_to_stream(payload):
        record_count = 0
        # kinesis put status identifier.
        kinesis_put_status = {
            'status' : True,
            'message' : 'Data pushed to kinesis data stream',
            'records' : record_count
        }
        # Push the data payload to
        # the kinesis stream.
        while True:
            # Error checking for index
            # out of range error.
            try:
                # wait for 4 seconds
                time.sleep(4)
                # Post the record to
                # kinesis data stream.
                put_response = kinesis_client.put_record(
                    StreamName = my_stream_name,
                    Data = json.dumps(payload['entries'][record_count]),
                    PartitionKey = str(payload['entries'][record_count]['date_create']))
                print(payload['entries'][record_count])
                # Increment the record counter.
                record_count += 1
            except IndexError:
                kinesis_put_status['records'] = record_count
                return kinesis_put_status
        else:
            kinesis_put_status['status'] = False
            kinesis_put_status['message'] = 'Can not connect to Kinesis'
            kinesis_put_status['records'] = record_count
            return kinesis_put_status
    # Call the request module
    # to fetch slack API data.
    print(request_from_slack())

if __name__ == '__main__':
    lambda_handler('hello', 'new_cntext')