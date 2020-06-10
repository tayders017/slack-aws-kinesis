'''
@Module: Consumer module for AWS
         kinesis.
@Language: Python 3.7
@Author: Suryadeep(schatt37@asu.edu)
@Version: 1.0
@Author: Suryadeep(schatt37@asu.edu)
@Version: 1.2
@Author: Suryadeep(schatt37@asu.edu)
@Version: 1.4
'''
import json
import boto3
import logging
import time
def lambda_handler(event, context):
    logging.basicConfig(level = logging.INFO)
    logger = logging.getLogger()
    # Often connection errors occur
    # while importing from boto3
    try:
    	s3 = boto3.client('s3')
    except Exception as import_err:
    	print('Exception S3 client import', import_err)

    # Function to perform data fetch
    # action from the kinesis stream.
    def fetch_kinesis_data():
    	# Kinesis stream name from
    	# where we have to consume
    	# streaming data.
    	my_stream_name = 'surya-python-trial-stream' # Replace the data stream name here.
    	# Import the kinesis client
    	# module from boto3.
    	kinesis_client = boto3.client('kinesis', region_name = 'us-west-2')
    	# Get details about the kinesis
    	# stream name.
    	response = kinesis_client.describe_stream(StreamName = my_stream_name)
    	# Get shard Id from kinesis
    	# description.
    	my_shard_id = response['StreamDescription']['Shards'][0]['ShardId']
    	response = kinesis_client.describe_stream(StreamName = my_stream_name)
    	# Get shard iterator of shard
    	# id.
    	shard_iterator = kinesis_client.get_shard_iterator(StreamName = my_stream_name,
    										ShardId = my_shard_id,
    										ShardIteratorType = 'LATEST')

    	my_shard_iterator = shard_iterator['ShardIterator']
    	# Get records from the shard
    	# iterator which is pushed by
    	# the producer.
    	record_response = kinesis_client.get_records(ShardIterator = my_shard_iterator, Limit = 2)
    	# Get the records from shard
    	# and push it to s3 bucket.
    	while 'NextShardIterator' in record_response:
    		record_response = kinesis_client.get_records(ShardIterator=record_response['NextShardIterator'], Limit=10)
    		# Call the push to s3 bucket
    		# module if data is available.
    		if len(record_response['Records']) != 0:
    			push_to_s3(record_response)
    		# wait for 2 seconds
    		time.sleep(2)

    # Function to push the stream
    # data to landing s3 bucket.
    def push_to_s3(data_dumps):
    	# Status message of data push.
    	push_message = {
    		'status' : True,
    		'message' : 'Data pushed to bucket'
    	}
    	# Look for exceptions while
    	# pushing data to bucket.
    	try:
    		# Put the data object to
    		# s3 bucket.
    		print(data_dumps)
    		print('---------')
            # Add the bucket name here.
    		s3.put_object(Bucket = 'surya-boto3-trial', Key = data_dumps['ResponseMetadata']['HTTPHeaders']['date'], Body = data_dumps['Records'][0]['Data'])
    		print(push_message)
    	except Exception as put_bucket_err:
    		print('Exception S3 put to bucket', put_bucket_err)
    # Initiate the function
    fetch_kinesis_data()
if __name__ == '__main__':
	lambda_handler('event', 'context')