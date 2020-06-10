# Slack-Kinesis

Fetch slack api data using audit api for slack and push it to S3 bucket using AWS kinesis

## Method-1
In this method I have used python's kinesis client library and created kinesis producer and consumer. The Producer code fetches real time slack data and pushes it to the Kinesis consumer. The consumer receives the incoming data and pushes it into S3 bucket. The python code will be running on an EC2 container.
### Step-1
Create a kinesis data stream.
### Step-2
Create a bucket to store the incoming data.
### Step-3
Run the producer and consumer code.

## Method-2
In this method I have used kinesis firehose as a consumer to our kinesis data stream. So, instead of the consumer python code, the firehose pushes the incoming data to S3 bucket.
### Step-1
Create a kinesis data stream.
### Step-2
Create a bucket to store the incoming data.
### Step-3
Create kinesis firehose and configure it's destination to be the S3 bucket created in the previous step.
