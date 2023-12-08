import boto3
import os
import tempfile
from rococo.messaging import SqsConnection

# Specify the AWS region you want to use
region = 'us-east-1'  # Replace with your desired region


AWS_S3_BUCKET_NAME: str = "er-sir"

# Create an AWS session with the specified region
session = boto3.Session(region_name=region)

################
# #### S3 #######
################

print("\n\n##### S3 #######\n\n")

# Use Boto to interact with AWS services, e.g., S3 and SQS
s3 = session.client('s3')
# print(s3)

with tempfile.NamedTemporaryFile(delete=False, mode='w') as temp_file:
    temp_file.write("This is the content of the AWS ad-hoc file - v2.")
    temp_file_path = temp_file.name

s3_file_key = 'test/adhoc_file.txt'

s3.upload_file(temp_file_path, AWS_S3_BUCKET_NAME, s3_file_key)

# Get the directory of the current script
current_script_directory = os.path.dirname(os.path.abspath(__file__))
print("current_script_directory", current_script_directory)

s3.download_file(AWS_S3_BUCKET_NAME, s3_file_key, os.path.join(current_script_directory,
                                                               'demo_data', 'file_from_aws.txt'))

################
# ##### SQS #####
################

print("\n\n##### SQS #######\n\n")

sqs = session.client('sqs')
print(sqs)
QUEUE_NAME: str = 'queue-er-sir'

# Create the SQS queue
create_queue_response = sqs.create_queue(QueueName=QUEUE_NAME)

# https://us-east-1.console.aws.amazon.com/sqs/v3/home?region=us-east-1#/queues

# Print the queue URL and other information
print(f"create_queue_response: {create_queue_response}")
print(f"Queue URL: {create_queue_response['QueueUrl']}")

# Producer


def process_message(message_data: dict):
    print(f"Processing message {message_data}...")


# with SqsConnection(region_name='us-east-1') as conn:
with SqsConnection(region_name=region) as conn:
    conn.send_message(QUEUE_NAME, {'message1': 'data12'})
    conn.consume_messages(QUEUE_NAME, process_message)


# accessible from the console:
# https://us-east-1.console.aws.amazon.com/sqs/v3/home?region=us-east-1#/queues/https%3A%2F%2Fsqs.us-east-1.amazonaws.com%2F266062292523%2Fqueue-er-sir/send-receive

# # Note: since cleanup is not required for SQS connections, you can also do:
# conn = SqsConnection(region_name='us-east-1')
# conn.send_message(QUEUE_NAME, {'message': 'data'})
# conn.consume_messages(QUEUE_NAME, process_message)
