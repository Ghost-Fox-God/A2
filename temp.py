import boto3

# Create an S3 client
s3_client = boto3.client('s3')

# Specify the bucket name
bucket_name = 'b00925429cloudcomputingassignment2'

# List all objects in the bucket
response = s3_client.list_objects_v2(Bucket=bucket_name)

# Print file names
print("List of Files in S3 Bucket:")
for file in response['Contents']:
    print(file['Key'])
