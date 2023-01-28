#!/usr/bin/python3
import boto3

# Connect to S3
s3 = boto3.client('s3')

# Specify the S3 bucket and file name to search for
bucket_name = 'bucket-name'
file_name = 'file-name'

# Search for the file in the bucket
result = s3.list_objects_v2(Bucket=bucket_name, Prefix=file_name)

# Check if the file was found in the bucket
if 'Contents' in result:
    # Get the file's key
    key = result['Contents'][0]['Key']
    # Download the file
    s3.download_file(bucket_name, key, file_name)
    print(f'File {file_name} was found, saving to the local directory.')
else:
    print(f'File {file_name} was not found in the {bucket_name} bucket.')
