import json
import boto3
import os
from datetime import datetime
from botocore.exceptions import ClientError
from urllib.request import Request, urlopen
from urllib.error import URLError, HTTPError

# This account s3 client
s3 = boto3.client('s3')

# Main account sts client
sts = boto3.client('sts')

# SSM client
ssm = boto3.client('ssm')

# Main account s3 client (assumed role)
main_account = sts.assume_role(
    RoleArn="arn:aws:iam::<source account>:role/<source account role>",
    RoleSessionName="cross_acct_lambda"
)
credentials = main_account['Credentials']
s3_main = boto3.client(
    's3',
    aws_access_key_id=credentials['AccessKeyId'],
    aws_secret_access_key=credentials['SecretAccessKey'],
    aws_session_token=credentials['SessionToken'],
)

# The SSM Parameter name used in the get_parameter call that is stored in the SSM_PARAMETER environment variable
SSM_PARAMETER = os.environ['SSM_PARAMETER']
# Call parameter store and return the SSM_PARAMETER variable name result with decryption
SECRET = ssm.get_parameter(Name=SSM_PARAMETER, WithDecryption=True)
# The output from above's parameter value set as the HOOK_URL
HOOK_URL = ("{}".format(SECRET['Parameter']['Value']))

# The Slack channel to send error messages to stored in the slackChannel environment variable
SLACK_CHANNEL = os.environ['slackChannel']
LAMBDA = os.environ['AWS_LAMBDA_FUNCTION_NAME']

# Get current account's canonical_id
CANONICAL_ID = s3.list_buckets()['Owner']['ID']

# Runtime Region
REGION = os.environ['AWS_REGION']
#Lambda URL
LAMBDA_URL = "<https://" + REGION + ".console.aws.amazon.com/lambda/home?region=" + REGION + "#/functions/" + LAMBDA + "|AWS Console Link>"

# For a PutObject API Event, get the bucket and key name from the event
# if the object acl does not have full permissions, grant the object full permissions
# by assuming into the object owner's account/role and using the PutObjectAcl call.

def lambda_handler(event, context):
    # Get bucket name from the event
    bucket = event['detail']['requestParameters']['bucketName']
    
    # Get key name from the event
    key = event['detail']['requestParameters']['key']
    
    # Print full object path
    print("New Upload: " + bucket + "/" + key)
   
    # If object ACL returns "AccessDenied" then add full ACL permissions to object (update_acl function)
    try:
        # Get the object ACL from S3
        acl = s3.get_object_acl(Bucket=bucket, Key=key)
        # Retrieve owner_id and grantee_id to compare them to canonical_id later
        owner_id = acl['Owner']['ID']
        grantee_id = acl['Grants'][0]['Grantee']['ID']
        # Compare the owner_id or Grantee_id to this account's canonical_id (One of them should match)
        # If they match, exit because object has correct permissions
        if (owner_id or grantee_id == CANONICAL_ID):
            print("S3 Object: " + bucket + "/" + key + " has the correct ACL permissions, skipping.")
            return
        else:
            print("Unexpected error occurred: %s" % err)
            slack_send(bucket, key, err)
            return
    except ClientError as err:
        # We should expect an "AccessDenied" error if this account doesnt have full permissions on the s3 object
        if err.response['Error']['Code'] == 'AccessDenied':
            print("S3 Object: " + bucket + "/" + key + " does not have the correct ACL permissions, updating now...")
            update_acl(bucket, key)
            return
        # If an object gets uploaded then deleted, exit (response should be "NoSuchKey")
        if err.response['Error']['Code'] == 'NoSuchKey':
            print("Cannot retrieve ACL permissions. Uploaded object no longer exists, exiting")
            return
        else:
            print("Unexpected error occurred: %s" % err)
            slack_send(bucket, key, err)
            return

# Grants an object with given bucket and key full acl permissions.
def update_acl(bucket, key):
    global credentials
    global main_account
    global s3_main
    
    # STS credential expiration
    time_left = int(
            credentials['Expiration'].timestamp() - datetime.now().timestamp()
        )
    print('Assumed Role Timeleft: {}'.format(time_left))
    # If credential expiration is less than 5 minutes, refresh
    if time_left < 300:
        main_account = sts.assume_role(
            RoleArn="arn:aws:iam::<source account>:role/<source account role>",
            RoleSessionName="cross_acct_lambda"
        )
        credentials = main_account['Credentials']
        print("Assumed Role Credentials Renewed. New Time: {}".format(
            credentials['Expiration']
        ))
        s3_main = boto3.client(
            's3',
            aws_access_key_id=credentials['AccessKeyId'],
            aws_secret_access_key=credentials['SecretAccessKey'],
            aws_session_token=credentials['SessionToken']
        )
    try:
        # Assume role into the object owner's account (Main) and update the object acl with given bucket and key 
        s3_main.put_object_acl(Bucket=bucket, Key=key, ACL="bucket-owner-full-control")
    except ClientError as err:
        # If an object gets uploaded then deleted at this point, exit and don't send slack message
        if err.response['Error']['Code'] == 'NoSuchKey':
            print("Cannot update ACL. Uploaded object no longer exists, exiting")
            return
        else:
            print("Unexpected error occurred: %s" % err)
            slack_send(bucket, key, err)
            return
    print("S3 Object:" + bucket + "/" + key + " has been updated with correct permissions.")

def slack_send(bucket, key, err):
    slack_message = {
        'channel': SLACK_CHANNEL,
        'text': "*Lambda Name:* *`%s`*\n*Lambda Link:* *%s*\n*S3 Object:* *`%s/%s`*\n*Error:* *`%s`*" % (LAMBDA,LAMBDA_URL,bucket,key,err)
        }
    req = Request(HOOK_URL, json.dumps(slack_message).encode('utf-8'))
    try:
        response = urlopen(req)
        response.read()
        print("Error Message Posted To Slack Channel", slack_message['channel'])
    except HTTPError as e:
        print("Request Failed: %d %s", e.code, e.reason)
    except URLError as e:
        print("Server Connection Failed: %s", e.reason)
    return
