# This is sample code for lambda function collecting messages from SQS and placing output files into S3 as csv
import json
import os
import boto3
import arrow

s3 = boto3.resource('s3')
bucket_name = 'put your target S3 bucket name'

column1_name = "column1"
column2_name = "column2"
column3_name = "column3"


def main_handler(event, context):

    request_id = context.aws_request_id

    bucket = s3.Bucket(bucket_name)
    object_key_name = f"{request_id}.csv"

    obj = bucket.Object(object_key_name)

    write_contents_header = [column1_name,column2_name,column3_name]
    write_contents = []

    records = event["Records"]

    for content_rec in records:
        body_content = content_rec["body"]
        body_content_dict = json.loads(body_content)
        message = body_content_dict["Message"]
        message_dict = json.loads(message)
        value_col1 = message_dict[column1_name]
        value_col2 = message_dict[column2_name]
        value_col3 = message_dict[column3_name]

        write_contents.append[value_col1,value_col2,value_col3]

    upload_file = "/tmp/" + object_key_name
    
    with open(upload_file,"w", newline="", encoding="utf-8-sig") as f:
        csvfile = csv.writer(f)
        csvfile.writerow(write_contents_header)
        csvfile.writerows(write_contents)

    with open(upload_file,"rb") as uf:
        r = obj.put(Body = uf)

    os.remove(upload_file)

    return {"statusCode":200, "body": "Success"}

