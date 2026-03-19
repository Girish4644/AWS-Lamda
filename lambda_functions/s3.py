import boto3
from dotenv import load_dotenv
import os
load_dotenv()
import csv
import io

s3 = boto3.client('s3',
                  region_name='us-east-2',
                  aws_access_key_id=os.getenv('aws_access_key_id'),
                  aws_secret_access_key=os.getenv('aws_secret_access_key')
                  )
def move_s3_object(bucket, source_key, target_prefix):
    file_name = source_key.split("/")[-1]
    target_key = f"{target_prefix}/{file_name}"

    s3.copy_object(
        Bucket=bucket,
        CopySource={"Bucket": bucket, "Key": source_key},
        Key=target_key
    )

    s3.delete_object(Bucket=bucket, Key=source_key)
    return target_key

bucket_name = "gbuckety"
files = ["raw/data_1.csv", "raw/data_2.csv"]

for file_key in files:
    response = s3.get_object(Bucket=bucket_name, Key=file_key)
    content = response["Body"].read().decode("utf-8-sig")

    reader = csv.DictReader(io.StringIO(content))


    if "OrderID" not in reader.fieldnames:
        new_key = move_s3_object(bucket_name, file_key, "invalid")
        print(file_key, "-> OrderID column missing, moved to", new_key)


    else:
        row_count = sum(1 for _ in reader)

        if row_count > 2:
            new_key =move_s3_object(bucket_name, file_key, "processed")
            print(file_key, "-> Moved to processed as", new_key)
        else:
            new_key = move_s3_object(bucket_name, file_key, "invalid")
            print(file_key, "-> Moved to invalid as", new_key)


import json
import os
import csv
import io

def lambda_handler(event, context):

    def move_s3_object(bucket, source_key, target_prefix):
        file_name = source_key.split("/")[-1]
        target_key = f"{target_prefix}/{file_name}"

        s3.copy_object(
            Bucket=bucket,
            CopySource={"Bucket": bucket, "Key": source_key},
            Key=target_key
        )

        s3.delete_object(Bucket=bucket, Key=source_key)
        return target_key

    bucket_name = "gbuckety"
    files = ["raw/data_1.csv", "raw/data_2.csv"]

    for file_key in files:
        response = s3.get_object(Bucket=bucket_name, Key=file_key)
        content = response["Body"].read().decode("utf-8-sig")

        reader = csv.DictReader(io.StringIO(content))


        if "OrderID" not in reader.fieldnames:
            new_key = move_s3_object(bucket_name, file_key, "invalid")
            print(file_key, "-> OrderID column missing, moved to", new_key)


        else:
            row_count = sum(1 for _ in reader)

            if row_count > 2:
                new_key = move_s3_object(bucket_name, file_key, "processed")
                print(file_key, "-> Moved to processed as", new_key)
            else:
                new_key = move_s3_object(bucket_name, file_key, "invalid")
                print(file_key, "-> Moved to invalid as", new_key)


    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }
