import boto3
import os
import botocore
import os.path
import pandas


def create_key_pair():
    ec2_client = boto3.client("ec2", region_name="us-west-2")
    key_pair = ec2_client.create_key_pair(KeyName="ec2-key-pair")
    private_key = key_pair["KeyMaterial"]
    with os.fdopen(
        os.open("aws_ec2_key.pem", os.O_WRONLY | os.O_CREAT, 0o400), "w+"
    ) as handle:
        handle.write(private_key)

create_key_pair()

def get_public_ip(instance_id):
    ec2_client = boto3.client("ec2", region_name="us-west-2")

    reservations = ec2_client.describe_instances(InstanceIds=[instance_id]).get(
        "Reservations"
    )

    for reservation in reservations:
        for instance in reservation["Instances"]:
            print(instance.get("PublicIpAddress"))

get_public_ip("i-071e9df0f4f39e2f1")

def get_running_instances():
    ec2_client = boto3.client("ec2", region_name="us-west-2")
    reservations = ec2_client.describe_instances(
        Filters=[
            {
                "Name": "instance-state-name",
                "Values": ["running"],
            },
            {"Name": "instance-type", "Values": ["t2.micro"]},
        ]
    ).get("Reservations")

    for reservation in reservations:
        for instance in reservation["Instances"]:
            instance_id = instance["InstanceId"]
            instance_type = instance["InstanceType"]
            public_ip = instance["PublicIpAddress"]
            private_ip = instance["PrivateIpAddress"]
            print(f"{instance_id}, {instance_type}, {public_ip}, {private_ip}")

get_running_instances()

def get_instance_info(instance_id):
    ec2_client = boto3.client("ec2", region_name="us-west-2")
    response = ec2_client.describe_instance_status(InstanceIds=[instance_id])
    print(response)

get_instance_info("i-071e9df0f4f39e2f1")

def stop_instance(instance_id):
    ec2_client = boto3.client("ec2", region_name="us-west-2")
    response = ec2_client.stop_instances(InstanceIds=[instance_id])
    print(response)

stop_instance("i-071e9df0f4f39e2f1")

def start_instance(instance_id):
    ec2_client = boto3.client("ec2", region_name="us-west-2")
    response = ec2_client.start_instances(InstanceIds=[instance_id])
    print(response)

start_instance("i-071e9df0f4f39e2f1")

def create_bucket(bucket_name, region):
    try:
        s3_client = boto3.client("s3", region_name=region)
        location = {"LocationConstraint": region}
    except ValueError:
        print("Error!!!")
        return
    try:
        response = s3_client.create_bucket(
            Bucket=bucket_name, CreateBucketConfiguration=location
        )
        print(response)
    except botocore.exceptions.ClientError:
        print("Error, such backet is already exists")
        return
    except botocore.exceptions.ParamValidationError:
        print(
            "Error, invalid name. Bucket name must contain only letters, numbers and '-'"
        )
        return

create_bucket("awixess-bucket-lab4", "us-west-2")

def bucket_exists(bucket_name):
    s3_client = boto3.resource("s3")
    if s3_client.Bucket(bucket_name) not in s3_client.buckets.all():
        return False
    return True

def bucket_element_exists(bucket_name, s3_obj_name):
    s3_client = boto3.client("s3")
    try:
        s3_client.get_object(Bucket=bucket_name, Key=s3_obj_name)
    except:
        return False
    return True

print("Write bucket name:")
bucket = str(input())
bucket_exists(bucket)

print("Write file name:")
filename = str(input())
bucket_element_exists(bucket, filename)

def buckets_list():
    s3 = boto3.client("s3")
    response = s3.list_buckets()
    print("Existing buckets:")
    for bucket in response["Buckets"]:
        print(f' {bucket["Name"]}')

buckets_list()

def upload(file_name, bucket_name, s3_obj_name):
    s3_client = boto3.client("s3")
    if not bucket_exists(bucket_name):
        print(f"Error. No such bucket {bucket_name}")
        return
    if not os.path.exists(file_name):
        print(f"{file_name}:: such file does not exists")
        return
    try:
        s3_client.head_object(Bucket=bucket_name, Key=s3_obj_name)
    except:
        response = s3_client.upload_file(
            Filename=file_name, Bucket=bucket_name, Key=s3_obj_name
        )
        print(response)
        return
    print(f"{s3_obj_name} is already exists on {bucket_name}")

upload("lab4.py", "awixess-bucket-lab4", "lab4.py")

def read_csv_from_bucket(bucket_name, s3_obj_name):
    s3_client = boto3.client("s3")
    if not bucket_exists(bucket_name):
        print(f"Error. No such bucket {bucket_name}")
        return
    if not bucket_element_exists(bucket_name, s3_obj_name):
        print(f"Error. No such file {s3_obj_name}")
        return
    obj = s3_client.get_object(Bucket=bucket_name, Key=s3_obj_name)
    data = pandas.read_csv(obj["Body"])
    print("Printing the data frame...")
    print(data.head())

read_csv_from_bucket("awixess-bucket-lab2", "data.csv")

def destroy_bucket(bucket_name):
    s3_client = boto3.client("s3")
    response = s3_client.delete_bucket(Bucket=bucket_name)
    print(response)

destroy_bucket("awixess-bucket-lab4")
