import json
import boto3
import os
from urllib.parse import unquote_plus

s3 = boto3.client('s3')
sns = boto3.client('sns')
ec2 = boto3.client('ec2')

TARGET_BUCKET = os.environ['TARGET_BUCKET']
SNS_TOPIC_ARN = os.environ['SNS_TOPIC_ARN']

def lambda_handler(event, context):
    source_bucket = event['Records'][0]['s3']['bucket']['name']
    file_key = unquote_plus(event['Records'][0]['s3']['object']['key'])
    output_s3 = 'magnasoft-ai-ml'
    print("Creating EC2 instance...")
    try:
        # Launch EC2 instance with user data to process the CSV
        instance_response = ec2.run_instances(
            ImageId='ami-0866a3c8686eaeeba',  # Replace with Amazon Linux or Ubuntu AMI
            InstanceType='t2.micro',
            MinCount=1,
            MaxCount=1,
            IamInstanceProfile={'Name': 'forlambda_s3-ec2'},  # Replace with correct IAM role
            KeyName='key',  # Replace with your EC2 key pair name
            UserData=f"""#!/bin/bash
                echo "Starting user data script"

                # Update package list and install virtual environment
                apt update -y
                apt install -y python3-venv
                
                # Create a virtual environment for AWS CLI
                python3 -m venv /home/ubuntu/awscli-env
                source /home/ubuntu/awscli-env/bin/activate

                # Install AWS CLI in the virtual environment
                /home/ubuntu/awscli-env/bin/pip install awscli boto3
                
                # Set AWS region
                export AWS_DEFAULT_REGION="us-east-1"  # Change to your desired region

                # Check if AWS CLI was installed successfully
                if /home/ubuntu/awscli-env/bin/aws --version; then
                    echo "AWS CLI installed successfully."
                else
                    echo "Failed to install AWS CLI." >&2
                    exit 1
                fi

                # Define variables
                bucket='{source_bucket}'
                script_key='{file_key}'
                output_key='{output_s3}'

                # Copy CSV file from S3
                echo "Copying CSV file from S3..."
                /home/ubuntu/awscli-env/bin/aws s3 cp s3://$bucket/$script_key s3://$output_key/$script_key

                shutdown -h now
                
            """,
            TagSpecifications=[{
                'ResourceType': 'instance',
                'Tags': [{'Key': 'Name', 'Value': 'KadasterS3-to-AI/ML-S3'}]
            }]
        )
        
        # Retrieve the instance ID and log
        instance_id = instance_response['Instances'][0]['InstanceId']
        print(f'Launched EC2 instance {instance_id} to process CSV file')

        # Prepare SNS messgae
        
        input_path = f"s3://{source_bucket}/{file_key}"
        output_path = f"s3://{TARGET_BUCKET}/{file_key}"
        
        sns_message = (
            f"File copied successfully!\n\n"
            f"Input S3 Path: {input_path}\n"
            f"Output S3 Path: {output_path}\n"
            f"File Name: {file_key}"
        )
        
        #Publish message to SNS
        
        sns.publish(
            TopicArn = SNS_TOPIC_ARN,
            Message = sns_message,
            Subject = "S3 File Copy Notification"
        )
        
        return {
            'statuscode' : 200,
            'body' : json.dumps('Copy succeeded!')
        }
    
    except Exception as e:
        print(f"Error copying file: {e}")
        
        
        # Publish error message to SNS
        error_message = (
            f"Error copying file.\n\n"
            f"Source Bucket : {source_bucket}\n"
            f"File Key : {file_key}\n"
            f"Target Bucket : {TARGET_BUCKET}\n"
            f"Error Details : {e}"
        )
        
        sns.publish(
            TopicArn = SNS_TOPIC_ARN,
            Message = error_message,
            Subject = "S3 File Copy Error"
        )
        
        return {
            'statuscode': 500,
            'body': json.dumps('Copy failed.')
        }
        