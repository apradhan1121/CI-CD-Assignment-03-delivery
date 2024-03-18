import json
import pandas as pd
import boto3
import logging

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

s3_client = boto3.client('s3')
sns_client = boto3.client('sns')

def lambda_handler(event, context):
    try:
        print("Step 1: Extracting bucket name and file key from the S3 event...")
        # Get the bucket and key from the S3 event
        bucket_name = event['Records'][0]['s3']['bucket']['name']
        file_key = event['Records'][0]['s3']['object']['key']
        print(f"Bucket name: {bucket_name}, File key: {file_key}")

        print("Step 2: Downloading the file from S3...")
        # Download the file from S3
        file_obj = s3_client.get_object(Bucket=bucket_name, Key=file_key)
        
        print("Step 3: Reading the file contents...")
        # Read the file contents
        file_content = file_obj['Body'].read().decode('utf-8')
        
        # Check if file content is empty
        if not file_content.strip():
            raise ValueError("File is empty")
        
        print("Step 4: Parsing JSON data...")
        # Parse JSON data
        data = json.loads(file_content)

        print("Step 5: Creating DataFrame from JSON data...")
        # Create DataFrame from JSON data
        df = pd.DataFrame(data)

        print("Step 6: Filtering records where status is 'delivered'...")
        # Filter records where status is "delivered"
        df_delivered = df[df['status'] == 'delivered']

        print("Step 7: Defining the new file name...")
        # Define the new file name
        new_file_key = 'filtered_' + file_key

        print("Step 8: Writing the filtered DataFrame to a new JSON file in S3...")
        # Write the filtered DataFrame to a new JSON file
        s3_client.put_object(Bucket='doordash-landing-zn-02', Key=new_file_key, Body=df_delivered.to_json(orient='records'))

        print("Step 9: Publishing success message to SNS topic...")
        # Publish success message to SNS topic
        sns_client.publish(
            TopicArn='arn:aws:sns:ap-south-1:291466053683:s3-trigger-SNS',
            Subject='Lambda Function Execution Success',
            Message='File processing successful!'
        )
        
        print('File processing successful!')

        return {
            'statusCode': 200,
            'body': json.dumps('File processing successful!')
        }
    except Exception as e:
        # Log the error
        logger.error(f'File processing failed: {str(e)}')
        
        print(f"Step 10: Publishing failure message to SNS topic...")
        # Publish failure message to SNS topic
        sns_client.publish(
            TopicArn='arn:aws:sns:ap-south-1:291466053683:s3-trigger-SNS',
            Subject='Lambda Function Execution Failure',
            Message=f'File processing failed: {str(e)}'
        )
        
        print(f'File processing failed: {str(e)}')

        return {
            'statusCode': 500,
            'body': json.dumps(f'File processing failed: {str(e)}')
        }
