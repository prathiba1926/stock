import json
import datetime as dt
import requests
import Consonants as constant
import pandas as pd
import boto3
import os
pd.set_option('display.max_columns', None)
import logging

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Initialize AWS clients
client = boto3.client("ssm")
sns = boto3.client("sns", region_name="ap-southeast-2")
ssm = boto3.client("ssm", region_name="ap-southeast-2")
s3 = boto3.client('s3')

# Fetch URL from SSM Parameter Store
#url = ssm.get_parameter(Name=constant.urlapi, WithDecryption=True)["Parameter"]["Value"]

def send_sns_success():
    try:
        success_sns_arn = ssm.get_parameter(Name=constant.SUCCESSNOTIFICATIONARN, WithDecryption=True)["Parameter"]["Value"]
        print("SARN",success_sns_arn)
        component_name = constant.COMPONENT_NAME
        env = ssm.get_parameter(Name=constant.ENVIRONMENT, WithDecryption=True)['Parameter']['Value']
        success_msg = constant.SUCCESS_MSG
        sns_message = f"{component_name} : {success_msg}"
        logger.info(f"Sending SNS Success Message: {sns_message}")
        succ_response = sns.publish(TargetArn=success_sns_arn,Message=json.dumps({'default': sns_message}),Subject=f"{env} : {constant.COMPONENT_NAME}",MessageStructure="json")
        return succ_response
    except Exception as sns_error:
        logger.error("Failed to send success SNS", exc_info=True)

def send_error_sns():
    try:
        error_sns_arn = ssm.get_parameter(Name=constant.ERRORNOTIFICATIONARN, WithDecryption=True)["Parameter"]["Value"]
        env = ssm.get_parameter(Name=constant.ENVIRONMENT, WithDecryption=True)['Parameter']['Value']
        error_message = constant.ERROR_MSG
        component_name = constant.COMPONENT_NAME
        sns_message = f"{component_name} : {error_message}"
        logger.error(f"Sending SNS Error Message: {sns_message}")
        err_response = sns.publish(TargetArn=error_sns_arn,Message=json.dumps({'default': sns_message}),Subject=f"{env} : {constant.COMPONENT_NAME}",MessageStructure="json")
        return err_response
    except Exception as sns_error:
        logger.error("Failed to send error SNS", exc_info=True)


def lambda_handler(event, context):
    try:
        url = ssm.get_parameter(Name=constant.urlapi, WithDecryption=True)["Parameter"]["Value"]
        response = requests.get(url)
        response.raise_for_status()
        logger.info(f"Fetched data from URL: {url}")
        b = response.json()

        # Process CSV files
        csv_files = [file['download_url'] for file in b if file['name'].endswith('.csv')]
        if not csv_files:
            logger.warning("No CSV files found.")
            return {
                'statusCode': 404,
                'body': json.dumps('No CSV files available')
            }

        d = pd.read_csv(csv_files.pop())
        dataframes = []
        
        for url in csv_files:
            file_name = url.split("/")[-1].replace(".csv", "")
            df = pd.read_csv(url)
            df['Symbol'] = file_name
            dataframes.append(df)
            logger.info(f"Processed file: {file_name}")

        combined_df = pd.concat(dataframes, ignore_index=True)
        o_df = pd.merge(combined_df, d, on='Symbol', how='left')

        # Group data
        o_df["timestamp"] = pd.to_datetime(o_df["timestamp"], errors='coerce')
        filtered_df = o_df[(o_df['timestamp'] >= "2021-01-01") & (o_df['timestamp'] <= "2021-05-26")]
        result_time = filtered_df.groupby("Sector").agg({
            'open': 'mean', 'close': 'mean', 'high': 'max', 'low': 'min', 'volume': 'mean'
        }).reset_index()

        list_sector = ["TECHNOLOGY", "FINANCE"]
        result_time = result_time[result_time["Sector"].isin(list_sector)].reset_index(drop=True)

        logger.info("Data processing complete.")

        # Upload to S3
        current_datetime = dt.datetime.now().strftime('%Y-%m-%d_%H-%M-%S')
        csv_data = result_time.to_csv(index=False)
        bucket_name = os.getenv('BUCKET_NAME', 'stock-dev-06')
        file_name = f'stock_data_{current_datetime}.csv'
        s3.put_object(Bucket=bucket_name, Key=file_name, Body=csv_data.encode('utf-8'))
        logger.info(f"File {file_name} uploaded to S3 bucket {bucket_name}.")

        send_sns_success()

        return {
            'statusCode': 200,
            'body': json.dumps('Processing complete and file uploaded successfully.')
        }

    except Exception as e:
        logger.error("An error occurred during execution", exc_info=True)
        send_error_sns()
        return {
            'statusCode': 500,
            'body': json.dumps('An error occurred during processing.')
        }
