import json
import os
import boto3
import pandas as pd
from datetime import datetime
from kaggle.api.kaggle_api_extended import KaggleApi

def lambda_handler(event, context):
    
    # Initialize Kaggle API
    api = KaggleApi()
    api.authenticate()
    
    # Download insurance dataset from Kaggle
    dataset_name = "easonlai/sample-insurance-claim-prediction-dataset"
    api.dataset_download_files(dataset_name, path='/tmp/', unzip=True)
    
    # Read CSV and convert to JSON
    df = pd.read_csv('/tmp/insurance_claims.csv')
    insurance_data = df.to_dict('records')
    
    # Upload to S3
    client = boto3.client('s3')
    filename = "insurance_raw_" + str(datetime.now()) + ".json"
    
    client.put_object(
        Bucket="insurance-etl-pipeline-demo",
        Key="raw_data/to_processed/" + filename,
        Body=json.dumps(insurance_data)
    )