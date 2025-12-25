import json
import boto3
from io import StringIO
from datetime import datetime
import pandas as pd

def policy(data):
    policy_list = []
    for row in data:
        policy_id = row.get('policy_number', f"POL_{len(policy_list)+1}")
        customer_age = row.get('age', 30)
        gender = row.get('sex', 'Unknown')
        region = row.get('policy_state', 'Unknown')
        premium_amount = row.get('policy_annual_premium', 0)
        policy_element = {
            'policy_id': policy_id,
            'customer_age': customer_age,
            'gender': gender,
            'region': region,
            'premium_amount': premium_amount
        }
        policy_list.append(policy_element)
    return policy_list

def claims(data):
    claims_list = []
    for row in data:
        claim_id = row.get('policy_number', f"CLM_{len(claims_list)+1}")
        incident_date = row.get('incident_date', str(datetime.now().date()))
        incident_type = row.get('incident_type', 'Unknown')
        claim_amount = row.get('total_claim_amount', 0)
        fraud_reported = row.get('fraud_reported', 'N')
        claim_element = {
            'claim_id': claim_id,
            'incident_date': incident_date,
            'incident_type': incident_type,
            'claim_amount': claim_amount,
            'fraud_reported': fraud_reported
        }
        claims_list.append(claim_element)
    return claims_list

def customers(data):
    customer_list = []
    for row in data:
        customer_id = row.get('policy_number', f"CUST_{len(customer_list)+1}")
        age = row.get('age', 30)
        gender = row.get('sex', 'Unknown')
        occupation = row.get('occupation', 'Unknown')
        annual_income = row.get('capital-gains', 0)
        customer_element = {
            'customer_id': customer_id,
            'age': age,
            'gender': gender,
            'occupation': occupation,
            'annual_income': annual_income
        }
        customer_list.append(customer_element)
    return customer_list

def lambda_handler(event, context):
    s3 = boto3.client('s3')
    Bucket = "insurance-etl-pipeline-demo"
    Key = "raw_data/to_processed/"
    
    insurance_data = []
    insurance_keys = []

    for file in s3.list_objects(Bucket=Bucket, Prefix=Key)['Contents']:
        file_key = file['Key']
        if file_key.split('.')[-1] == "json":
            response = s3.get_object(Bucket=Bucket, Key=file_key)
            content = response['Body']
            jsonObject = json.loads(content.read())
            insurance_data.append(jsonObject)
            insurance_keys.append(file_key)
            
    for data in insurance_data:
        policy_list = policy(data)
        claims_list = claims(data)
        customer_list = customers(data)
        
        policy_df = pd.DataFrame.from_dict(policy_list)
        policy_df = policy_df.drop_duplicates(subset=['policy_id'])
        
        claims_df = pd.DataFrame.from_dict(claims_list)
        claims_df = claims_df.drop_duplicates(subset=['claim_id'])
        
        customer_df = pd.DataFrame.from_dict(customer_list)
        customer_df = customer_df.drop_duplicates(subset=['customer_id'])
        
        # Save to S3
        policy_key = "transformed_data/policy_data/policy_transformed_" + str(datetime.now()) + ".csv"
        policy_buffer = StringIO()
        policy_df.to_csv(policy_buffer, index=False)
        policy_content = policy_buffer.getvalue()
        s3.put_object(Bucket=Bucket, Key=policy_key, Body=policy_content)
        
        claims_key = "transformed_data/claims_data/claims_transformed_" + str(datetime.now()) + ".csv"
        claims_buffer = StringIO()
        claims_df.to_csv(claims_buffer, index=False)
        claims_content = claims_buffer.getvalue()
        s3.put_object(Bucket=Bucket, Key=claims_key, Body=claims_content)
        
        customer_key = "transformed_data/customer_data/customer_transformed_" + str(datetime.now()) + ".csv"
        customer_buffer = StringIO()
        customer_df.to_csv(customer_buffer, index=False)
        customer_content = customer_buffer.getvalue()
        s3.put_object(Bucket=Bucket, Key=customer_key, Body=customer_content)
        
    # Move processed files
    s3_resource = boto3.resource('s3')
    for key in insurance_keys:
        copy_source = {
            'Bucket': Bucket,
            'Key': key
        }
        s3_resource.meta.client.copy(copy_source, Bucket, 'raw_data/processed/' + key.split("/")[-1])
        s3_resource.Object(Bucket, key).delete()