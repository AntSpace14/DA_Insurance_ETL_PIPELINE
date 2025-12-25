import json
import boto3
from datetime import datetime

def policy(data):
    policy_list = []
    for i, row in enumerate(data):
        policy_element = {
            'policy_id': f"POL_{i+1}",
            'customer_age': row.get('age', '30'),
            'gender': row.get('sex', 'Unknown'),
            'region': row.get('region', 'Unknown'),
            'premium_amount': row.get('charges', '0')
        }
        policy_list.append(policy_element)
    return policy_list

def claims(data):
    claims_list = []
    for i, row in enumerate(data):
        claim_element = {
            'claim_id': f"CLM_{i+1}",
            'policy_id': f"POL_{i+1}",
            'claim_amount': row.get('charges', '0'),
            'bmi': row.get('bmi', '0'),
            'smoker': row.get('smoker', 'no')
        }
        claims_list.append(claim_element)
    return claims_list

def customers(data):
    customer_list = []
    for i, row in enumerate(data):
        customer_element = {
            'customer_id': f"CUST_{i+1}",
            'policy_id': f"POL_{i+1}",
            'age': row.get('age', '30'),
            'gender': row.get('sex', 'Unknown'),
            'children': row.get('children', '0'),
            'smoker': row.get('smoker', 'no'),
            'region': row.get('region', 'Unknown')
        }
        customer_list.append(customer_element)
    return customer_list

def lambda_handler(event, context):
    s3 = boto3.client('s3')
    Bucket = "insurance-etl-datalake-antariksh"  # UPDATE THIS
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
        
        # Convert to CSV strings (no pandas needed)
        policy_csv = "policy_id,customer_age,gender,region,premium_amount\n"
        for item in policy_list:
            policy_csv += f"{item['policy_id']},{item['customer_age']},{item['gender']},{item['region']},{item['premium_amount']}\n"
        
        claims_csv = "claim_id,policy_id,claim_amount,bmi,smoker\n"
        for item in claims_list:
            claims_csv += f"{item['claim_id']},{item['policy_id']},{item['claim_amount']},{item['bmi']},{item['smoker']}\n"
        
        customer_csv = "customer_id,policy_id,age,gender,children,smoker,region\n"
        for item in customer_list:
            customer_csv += f"{item['customer_id']},{item['policy_id']},{item['age']},{item['gender']},{item['children']},{item['smoker']},{item['region']}\n"
        
        # Save to S3
        policy_key = "transformed_data/policy_data/policy_transformed_" + str(datetime.now()) + ".csv"
        s3.put_object(Bucket=Bucket, Key=policy_key, Body=policy_csv)
        
        claims_key = "transformed_data/claims_data/claims_transformed_" + str(datetime.now()) + ".csv"
        s3.put_object(Bucket=Bucket, Key=claims_key, Body=claims_csv)
        
        customer_key = "transformed_data/customer_data/customer_transformed_" + str(datetime.now()) + ".csv"
        s3.put_object(Bucket=Bucket, Key=customer_key, Body=customer_csv)
        
    # Move processed files
    s3_resource = boto3.resource('s3')
    for key in insurance_keys:
        copy_source = {
            'Bucket': Bucket,
            'Key': key
        }
        s3_resource.meta.client.copy(copy_source, Bucket, 'raw_data/processed/' + key.split("/")[-1])
        s3_resource.Object(Bucket, key).delete()
