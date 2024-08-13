import http.client
import boto3
import json
from botocore.exceptions import NoCredentialsError

def fetch_job_details():
   
    conn = http.client.HTTPSConnection("fresh-linkedin-profile-data.p.rapidapi.com")

    headers = {
        'x-rapidapi-key': "a2415ae77amsh2fcd50faf54c128p15eb0cjsn2e1bea820544",
        'x-rapidapi-host': "fresh-linkedin-profile-data.p.rapidapi.com"
    }

    conn.request("GET", "/get-job-details?job_url=https%3A%2F%2Fwww.linkedin.com%2Fjobs%2Fview%2F3766410207%2F&include_skills=false&include_hiring_team=false", headers=headers)

    res = conn.getresponse()
    data = res.read()
    data_str = data.decode("utf-8")

   
    print(data_str)


    s3_client = boto3.client('s3', region_name='your-region')  
    bucket_name = 'airflow-001' 
    s3_key = 's3://airflow-001/src-raw-001/'  

    try:
        s3_client.put_object(Body=data_str, Bucket=bucket_name, Key=s3_key)
        print(f"Data successfully uploaded to S3 bucket '{bucket_name}' with key '{s3_key}'")
    except NoCredentialsError:
        print("Credentials not available")

    return data_str
