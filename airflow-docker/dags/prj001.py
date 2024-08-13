from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.operators.glue import AwsGlueJobOperator
from datetime import datetime
import http.client
import boto3
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

    # Upload to S3
    s3_client = boto3.client('s3', region_name='your-region')  
    bucket_name = 'airflow-001'  
    s3_key = 'src-raw-001/data.json'  

    try:
        s3_client.put_object(Body=data_str, Bucket=bucket_name, Key=s3_key)
        print(f"Data successfully uploaded to S3 bucket '{bucket_name}' with key '{s3_key}'")
    except NoCredentialsError:
        print("Credentials not available")

    return data_str



default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 8, 12),
    'retries': 1,
}


dag = DAG(
    'prj001',
    default_args=default_args,
    description='A simple test DAG',
    schedule_interval='@daily',
)

fetch_data_task = PythonOperator(
    task_id='fetch_data_and_upload_to_s3',
    python_callable=fetch_job_details,
    dag=dag,
)

run_glue_job_task = AwsGlueJobOperator(
    task_id='run_glue_job',
    job_name='prj001',
    script_location='s3://airflow-001/src-script/',
    iam_role_name='AWSGlueServiceRole-suraj0014',
    dag=dag,
)

fetch_data_task >> run_glue_job_task
