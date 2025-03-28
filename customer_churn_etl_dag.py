
# Import relevant libraries
from airflow import DAG
from datetime import timedelta, datetime
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.amazon.aws.hooks.base_aws import AwsGenericHook
from airflow.providers.amazon.aws.sensors.glue import GlueJobSensor
import kagglehub
import pandas as pd
import os
import time


# S3 Bucket details
S3_BUCKET = "customer-churn-data-landing-zone-bucket"


# Define pagination variables
RECORDS_PER_FETCH = 2345


# Function to fetch data from Kaggle and dump in S3 bucket
def fetch_kaggle_data(**kwargs):
    """
    Fetches the next batch of 2345 records from the Kaggle dataset and saves it to S3.
    """
    ti = kwargs['ti']

    # Get the last fetched index from XCom (defaults to 0)
    last_index = ti.xcom_pull(task_ids='fetch_kaggle_data', key='last_index') or 0
    
    # Download dataset
    dataset_path = kagglehub.dataset_download("yeanzc/telco-customer-churn-ibm-dataset")

    # Locate the actual file dynamically
    file_path = os.path.join(dataset_path, "Telco_customer_churn.xlsx")    # The kaggle filename

    # Read the Excel file
    df = pd.read_excel(file_path, engine='openpyxl')  # Use `openpyxl` for XLSX

    # Get the next batch of records
    next_index = last_index + RECORDS_PER_FETCH
    data_batch = df.iloc[last_index:next_index]  # Slice DataFrame

    if data_batch.empty:
        print("No more data to fetch.")
        return

    # Save to CSV
    timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
    local_filename = f"kaggle_data_{timestamp}.csv"
    data_batch.to_csv(local_filename, index=False)

    # Upload to S3
    s3_hook = S3Hook(aws_conn_id="aws_new_conn")
    s3_key = f"{local_filename}"
    s3_hook.load_file(local_filename, bucket_name=S3_BUCKET, key=s3_key, replace=True)

    # Store last index for next run
    ti.xcom_push(key="last_index", value=next_index)

    # Clean up local file
    os.remove(local_filename)

    print(f"Uploaded {s3_key} to {S3_BUCKET}")


# The function that would start the Glue job run
def glue_job_s3_redshift_transfer(job_name, **kwargs):
    session = AwsGenericHook(aws_conn_id='aws_new_conn')
      
    # Get a client in the same region as the Glue job
    boto3_session = session.get_session(region_name='af-south-1')
    
    # Trigger the job using its name
    client = boto3_session.client('glue')
    client.start_job_run(
        JobName=job_name,    # The job name has been defined by the task that calls this function
    )


# The function that would retrieve the Glue job run ID
def get_run_id():
    time.sleep(8)
    session = AwsGenericHook(aws_conn_id='aws_new_conn')
    boto3_session = session.get_session(region_name='af-south-1')
    glue_client = boto3_session.client('glue')
    response = glue_client.get_job_runs(JobName="s3-load-to-redshift-etl-glue-job")
    job_run_id = response["JobRuns"][0]["Id"]
    return job_run_id 


# Define the default arguments and parameters
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 3, 26),   # Make sure to enter the current date here
    'email': ['donatus.enebuse@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(seconds=5)
}


# Authoring the sequence of the tasks
with DAG('customer-churn-etl-pipeline',
        default_args=default_args,
        schedule_interval='@weekly',  # Runs once every week
        max_active_runs=1,  # Ensures only one DAG run at a time
        catchup=False) as dag:

        # Fetch data from kaggle and dump in S3 bucket landing zone
        fetch_data_task = PythonOperator(
            task_id="fetch_kaggle_data",
            python_callable=fetch_kaggle_data,
            provide_context=True,
        )

        # Start the Glue job run to perform the ETL
        glue_job_trigger = PythonOperator(
        task_id='tsk_glue_job_trigger',
        python_callable=glue_job_s3_redshift_transfer,
        op_kwargs={
            'job_name': 's3-load-to-redshift-etl-glue-job'    # Pass this job name to the function callable
        },
        )

        # Retrieve the Glue job run ID
        grab_glue_job_run_id = PythonOperator(
        task_id='tsk_grab_glue_job_run_id',
        python_callable=get_run_id,
        )

        # Check if the Glue job is completed then end the orchestration
        is_glue_job_finish_running = GlueJobSensor(
        task_id="tsk_is_glue_job_finish_running",      
        job_name='s3-load-to-redshift-etl-glue-job',
        run_id='{{task_instance.xcom_pull("tsk_grab_glue_job_run_id")}}',
        verbose=True,     # Prints Glue job logs in Airflow logs
        aws_conn_id='aws_new_conn',
        poke_interval=5,
        timeout=3600,
        )

        fetch_data_task >> glue_job_trigger >> grab_glue_job_run_id >> is_glue_job_finish_running
