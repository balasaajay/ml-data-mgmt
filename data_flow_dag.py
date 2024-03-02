import textwrap
from datetime import datetime, timedelta
import pandas as pd
import os

from airflow.models.dag import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonVirtualenvOperator

DATA_REPO_NAME = os.environ['DATA_REPO_NAME']
RAW_DATA_LOCAL_PATH = f'/opt/airflow/{DATA_REPO_NAME}/WA_Fn-UseC_-Telco-Customer-Churn.csv'
TRANSFORMED_DATA_LOCAL_PATH = f'/opt/airflow/{DATA_REPO_NAME}/WA_Fn-UseC_-Telco-Customer-Churn-transformed.csv'


def processdf_virtualenv_fn():
    """
    Process dataframe in a virtual env 
    """
    import pandas as pd
    csv_path = '/opt/airflow/ml-data-mgmt/WA_Fn-UseC_-Telco-Customer-Churn.csv'
    print("{}".format(csv_path))
    df = pd.read_csv("{}".format(csv_path))
    print(df.head(2))

    print("Finished")

def virtualenv_parquet_fn():
    """
    Process dataframe in a virtual env 
    """
    import pandas as pd
    csv_path = '/opt/airflow/ml-data-mgmt/WA_Fn-UseC_-Telco-Customer-Churn.csv'
    df = pd.read_csv("{}".format(csv_path))
    df.to_parquet("/opt/airflow/ml-data-mgmt/dataset/Telco-Customer-Churn.parquet")

with DAG(
    "telco-dag",
    description="Telco DAG - download/process and version dataset",
    catchup=False,
    schedule=timedelta(days=1),
    start_date=datetime(2024, 1, 1),
) as dag:

    download_task =  BashOperator(
        task_id="download",
        bash_command="kaggle d download --unzip blastchar/telco-customer-churn --path /opt/airflow/$DATA_REPO_NAME",
    )

    virtualenv_process_data = PythonVirtualenvOperator(
        task_id="virtualenv_process_data",
        python_callable=processdf_virtualenv_fn,
        requirements=["pandas"],
        system_site_packages=False,
    )

    virtualenv_parquet = PythonVirtualenvOperator(
        task_id="virtualenv_parquet",
        python_callable=virtualenv_parquet_fn,
        requirements=["pandas", "pyarrow", "fastparquet"],
        system_site_packages=False,
    )

    dvc_task =  BashOperator(
        task_id="dvc",
        bash_command="cd /home/airflow/.kaggle/ && rm -rf ml-data-mgmt && git clone https://github.com/balasaajay/ml-data-mgmt.git && cp -rf /opt/airflow/ml-data-mgmt/dataset/Telco-Customer-Churn.parquet ml-data-mgmt/ && pwd && ls -al",
    )

    # download_task >> virtualenv_process_data >> virtualenv_parquet >> dvc_task
    dvc_task
