from datetime import datetime
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
import sys
import psycopg2
import pandas
import datetime as dt
import boto3

# ETL pipeline orchestration tool
# scheduler/worker/webserver(visualize for use to monitor airflow job)

##steps:
# 1. extract data from source tables
# 2. apply some changes, minimum change, might apply some audit column, (created_at, updated_at)
# 3. load extracted data into destionation, save extracted data into a csv file, copy into redshift table

host = "ac-shopping-crm.cmxlethtljrk./*******"
username = ""
password = ""
port = "5432"
dbname = "ac_shopping_crm"

redshift_host = (
    "ac-shopping-datawarehouse.************.amazonaws.com"
)
redshift_username = ""
redshift_password = ""
redshift_port = "5439"
redshift_dbname = "data_lake"


def connect_postgres_db(host, username, password, dbname, port):
    postgres_db_connection = psycopg2.connect(
        host=host, user=username, password=password, port=port, dbname=dbname,
    )

    return postgres_db_connection


def get_incremental_load_parameter(parameter_name):
    ssm_client = boto3.client("ssm", region_name="ap-southeast-2",)
    response = ssm_client.get_parameter(Name=parameter_name)
    return response["Parameter"]["Value"]


def update_incremental_load_parameter(parameter_name, parameter_value):
    try:
        ssm_client = boto3.client("ssm", region_name="ap-southeast-2",)
        ssm_client.put_parameter(
            Name=parameter_name, Value=parameter_value, Overwrite=True,
        )
    except Exception as e:
        print("update ssm parameter value failed: {e}")
        raise ValueError


def extract_data(table_name, table_columns, last_loaded_datetime):
    ac_shopping_crm_db_connection = connect_postgres_db(
        host, username, password, dbname, port
    )

    extraction_df = pandas.read_sql(
        """select {}
           from ac_shopping.{}
           where updated_at>='{}'""".format(
            ",".join(table_columns), table_name, last_loaded_datetime
        ),
        ac_shopping_crm_db_connection,
    )

    extraction_df.to_csv(
        "home/airflow/airflow/tmp/{}.csv".format(table_name),
        header="true",
        index=False,
    )


def upload_csv_to_s3(file_to_upload, s3_bucket_name, s3_folder_path):
    s3_client = boto3.client(
        "s3",
        aws_access_key_id="AKIA2*****F5M",
        aws_secret_access_key="xJMQ6Skg********MjwatwCCUV2RS",
    )
    s3_client.upload_file(file_to_upload, s3_bucket_name, s3_folder_path)


def copy_csv_to_redshift_staging(
    table_name,
    table_columns,
    s3_bucket_name,
    s3_file_folder_path,
    copy_extra_parameters,
):
    redshift_db_connection = connect_postgres_db(
        redshift_host,
        redshift_username,
        redshift_password,
        redshift_dbname,
        redshift_port,
    )
    copy_cmd = """copy staging.{}({})
                   from 's3://{}/{}/{}.csv'
                   credentials 'aws_iam_role=arn:*****role/redshift-admin'
                   {}""".format(
        table_name,
        table_columns,
        s3_bucket_name,
        s3_file_folder_path,
        table_name,
        copy_extra_parameters,
    )
    cursor = redshift_db_connection.cursor()
    cursor.execute(copy_cmd)
    cursor.execute("commit")


def upsert_staging_into_destination_table(table_name, table_columns, primary_key):
    delete_sql_cmd = """delete from ac_shopping_crm.{} where {} in (
        select {} from staging.{});""".format(
        table_name, primary_key, primary_key, table_name
    )

    insert_sql_cmd = """insert into ac_shopping_crm.{} ({})
                       select {} from staging.{}""".format(
        table_name, ",".join(table_columns), ",".join(table_columns), table_name,
    )

    redshift_db_connection = connect_postgres_db(
        redshift_host,
        redshift_username,
        redshift_password,
        redshift_dbname,
        redshift_port,
    )
    cursor = redshift_db_connection.cursor()
    cursor.execute(delete_sql_cmd + "\n" + insert_sql_cmd)
    cursor.execute("COMMIT")


def main():
    s3_bucket_name = "ac-shopping-datalake"
    s3_file_folder_path = "ac_shopping_crm_ch"
    export_file_name = "product.csv"
    export_file_s3_location = "ac_shopping_crm_ch/product.csv"

    table_name = "product"
    table_columns = [
        "product_id",
        "product_title",
        "brand_name",
        "category_level_1",
        "category_level_2",
        "image_url",
        "is_active",
        "unit_price",
        "quantity_for_sale",
        "created_at",
        "updated_at",
    ]

    parameter_name = "de_daily_load_ac_shopping_crm_product"

    # last_loaded_datetime = "2021-06-13 14:30:00"
    last_loaded_datetime = get_incremental_load_parameter(parameter_name)

    print(last_loaded_datetime)

    extract_data(table_name, table_columns, last_loaded_datetime)

    upload_csv_to_s3(
        "/home/airflow/airflow/tmp/" + export_file_name,
        s3_bucket_name,
        export_file_s3_location,
    )
    copy_extra_parameters = "csv delimiter ',' ignoreheader 1"

    copy_csv_to_redshift_staging(
        table_name,
        ",".join(table_columns),
        s3_bucket_name,
        s3_file_folder_path,
        copy_extra_parameters,
    )

    upsert_staging_into_destination_table(table_name, table_columns, "product_id")

    redshift_db_connection = connect_postgres_db(
        redshift_host,
        redshift_username,
        redshift_password,
        redshift_dbname,
        redshift_port,
    )
    cursor = redshift_db_connection.cursor()

    cursor.execute(
        """select date_add('days',-2,max(created_at)) from ac_shopping_crm_ch.{};""".format(
            table_name
        )
    )
    parameter_value = cursor.fetchall()[0][0]

    print(parameter_value)

    update_incremental_load_parameter(parameter_name, str(parameter_value))


dag = DAG(
    # dag name
    "de_daily_dummy_etlch",
    description="Simple tutorial DAG",
    # when start run job, middle of day/crontab guru
    schedule_interval="0 12 * * *",
    start_date=datetime(2022, 1, 20),
    # no need to run fail job
    catchup=False,
)

dummy_operator = DummyOperator(task_id="start_etl_job", retries=0, dag=dag)

# execute code/ which fuction execute
etl_operator = PythonOperator(task_id="main_etl_job", python_callable=main, dag=dag)

# define dependency
dummy_operator >> etl_operator
