import os
import logging
from datetime import datetime
from tracemalloc import start

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateExternalTableOperator, BigQueryInsertJobOperator
from airflow.providers.google.cloud.transfers.gcs_to_gcs import GCSToGCSOperator

PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET = os.environ.get("GCP_GCS_BUCKET")
AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME", "/opt/airflow")

path_to_local_home = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")
BIGQUERY_DATASET = os.environ.get("BIGQUERY_DATASET", 'trips_data_all')

default_args = {
    "owner": "airflow",
    "start_date": days_ago(1),
    "depends_on_past": False,
    "retries": 1,
}

with DAG (
  dag_id="gcs_2_bigquery_dag",
    schedule_interval="@daily",
    default_args=default_args,
    catchup=False,
    max_active_runs=1,
    tags=['dtc-de'],
) as dag:

    gcs_2_gcs = GCSToGCSOperator(
        task_id = "gcs_2_gcs_task",
        source_bucket=BUCKET,
        # source_object="raw/yellow_tripdata/2019/yellow_tripdata_2019-02.parquet",
        source_object="raw/yellow_tripdata/*.parquet",
        destination_bucket=BUCKET,
        destination_object="yellow/",
        move_object=False
    )

    # GCP_PROJECT_ID: 'dtc-de-2022-mg-339910'
    # GCP_GCS_BUCKET: "dtc_data_lake_dtc-de-2022-mg"
#source gs://dtc_data_lake_dtc-de-2022-mg/yellow/*
    gcs_2_bq_ext = BigQueryCreateExternalTableOperator(
        task_id="gcs_2_bq_ext",
        table_resource={
            "tableReference": {
                "projectId": PROJECT_ID,
                "datasetId": BIGQUERY_DATASET,
                "tableId": "external_yellow_tripdata",
            },
            "externalDataConfiguration": {
                "sourceFormat": "PARQUET",
                "sourceUris": [f"gs://{BUCKET}/yellow/*"],
            },
        },
    )

    CREATE_PART_TBL_QUERY = f"CREATE OR REPLACE TABLE {BIGQUERY_DATASET}.yellow_tripdata_partitioned \
        PARTITION BY \
        DATE(tpep_pickup_datetime) AS \
        SELECT * FROM {BIGQUERY_DATASET}.external_yellow_tripdata;"


    bq_ext_2_partition = BigQueryInsertJobOperator(
        task_id = "bq_ext_2_partition",
        configuration={
            "query": {
                "query": CREATE_PART_TBL_QUERY,
                "useLegacySql": False
            }
        },
    )
    
    gcs_2_gcs >> gcs_2_bq_ext >> bq_ext_2_partition 