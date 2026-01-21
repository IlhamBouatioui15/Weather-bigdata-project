from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'Maria',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

with DAG(
    'weather_pipeline',
    default_args=default_args,
    description='Pipeline Weather API → NiFi → Kafka → Spark → MinIO',
    schedule_interval='@hourly',
    start_date=datetime(2025, 12, 23),
    catchup=False,
) as dag:

    # 1️⃣ Déclencher NiFi flow via REST API
    run_nifi_flow = BashOperator(
        task_id="run_nifi_flow",
        bash_command="""
        curl -X PUT http://bigdataproject-nifi-1:8080/nifi-api/flow/process-groups/0b1024d0-019a-1000-df8a-68baa1426991 \
        -H "Content-Type: application/json" \
        -d '{
        "id": "0b1024d0-019a-1000-df8a-68baa1426991",
        "state": "RUNNING"
        }'

        """
    )


    # 2️⃣ Lancer Spark Streaming
    run_spark = BashOperator(
        task_id='run_spark_streaming',
        bash_command=(
            'docker exec bigdataproject-spark-1 spark-submit '
            '--conf spark.jars.ivy=/opt/spark/work-dir/.ivy2 '
            '--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,'
            'org.apache.hadoop:hadoop-aws:3.3.4,'
            'com.amazonaws:aws-java-sdk-bundle:1.12.262 '
            '/opt/spark/work-dir/weather_streaming.py'
        )

    )


