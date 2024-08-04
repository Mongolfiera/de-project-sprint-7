from datetime import datetime
import os

from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

os.environ['HADOOP_CONF_DIR'] = '/etc/hadoop/conf'
os.environ['YARN_CONF_DIR'] = '/etc/hadoop/conf'
os.environ['JAVA_HOME'] = '/usr'
os.environ['SPARK_HOME'] = '/usr/lib/spark'

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2020, 1, 1),
}

dag_spark = DAG(
    dag_id='user_mart_dag',
    default_args=default_args,
    schedule_interval=None,
)

# объявляем задачу с помощью SparkSubmitOperator
spark_user_mart = SparkSubmitOperator(
                        task_id='user_mart_task',
                        dag=dag_spark,
                        application='/lessons/users_mart.py',
                        conn_id='yarn_spark',
                        application_args=[
                            '2022-03-01',
                            '2022-05-31',
                            '/user/helendrug/data/project/events/',
                            '/user/helendrug/data/project/geo/',
                            '/user/helendrug/data/project/analytics/'
                        ],
                        conf={
                            "spark.driver.maxResultSize": '20g'
                        },
                        executor_cores=2,
                        executor_memory='2g'
                        )

spark_user_mart
