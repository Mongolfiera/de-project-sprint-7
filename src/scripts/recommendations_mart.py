import os
import sys
from datetime import date, datetime, timedelta

from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext, DataFrame
from pyspark.sql.types import DateType, TimestampType
import pyspark.sql.functions as F
from pyspark.sql.window import Window

os.environ["PYSPARK_PYTHON"] = "/usr/bin/python3"
os.environ["YARN_CONF_DIR"] = "/etc/hadoop/conf"
os.environ["PYSPARK_DRIVER_PYTHON"] = "/usr/bin/python3"
os.environ["HADOOP_CONF_DIR"] = "/etc/hadoop/conf/"

def load_events(events_path: str, start_date: str, end_date: str, spark) -> DataFrame:
    events_df = (
        spark.read.parquet(events_path).sample(0.3)
        .where(f"date >= '{start_date}' and date <= '{end_date}' and event_phi is not null and event_lambda is not null")
        .withColumn('event_dt', F.coalesce(
            F.col('datetime').cast(TimestampType()), 
            (F.col('message_ts').cast(TimestampType()) + F.expr('INTERVAL 1 YEAR'))).cast(TimestampType()))
#         .filter('event_phi IS NOT NULL AND event_lambda IS NOT NULL')
    ).selectExpr('event_type', 'date', 'event_dt', 'user_id', 'message_id', 'message_to', 'event_phi', 'event_lambda', 'subscription_channel', 'subscription_user')

    return events_df

def get_messages_pairs(events_df: DataFrame) -> DataFrame:
    messages = (
        events_df
        .where(("event_type = 'message' AND message_to IS NOT NULL"))
        .select(F.col('user_id'), F.col("message_to")).distinct()
    )

    messages_rev = (
        messages
        .withColumnRenamed('message_to', 'user_id_left')
        .withColumnRenamed('user_id', 'user_id_right')
    )

    messages_pairs = (
        messages
        .withColumnRenamed('user_id', 'user_id_left')
        .withColumnRenamed('message_to', 'user_id_right')
        .union(messages_rev)
        .dropDuplicates()
    )
    
    return messages_pairs
        
def get_subscriptions_pairs(events_df: DataFrame) -> DataFrame:
    subscribtions = (
        events_df
        .where("event_type = 'subscription' AND subscription_channel IS NOT NULL")
        .select( 'user_id', 'event_phi', 'event_lambda', 'subscription_channel').distinct()
    )

    subscribtions_pairs = (
        subscribtions
        .withColumnRenamed('user_id', 'user_id_left')
        .withColumnRenamed('event_phi', 'event_phi_left')
        .withColumnRenamed('event_lambda', 'event_lambda_left')
        .join(subscribtions, 'subscription_channel', 'inner')
        .filter(F.col('user_id_left') != F.col('user_id'))
        .withColumnRenamed('user_id', 'user_id_right')
        .withColumnRenamed('event_phi', 'event_phi_right')
        .withColumnRenamed('event_lambda', 'event_lambda_right')
    #     .select('user_id_left', 'user_id_right').distinct()
        .withColumn('distance',  F.lit(2) * F.lit(6371.0) * F.asin(
            F.sqrt(F.pow(F.sin((F.col('event_phi_left') - F.col('event_phi_right')) / F.lit(2)), 2) +
                   F.cos(F.col('event_phi_right')) * F.cos(F.col('event_phi_left')) *
                   F.pow(F.sin((F.col('event_lambda_left') - F.col('event_lambda_right')) / F.lit(2)), 2)
                  )))
        .filter("distance <= 1")
    )

    return subscribtions_pairs

def get_unique_subscriptions_pairs(subscribtions_pairs: DataFrame, messages_pairs: DataFrame) -> DataFrame:
    
    unique_subscribtions_pairs = (
        subscribtions_pairs
        .withColumnRenamed('event_phi_left', 'event_phi')
        .withColumnRenamed('event_lambda_left', 'event_lambda')
        .join(messages_pairs, ['user_id_left', 'user_id_right'], 'left_anti')
        .select('user_id_left', 'user_id_right', 'event_phi', 'event_lambda').distinct())
    
    return unique_subscribtions_pairs

def add_event_city(events_df: DataFrame, geo_data_df: DataFrame) -> DataFrame:
    events_with_distance_df = (
        events_df.crossJoin(geo_data_df)
        .withColumn('distance',  F.lit(2) * F.lit(6371.0) * F.asin(
            F.sqrt(F.pow(F.sin((F.col('event_phi') - F.col('phi')) / F.lit(2)), 2) +
                   F.cos(F.col('phi')) * F.cos(F.col('event_phi')) *
                   F.pow(F.sin((F.col('event_lambda') - F.col('lambda')) / F.lit(2)), 2)
                  )))
    )

    window = Window().partitionBy('user_id_left').orderBy(F.col('distance').asc())
    events_with_city_df = (
        events_with_distance_df
        .withColumn('row_number', F.row_number().over(window))
        .filter(F.col('row_number')==1)
        .drop('phi', 'lambda', 'row_number', 'distance')
    )

    return events_with_city_df


def add_event_timezone(events_with_city_df: DataFrame, geo_data_df: DataFrame) -> DataFrame:
    geo_timezones = (geo_data_df
                     .where('city in ("Brisbane","Sydney","Adelaide","Perth","Melbourne","Darwin","Hobart","Canberra")')
                     .selectExpr('city as city_tz', 'lambda')
                    )
    events_with_timezone_df = (
        events_with_city_df.crossJoin(geo_timezones)
        .withColumn('distance_lon',  F.lit(6371.0) * F.abs(F.col('event_lambda') - F.col('lambda'))
    ))

    window = Window().partitionBy('user_id_left').orderBy(F.col('distance_lon').asc())
    events_with_timezone_df = (
        events_with_timezone_df
        .withColumn('row_number', F.row_number().over(window))
        .filter(F.col('row_number') == 1)
        .withColumn('timezone', F.concat(F.lit('Australia/'), F.col('city_tz')))
        .selectExpr('user_id_left', 'user_id_right', 'city_id as zone_id', 'timezone as local_time').distinct()
    )

    return events_with_timezone_df

def main():
    start_date = sys.argv[1]
    end_date = sys.argv[2]
    events_path = sys.argv[3]
    geo_path = sys.argv[4]
    marts_output_path = sys.argv[5]

    conf = SparkConf().setAppName('GeoMartJob')
    sc = SparkContext(conf=conf)
    sql = SQLContext(sc)
    
    # читаем гео-данные городов
    geo_data_df = sql.read.parquet(geo_path)
    
    # читаем события
    events_df = load_events(events_path, start_date, end_date, sql)
    
    # получаем пары пользователей, отсылавших друг другу сообщения
    messages_pairs = get_messages_pairs(events_df)

    subscribtions_pairs =  get_subscriptions_pairs(events_df)
    
    unique_subscribtions_pairs = get_unique_subscriptions_pairs(subscribtions_pairs, messages_pairs)
    
    pairs_with_cities = add_event_city(unique_subscribtions_pairs, geo_data_df)
    
    recommendations_mart = add_event_timezone(pairs_with_cities, geo_data_df)
    recommendations_mart.write.mode("overwrite").parquet(f"{marts_output_path}recommendations/")
    
if __name__ == "__main__":
    main()
