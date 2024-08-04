import os
import sys

import pyspark.sql.functions as F
from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext, DataFrame
from pyspark.sql.types import TimestampType
from pyspark.sql.window import Window

os.environ["PYSPARK_PYTHON"] = "/usr/bin/python3"
os.environ["YARN_CONF_DIR"] = "/etc/hadoop/conf"
os.environ["PYSPARK_DRIVER_PYTHON"] = "/usr/bin/python3"
os.environ["HADOOP_CONF_DIR"] = "/etc/hadoop/conf/"


def load_events(events_path: str, start_date: str, end_date: str, spark) -> DataFrame:
    events_df = (
        spark.read.parquet(events_path)
        .where(f"date >= '{start_date}' and date <= '{end_date}'")
        .withColumn('event_dt', F.coalesce(
            F.col('datetime').cast(TimestampType()),
            (F.col('message_ts').cast(TimestampType()) + F.expr('INTERVAL 1 YEAR'))).cast(TimestampType())
        ).filter('event_phi IS NOT NULL AND event_lambda IS NOT NULL')
    ).selectExpr('event_type', 'date', 'event_dt', 'user_id', 'message_id', 'event_phi', 'event_lambda', 'subscription_channel', 'subscription_user')

    return events_df


def add_city(events_df: DataFrame, geo_data_df: DataFrame) -> DataFrame:
    events_with_distance_df = (
        events_df
        .withColumn('event_id', F.coalesce(F.col('message_id'), F.col('subscription_channel')))
        .crossJoin(geo_data_df)
        .withColumn('distance',  F.lit(2) * F.lit(6371.0) * F.asin(
            F.sqrt(F.pow(F.sin((F.col('event_phi') - F.col('phi')) / F.lit(2)), 2) +
                   F.cos(F.col('phi')) * F.cos(F.col('event_phi')) *
                   F.pow(F.sin((F.col('event_lambda') - F.col('lambda')) / F.lit(2)), 2))))
    )

    window = Window().partitionBy('event_type', 'event_id').orderBy(F.col('distance').asc())
    events_with_city_df = (
        events_with_distance_df
        .withColumn('row_number', F.row_number().over(window))
        .filter(F.col('row_number') == 1)
        .drop('phi', 'lambda', 'row_number', 'distance', 'event_phi', 'event_lambda', 'event_id')
    )

    return events_with_city_df


def main():
    start_date = sys.argv[1]
    end_date = sys.argv[2]
    events_path = sys.argv[3]
    geo_path = sys.argv[4]
    marts_output_path = sys.argv[5]

    conf = SparkConf().setAppName('GeoZonesMartJob')
    sc = SparkContext(conf=conf)
    sql = SQLContext(sc)

    # читаем гео-данные городов
    geo_data_df = sql.read.parquet(geo_path)

    # читаем события
    events_df = load_events(events_path, start_date, end_date, sql)

    # определяем город события
    events_with_city_df = add_city(events_df, geo_data_df)

    # строим и сохраняем витрину
    df = (events_with_city_df
          .withColumn("month", F.date_trunc("month", F.col("date")))
          .withColumn("week", F.date_trunc("week", F.col("date")))
          .withColumn("message", F.when(F.col("event_type") == "message", 1).otherwise(0))
          .withColumn("reaction", F.when(F.col("event_type") == "reaction", 1).otherwise(0))
          .withColumn("subscription", F.when(F.col("event_type") == "subscription", 1).otherwise(0))
          .withColumn('rank',  F.row_number().over(Window.partitionBy('user_id').orderBy(F.col('date'))))
          .withColumn('new_user',  F.when((F.col("rank") == 1) & (F.col("event_type") == "message"), 1).otherwise(0))
          .withColumnRenamed('city_id', 'zone_id')
          .drop('subscription_channel', 'subscription_user', 'event_dt')
          )

    df_m = df.groupBy("month", "zone_id").agg(
                  F.sum("message").alias("month_message"),
                  F.sum("reaction").alias("month_reaction"),
                  F.sum("subscription").alias("month_subscription"),
                  F.sum("new_user").alias("month_user"),
                  )

    df_w = df.groupBy("month", "week", "zone_id").agg(
                  F.sum("message").alias("week_message"),
                  F.sum("reaction").alias("week_reaction"),
                  F.sum("subscription").alias("week_subscription"),
                  F.sum("new_user").alias("week_user"))

    zone_mart = df_w.join(df_m, ['month', 'zone_id'], 'left').orderBy('month', 'week', 'zone_id')
    zone_mart.write.mode("overwrite").parquet(f"{marts_output_path}geo_zones/")


if __name__ == "__main__":
    main()
