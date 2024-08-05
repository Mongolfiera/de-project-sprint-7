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


# Обновлённая таблица находится в HDFS по этому пути: /user/master/data/geo/events
def load_events(events_path: str, start_date: str, end_date: str, spark) -> DataFrame:
    events_df = (
        spark.read.parquet(events_path)
        .where("event_type = 'message'")
        .where(f"date >= '{start_date}' and date <= '{end_date}'")
        .withColumn('event_dt', F.coalesce(
            F.col('datetime').cast(TimestampType()),
            (F.col('message_ts').cast(TimestampType()) + F.expr('INTERVAL 1 YEAR'))).cast(TimestampType())
        )
    ).selectExpr('date',  'event_dt', 'user_id', 'message_id', 'event_phi', 'event_lambda')

    return events_df


def add_message_city(events_df: DataFrame, geo_data_df: DataFrame) -> DataFrame:
    events_with_distance_df = (
        events_df.crossJoin(geo_data_df)
        .withColumn('distance',  F.lit(2) * F.lit(6371.0) * F.asin(
            F.sqrt(F.pow(F.sin((F.col('event_phi') - F.col('phi')) / F.lit(2)), 2) +
                   F.cos(F.col('phi')) * F.cos(F.col('event_phi')) *
                   F.pow(F.sin((F.col('event_lambda') - F.col('lambda')) / F.lit(2)), 2))))
    )

    window = Window().partitionBy('message_id').orderBy(F.col('distance').asc())
    events_with_city_df = (
        events_with_distance_df
        .withColumn('row_number', F.row_number().over(window))
        .filter(F.col('row_number') == 1)
        .drop('phi', 'lambda', 'row_number', 'distance')
    )

    return events_with_city_df


def add_message_timezone(events_with_city_df: DataFrame, geo_data_df: DataFrame) -> DataFrame:
    geo_timezones = (geo_data_df
                     .where('city in ("Brisbane","Sydney","Adelaide","Perth","Melbourne","Darwin","Hobart","Canberra")')
                     .selectExpr('city as city_tz', 'lambda'))
    events_with_timezone_df = (
        events_with_city_df.crossJoin(geo_timezones)
        .withColumn('distance_lon',  F.lit(6371.0) * F.abs(F.col('event_lambda') - F.col('lambda'))))

    window = Window().partitionBy('message_id').orderBy(F.col('distance_lon').asc())
    events_with_timezone_df = (
        events_with_timezone_df
        .withColumn('row_number', F.row_number().over(window))
        .filter(F.col('row_number') == 1)
        .withColumn('timezone', F.concat(F.lit('Australia/'), F.col('city_tz')))
        .drop('lambda', 'row_number', 'distance_lon', 'city_tz')
    )

    return events_with_timezone_df


def get_cities(events_with_timezone_df: DataFrame) -> DataFrame:
    window = Window.partitionBy('user_id').orderBy(F.col('date'), F.col('city'))
    max_date = events_with_timezone_df.agg(F.max(F.col('date'))).collect()[0][0]

    user_cities_df = (
        events_with_timezone_df
        .withColumn('next_city', F.lead('city', 1).over(window))
        .filter('(city != next_city) or (next_city is Null)')
        .withColumn('next_date', F.coalesce(F.lead('date', 1).over(window), F.lit(max_date)))
        .withColumn('days_diff', F.datediff(F.col('next_date'), F.col('date')))
        .withColumn('travel_array', F.collect_list(F.col('city')).over(window))
        .withColumn('travel_count', F.count(F.col("city")).over(window))
        .withColumn('rank', F.row_number().over(window))
        .drop('event_type', 'event_lambda', 'event_phi'))

    home_city = (
        user_cities_df
        .filter("rank == 1 or days_diff > 27")  # если не найдется город, в котором пользователь был > 27д, домашним считаем первый город
        .withColumn("hc_rank", F.row_number().over(
            Window.partitionBy('user_id').orderBy(F.col('date').desc(), F.col('city').desc())))
        .filter("hc_rank == 1")
        .selectExpr('user_id', 'city as home_city'))

    travel_cities = (
        user_cities_df
        .withColumn("desc_rank", F.row_number().over(
            Window.partitionBy('user_id').orderBy(F.col('date').desc(), F.col('city').desc())))
        .filter("desc_rank == 1")
        .withColumn("local_time", F.from_utc_timestamp(F.col("event_dt"), F.col("timezone")))
        .join(home_city, "user_id", "left")
        .selectExpr('user_id', 'city as act_city', 'home_city', "travel_count", "travel_array", "local_time", "timezone"))

    return travel_cities


def main():
    start_date = sys.argv[1]
    end_date = sys.argv[2]
    events_path = sys.argv[3]
    geo_path = sys.argv[4]
    marts_output_path = sys.argv[5]

    conf = SparkConf().setAppName('UsersMartJob')
    sc = SparkContext(conf=conf)
    sql = SQLContext(sc)

    # start_date = '2022-03-01'
    # end_date = '2022-05-31'
    # events_path = '/user/helendrug/data/project/events/'
    # geo_path = '/user/helendrug/data/project/geo/'
    # marts_output_path = '/user/helendrug/data/project/analytics/'

    # читаем гео-данные городов
    geo_data_df = sql.read.parquet(geo_path)

    # читаем события
    events_df = load_events(events_path, start_date, end_date, sql)

    # определяем город события
    events_with_city_df = add_message_city(events_df, geo_data_df)

    # определяем часовой пояс события
    events_with_timezone_df = add_message_timezone(events_with_city_df, geo_data_df)

    # строим и сохраняем витрину
    user_mart = get_cities(events_with_timezone_df)

    user_mart.write.mode("overwrite").parquet(f"{marts_output_path}users/")


if __name__ == "__main__":
    main()
