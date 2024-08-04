import sys
from datetime import datetime, timedelta

import pyspark.sql.functions as F
from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext


def main():
    date = sys.argv[1]
    depth = int(sys.argv[2])
    base_input_path = sys.argv[3]
    base_output_path = sys.argv[4]
    geo_data_path = sys.argv[5]
    geo_output_path = sys.argv[6]
    base_date = datetime.strptime(date, '%Y-%m-%d')

    conf = SparkConf().setAppName('LoadDataJob')
    sc = SparkContext(conf=conf)
    sql = SQLContext(sc)

    geo_data = (sql.read.option('delimiter', ';').option('header', True).csv(geo_data_path)
                .withColumn('lat', F.regexp_replace('lat', ',', '.').cast('float'))
                .withColumn('lng', F.regexp_replace('lng', ',', '.').cast('float'))
                ).selectExpr('id as city_id', 'city', 'radians(lat) as phi', 'radians(lng) as lambda')

    # запись данных
    geo_data.write.mode('overwrite').format('parquet').save(f'{geo_output_path}')

    for day in range(depth):
        date_d = (base_date-timedelta(days=day)).strftime('%Y-%m-%d')

        # чтение данных
        events = (
            sql.read.parquet(f'{base_input_path}/date={date_d}')
            .withColumn('date', F.lit(date_d))
            .withColumn('user_id', F.coalesce(
                F.col('event.user'), F.col('event.message_from'), F.col('event.reaction_from')))
        ).selectExpr('event_type', 'user_id', 'event.message_id as message_id',
                     'event.message_to as message_to',
                     'radians(lat) as event_phi', 'radians(lon) as event_lambda',
                     'event.subscription_channel as subscription_channel',
                     'event.subscription_user as subscription_user',
                     'date', 'event.datetime as datetime', 'event.message_ts as message_ts')

        # запись данных
        events.write.mode('overwrite').partitionBy('event_type').format('parquet').save(f'{base_output_path}/date={date_d}')


if __name__ == '__main__':
    main()
