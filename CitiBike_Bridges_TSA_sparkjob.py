
import pyspark
from pyspark.sql import *
from pyspark.sql.functions import *
import sys



def clean_and_merge_citi(spark, folder):
    extension = "/*-citibike-tripdata.csv"  # all monthly citibike data csv files

    # no need to clean; I evaluated it before w OpenRefine and it all looks good!

    d = spark.read.csv(folder +  extension, header=True, inferSchema=True)  # we can read all at once!

    start_info = split(d["starttime"], "-")
    d = d.withColumn('start_year', start_info.getItem(0)) \
        .withColumn('start_month', start_info.getItem(1)) \
        .withColumn('start_day', start_info.getItem(2).substr(1, 2))

    stop_info = split(d["stoptime"], "-")
    d = d.withColumn('stop_year', stop_info.getItem(0)) \
        .withColumn('stop_month', stop_info.getItem(1)) \
        .withColumn('stop_day', stop_info.getItem(2).substr(1, 2))

    d = d.dropDuplicates()  # just in case...
    d.registerTempTable("citibike")

    # first group by day, and write to disk
    query_amount_trips = "SELECT concat(start_year, '-', start_month, '-', start_day) as date, " \
                         "count(*) as amount_trips " \
                         "FROM citibike " \
                         "GROUP BY start_year, start_month, start_day " \
                         "ORDER BY date"

    d_grouped_amount_trips = spark.sql(query_amount_trips)
    d_grouped_amount_trips.printSchema()

    query_sum_travelsecs = "SELECT concat(start_year, '-', start_month, '-', start_day) as date, " \
                           "sum(tripduration) / 3600 as sum_tripduration " \
                           "FROM citibike " \
                           "GROUP BY start_year, start_month, start_day " \
                           "ORDER BY date"
    # we divide the sum by 3600 to get the value in hours, not seconds

    d_grouped_sum_travelsecs = spark.sql(query_sum_travelsecs)
    d_grouped_sum_travelsecs.printSchema()

    d_grouped_amount_trips.coalesce(1).write.csv(folder + "/citibike-daily-counttrips.csv")
    d_grouped_sum_travelsecs.coalesce(1).write.csv(folder + "/citibike-daily-sumtriphours.csv")

    # now we group by month
    query_amount_trips_bymonth = "SELECT concat(start_year, '-', start_month) as date, " \
                                 "count(*) as amount_trips " \
                                 "FROM citibike " \
                                 "GROUP BY start_year, start_month " \
                                 "ORDER BY date"

    d_grouped_amount_trips_month = spark.sql(query_amount_trips_bymonth)
    d_grouped_amount_trips_month.printSchema()

    query_sum_travelsecs_bymonth = "SELECT concat(start_year, '-', start_month) as date, " \
                                   "sum(tripduration) / 3600 as sum_tripduration " \
                                   "FROM citibike " \
                                   "GROUP BY start_year, start_month " \
                                   "ORDER BY date"
    # we divide the sum by 3600 to get the value in hours, not seconds

    d_grouped_sum_travelsecs_month = spark.sql(query_sum_travelsecs_bymonth)
    d_grouped_sum_travelsecs_month.printSchema()

    d_grouped_amount_trips_month.coalesce(1).write.csv(folder + "/citibike-monthly-counttrips.csv")
    d_grouped_sum_travelsecs_month.coalesce(1).write.csv(folder + "/citibike-monthly-sumtriphours.csv")


def clean_and_merge_tsa(spark, folder):

    extension = "/TSA_Daily_Travelers_Comparison.csv"

    # no need to clean; I scraped the .csv myself from the TSA website

    d = spark.read.csv(folder +  extension, header=True, inferSchema=True)  # we can read all at once!

    d = d.withColumnRenamed("Total Traveler Throughput", "2020_traveler_count") \
        .withColumnRenamed("Total Traveler Throughput (1 Year Ago - Same Weekday)", "2019_traveler_count") \
        .withColumnRenamed("Date", "date")

    date_info = split(d["date"], "/")
    d = d.withColumn('month', date_info.getItem(0)) \
        .withColumn('day', date_info.getItem(1)) \
        .withColumn('year', date_info.getItem(2))

    d.registerTempTable("tsa_daily")
    query_date_2019 = "SELECT month, day, '2019' as year, 2019_traveler_count  as traveler_count " \
                      "FROM tsa_daily "

    prev_year = spark.sql(query_date_2019)

    query_date_2020 = "SELECT month, day, '2020' as year, 2020_traveler_count as traveler_count " \
                      "FROM tsa_daily "
    curr_year = spark.sql(query_date_2020)

    d_union = curr_year.union(prev_year)

    d_union.registerTempTable("tsa_union")
    query_format_date = "SELECT concat(year, '-', month, '-', day) as date, traveler_count " \
                        "FROM tsa_union " \
                        "ORDER BY date"

    d_final = spark.sql(query_format_date)

    d_final.coalesce(1).write.csv(folder + "/tsa-daily-travelers.csv")


def clean_and_merge_bridges_tunnels(spark, folder):
    extension = "/Daily_Traffic_on_MTA_Bridges___Tunnels.csv"
    bridges = spark.read.csv(folder + extension, header=True, inferSchema=True)

    bridges.show()

    bridges = bridges.withColumnRenamed("Date", "date")

    date_info = split(bridges["date"], "/")
    bridges = bridges.withColumn('month', date_info.getItem(0)) \
                     .withColumn('day', date_info.getItem(1)) \
                     .withColumn('year', date_info.getItem(2))

    bridges.registerTempTable("bridges")
    query_volume = "SELECT concat(year, '-', month, '-', day) as date, " \
                   "( sum(ezpass_amounts) + sum(toll_amounts) ) as total_traffic " \
                   "FROM bridges WHERE year >= '2019' " \
                   "GROUP BY year, month, day " \
                   "ORDER BY year, month, day"

    volume_per_day = spark.sql(query_volume)
    volume_per_day.coalesce(1).write.csv(folder + "/bridges-tunnels-daily-sumtraffic.csv")

    query_volume_month = "SELECT concat(year, '-', month) as date, " \
                   "( sum(ezpass_amounts) + sum(toll_amounts) ) as total_traffic " \
                   "FROM bridges WHERE year >= '2019' " \
                   "GROUP BY year, month " \
                   "ORDER BY date"

    volume_per_month = spark.sql(query_volume_month)
    volume_per_month.coalesce(1).write.csv(folder + "/bridges-tunnels-monthly-sumtraffic.csv")

if __name__ == "__main__":

    print(sys.argv)
    if len(sys.argv) < 2:
        print("Folder of CitiBike data not passed, exiting...")
        exit(1)

    spark = SparkSession \
        .builder \
        .appName("MergeCleanCitiBridgesTSA-sql") \
        .config("spark.some.config.option", "some-value") \
        .getOrCreate()
    #print("We're in spark territory now people")

    clean_and_merge_citi(spark, sys.argv[1])

    clean_and_merge_tsa(spark, sys.argv[1])

    clean_and_merge_bridges_tunnels(spark, sys.argv[1])

