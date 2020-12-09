from pyspark.sql import SparkSession
from pyspark.sql.functions import format_string
import sys

spark = SparkSession \
  .builder \
  .appName("GetRidesPerDay-sql") \
  .config("spark.some.config.option", "some-value") \
  .getOrCreate()

toLoad = sys.argv[1]

# table = spark.read.format('csv').options(header='true', inferschema='true').load('/user/jiw232/fhv_tripdata_2019-01.csv')
table = spark.read.format('csv').options(header='true', inferschema='true').load(toLoad)
table.createOrReplaceTempView("table")

# sql = "select * \
#   from table \
#   where not (dispatching_base_num is null or pickup_datetime is null or dropoff_datetime is null or PULocationID is null or DOLocationID is null)"

# cleaned = sparl.sql(sql)
# cleaned.createOrReplaceTempView("cleaned")

sql = "SELECT split(pickup_datetime, ' ')[0] AS date, COUNT(*) AS total_rides \
  FROM table \
  GROUP BY date \
  ORDER BY date"
result = spark.sql(sql)

newName = toLoad.split('.')[0]
newName += '_trips_per_day.csv'

result.select(format_string('%s, %s', result.date, result.total_rides))\
  .write.save(newName,format="text")
