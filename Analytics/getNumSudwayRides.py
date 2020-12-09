from pyspark.sql import SparkSession
from pyspark.sql.functions import format_string
import sys

spark = SparkSession \
  .builder \
  .appName("getNumSudwayRidesPerDay-sql") \
  .config("spark.some.config.option", "some-value") \
  .getOrCreate()

toLoad = sys.argv[1]
# table = spark.read.format('csv').options(header='true', inferschema='true').load('/user/jiw232/Turnstile_Usage_Data__2020.csv')
table = spark.read.format('csv').options(header='true', inferschema='true').load(toLoad)
table.createOrReplaceTempView("table")

sql = 'SELECT Unit, SCP, CONCAT(split(Date, "/")[2], "-", split(Date, "/")[0], "-", split(Date, "/")[1]) as D, max(Entries)-min(Entries) AS DailyRides \
  FROM table \
  WHERE Description="REGULAR" \
  GROUP BY Unit, SCP, Date \
  HAVING DailyRides < 1000000 \
  ORDER BY Unit, SCP, Date'

result = spark.sql(sql)
# result.show(400)

result.createOrReplaceTempView("result")

daily = spark.sql("SELECT D, sum(DailyRides) as Rides \
  FROM result \
  GROUP BY D \
  ORDER BY D")

newName = toLoad.split('.')[0]
newName += '_trips_per_day.csv'

daily.select(format_string('%s, %s', daily.D, daily.Rides))\
  .write.save(newName,format="text")

