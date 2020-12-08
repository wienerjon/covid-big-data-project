import sys
from pyspark.sql.functions import *
from pyspark import SparkContext
from pyspark.sql import SparkSession
from datetime import datetime
from pyspark.sql.types import *

# sc = SparkContext()

spark = SparkSession \
    .builder \
    .appName("Python Spark SQL basic example") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()


data = spark.read.format('csv').options(header='true', inferschema='true').load(sys.argv[1])
data.createOrReplaceTempView("d")

# group by date: _c0 is date, _c2 is pedestrian count
# output example: 10/25/19 3000
table1 = spark.sql("\
    select left(_c0, instr(_c0, ' ') - 1) as date, sum(_c2) as count \
    from d \
    group by date")

table1.createOrReplaceTempView("t")

result = spark.sql("\
	select date_format(t.date, 'yyyy-MM-dd') as dates, t.count as counts\
	from t\
	order by dates")

result.select(format_string('%s, %s',result.dates, result.counts)).write.save("pedestrianCountGroupByDate.out", format = "text")
