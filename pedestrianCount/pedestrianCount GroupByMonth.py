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

# 2017 data
table2017 = spark.sql("\
    select date, count \
    from t \
    where right(date, 2) = '17")

# 2018 data
table2018 = spark.sql("\
    select date, count \
    from t \
    where right(date, 2) = '18")

# 2019 data
table2019 = spark.sql("\
    select date, count \
    from t \
    where right(date, 2) = '19")

# 2020 data
table2020 = spark.sql("\
    select date, count \
    from t \
    where right(date, 2) = '20")

# output format: 10/25/19 3000 2019
table2017.createOrReplaceTempView("table2017")
table2018.createOrReplaceTempView("table2018")
table2019.createOrReplaceTempView("table2019")
table2020.createOrReplaceTempView("table2020")

# get the data group by month in each year
month2017 = spark.sql("\
    select left(date, instr(date, '/') - 1) as month, sum(count) as counts, '2017' as year \
    from table2017 \
    group by month")

month2018 = spark.sql("\
    select left(date, instr(date, '/') - 1) as month, sum(count) as counts, '2018' as year \
    from table2018 \
    group by month")

month2019 = spark.sql("\
    select left(date, instr(date, '/') - 1) as month, sum(count) as counts, '2019' as year \
    from table2019 \
    group by month")

month2020 = spark.sql("\
    select left(date, instr(date, '/') - 1) as month, sum(count) as counts, '2020' as year \
    from table2020 \
    group by month")

# output format: 1 3000 2019 (month, counts, year)
month2017.createOrReplaceTempView("month2017")
month2018.createOrReplaceTempView("month2018")
month2019.createOrReplaceTempView("month2019")
month2020.createOrReplaceTempView("month2020")

result = spark.sql("\
    select month, counts, year from month2017 \
    union \
    select month, counts, year from month2018 \
    union \
    select month, counts, year from month2019 \
    union \
    select month, counts, year from month2020 \
    order by year, month \
    ")

result.createOrReplaceTempView("result")
result = spark.sql("select * from result")

result.select(format_string('%s-%s: %s',result.year, result.month, result.counts)).write.save("result.out", format = "text")
