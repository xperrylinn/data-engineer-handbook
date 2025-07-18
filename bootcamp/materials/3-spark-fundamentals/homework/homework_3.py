from pyspark.sql import SparkSession
from pyspark.sql.functions import expr, col, lit
spark = SparkSession.builder.appName("Jupyter").getOrCreate()

spark

events = spark.read.option("header", "true").csv("bootcamp/materials/3-spark-fundamentals/data/events.csv").withColumn("event_date", expr("DATE_TRUNC('day', event_time)"))
devices = spark.read.option("header","true").csv("bootcamp/materials/3-spark-fundamentals/data/devices.csv")

df = events.join(devices,on="device_id",how="left")
df = df.withColumnsRenamed({'browser_type': 'browser_family', 'os_type': 'os_family'})

df.join(df, lit(1) == lit(1)).take(5)

print(df.show(5))