"""
    Spark Session Initialization file
"""
from pyspark.sql import SparkSession
from app.logger import logs

# create the spark session from builder
logs.info("====Spark session initialization====")

spark = SparkSession.builder.appName("PySparkAssignment").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

logs.info("====Spark session Created====")
