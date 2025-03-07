import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import count
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, IntegerType

# Load environment variables.
from dotenv import load_dotenv
load_dotenv()

# aws_access_key_id = os.environ.get("AWS_ACCESS_KEY_ID")
# aws_secret_access_key = os.environ.get("AWS_SECRET_ACCESS_KEY")


# Create a SparkSession
spark = SparkSession.builder \
        .appName("EquipmentLossesRvU") \
        .master("local") \
        .getOrCreate()

#For Windows users, quiet errors about not being able to delete temporary directories which make your logs impossible to read...
logger = spark.sparkContext._jvm.org.apache.log4j
logger.LogManager.getLogger("org.apache.spark.util.ShutdownHookManager"). setLevel( logger.Level.OFF )
logger.LogManager.getLogger("org.apache.spark.SparkEnv"). setLevel( logger.Level.ERROR )

#load data

# https://stackoverflow.com/questions/69350586/pyspark-read-multiple-csv-files-at-once

# equipment_losses = spark.read.csv("resources/animal-bites.csv", sep=",", header=True)

# # what_bit = (
#     animal_bites_data.groupBy("ANIMAL_TYPE","WARD")
#     .count()
#     .orderBy("count", ascending=False)
# )
# what_bit.show()

# what_bit.write.parquet("s3a://hwe-fall-2024/ccook/challenge", mode="overwrite")

print("\n -- Writing Data to Output Location -- \n")

## Stop the SparkSession
spark.stop()