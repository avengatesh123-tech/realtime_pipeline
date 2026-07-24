import socketserver
import sys
if not hasattr(socketserver, "UnixStreamServer"):
    socketserver.UnixStreamServer = socketserver.TCPServer
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StringType, IntegerType
wsl_ip = "172.18.18.28"  
mysql_pw = "2705"        
spark = SparkSession.builder \
    .appName("KafkaToMySQL") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.13:3.5.0") \
    .getOrCreate()
spark.sparkContext.setLogLevel("WARN")

try:
    df = spark.readStream.format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "order_topic") \
        .option("startingOffsets", "earliest") \
        .load()
    schema = StructType() \
        .add("product_name", StringType()) \
        .add("price", IntegerType())
    processed_df = df.selectExpr("CAST(value AS STRING)") \
        .select(from_json(col("value"), schema).alias("data")) \
        .select("data.*")

def write_to_mysql(target_df, batch_id):
        if target_df.count() > 0:
            print(f"Processing Batch ID: {batch_id}")
            try:
                target_df.write.format("jdbc") \
                    .option("url", f"jdbc:mysql://{wsl_ip}:3306/sales_db") \
                    .option("driver", "com.mysql.cj.jdbc.Driver") \
                    .option("dbtable", "orders") \
                    .option("user", "root") \
                    .option("password", mysql_pw) \
                    .mode("append").save()
                print(f"Batch {batch_id} successfully saved to MySQL!")
            except Exception as e:
                print(f" Error saving to MySQL: {e}")
        else:
    query = processed_df.writeStream \
        .foreachBatch(write_to_mysql) \
        .start()

    query.awaitTermination()

except Exception as e:
    print(f" Fatal Error: {e}")
