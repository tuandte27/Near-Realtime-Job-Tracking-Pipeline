from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import time

spark = (SparkSession.builder
    .appName("Realtime-ETL")
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0,com.mysql:mysql-connector-j:8.0.33") 
    .getOrCreate())

spark.sparkContext.setLogLevel("WARN")

schema_data_row_full = StructType([
    StructField("create_time", StringType()),
    StructField("bid", FloatType()),
    StructField("bn", StringType()),
    StructField("campaign_id", IntegerType()),
    StructField("cd", IntegerType()),
    StructField("custom_track", StringType()),
    StructField("de", StringType()),
    StructField("dl", StringType()),
    StructField("dt", StringType()),
    StructField("ed", StringType()),
    StructField("ev", IntegerType()),
    StructField("group_id", IntegerType()),
    StructField("id", StringType()),
    StructField("job_id", IntegerType()),
    StructField("md", StringType()),
    StructField("publisher_id", IntegerType()),
    StructField("rl", StringType()),
    StructField("sr", StringType()),
    StructField("ts", LongType()), 
    StructField("tz", IntegerType()),
    StructField("ua", StringType()),
    StructField("uid", StringType()),
    StructField("utm_campaign", StringType()),
    StructField("utm_content", StringType()),
    StructField("utm_medium", StringType()),
    StructField("utm_source", StringType()),
    StructField("utm_term", StringType()),
    StructField("v", IntegerType()),
    StructField("vp", StringType())
])

schema_json = StructType([
    StructField("payload", StructType([
        StructField("after", schema_data_row_full),
        StructField("op", StringType())
    ]))
])

def process_cassandra_data(df):
    df = df.fillna(0, subset=["job_id", "publisher_id", "group_id", "campaign_id"])
    
    final_data = df.groupBy(
        col("job_id"), 
        to_date(col("ts")).alias("dates"), 
        hour(col("ts")).alias("hours"), 
        col("publisher_id"), 
        col("campaign_id"), 
        col("group_id")
    ).agg(
        # Tính clicks
        sum(when(col("custom_track") == "click", 1).otherwise(0)).alias("clicks"),
        avg(when(col("custom_track") == "click", col("bid")).otherwise(None)).alias("bid_set"),
        sum(when(col("custom_track") == "click", col("bid")).otherwise(0)).alias("spend_hour"),
        
        # Tính conversion
        sum(when(col("custom_track") == "conversion", 1).otherwise(0)).alias("conversion"),
        
        # Tính qualified
        sum(when(col("custom_track") == "qualified", 1).otherwise(0)).alias("qualified_application"),
        
        # Tính unqualified
        sum(when(col("custom_track") == "unqualified", 1).otherwise(0)).alias("disqualified_application")
    )
    
    return final_data

def retrieve_company_data():
    sql = """(SELECT job_id, company_id, group_id, campaign_id FROM job) job_table"""
    company = spark.read.format("jdbc") \
        .option("driver","com.mysql.cj.jdbc.Driver") \
        .option("url", "jdbc:mysql://mysql:3306/project_db")\
        .option("dbtable", sql) \
        .option("user", "root") \
        .option("password", "123") \
        .load()
    return company

def import_to_mysql(output):
    output = output.select('job_id','dates','hours','publisher_id','campaign_id','company_id','group_id',
                           'disqualified_application','qualified_application','conversion','clicks','bid_set','spend_hour')
    output = output.withColumn('updated_at', current_timestamp())
    output.write.format("jdbc") \
        .option("driver","com.mysql.cj.jdbc.Driver") \
        .option("url", "jdbc:mysql://mysql:3306/project_db") \
        .option("dbtable", "events") \
        .mode("append") \
        .option("user", "root") \
        .option("password", "123") \
        .save()
    print('Data imported successfully')

def main_etl(batch_df, batch_id):
    print(f"--- Processing Batch ID: {batch_id} ---")
    
    if batch_df.isEmpty():
        print("⚠️ Batch is empty (No new data from Kafka).")
        return
    
    df_parsed = batch_df.select(from_json(col("value").cast("string"), schema_json).alias("data")) \
        .select("data.payload.after.*")
    
    df_clean = df_parsed.withColumn("ts", (col("ts") / 1000).cast("timestamp"))

    df_filtered = df_clean.filter(col("job_id").isNotNull())
    df_filtered.persist()
    
    try:
        print(">>> Starting processing logic...")

        cassandra_output = process_cassandra_data(df_filtered)
        company = retrieve_company_data()
        final_output = cassandra_output.join(company, ['job_id'], 'left') \
            .drop(company.group_id).drop(company.campaign_id)
        
        # Ghi xuống MySQL
        import_to_mysql(final_output)
        print("✅ Successfully wrote to MySQL.")
        
    except Exception as e:
        print(f"❌ CRITICAL ERROR in Logic/MySQL: {e}")
    
    df_filtered.unpersist()
    print("Batch finished.")

# MAIN STREAM
print("Starting Streaming Pipeline...")

kafka_stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "broker:29092") \
    .option("subscribe", "cassandra_cdc.cassandra_data.tracking") \
    .option("startingOffsets", "earliest") \
    .load()

query = kafka_stream.writeStream \
    .foreachBatch(main_etl) \
    .start()

query.awaitTermination()