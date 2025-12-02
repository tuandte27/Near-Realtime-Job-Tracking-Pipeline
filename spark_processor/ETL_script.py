from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import time

spark = (SparkSession.builder
    .appName("Realtime-ETL")
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0,com.mysql:mysql-connector-j:8.0.33") 
    .getOrCreate())

spark.sparkContext.setLogLevel("WARN")

# ĐỊNH NGHĨA SCHEMA
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

# CALCULATION FUNCTIONS
def calculating_clicks(df):
    df_clicks = df.filter(col("custom_track") == "click")
    df_clicks = df_clicks.fillna(0, subset=["bid", "job_id", "publisher_id", "group_id", "campaign_id"])
    
    clicks_output = df_clicks.groupBy(
        col("job_id"), 
        to_date(col("ts")).alias("dates"), 
        hour(col("ts")).alias("hours"), 
        col("publisher_id"), 
        col("campaign_id"), 
        col("group_id")
    ).agg(
        avg("bid").alias("bid_set"),
        count("*").alias("clicks"),
        sum("bid").alias("spend_hour")
    )
    return clicks_output

def calculating_conversion(df):
    df_conversion = df.filter(col("custom_track") == "conversion")
    df_conversion = df_conversion.fillna(0, subset=["job_id", "publisher_id", "group_id", "campaign_id"])
    
    conversion_output = df_conversion.groupBy(
        col("job_id"), 
        to_date(col("ts")).alias("dates"), 
        hour(col("ts")).alias("hours"), 
        col("publisher_id"), 
        col("campaign_id"), 
        col("group_id")
    ).agg(
        count("*").alias("conversions")
    )
    return conversion_output

def calculating_qualified(df):
    df_qualified = df.filter(col("custom_track") == "qualified")
    df_qualified = df_qualified.fillna(0, subset=["job_id", "publisher_id", "group_id", "campaign_id"])
    
    qualified_output = df_qualified.groupBy(
        col("job_id"), 
        to_date(col("ts")).alias("dates"), 
        hour(col("ts")).alias("hours"), 
        col("publisher_id"), 
        col("campaign_id"), 
        col("group_id")
    ).agg(
        count("*").alias("qualified")
    )
    return qualified_output

def calculating_unqualified(df):
    df_unqualified = df.filter(col("custom_track") == "unqualified")
    df_unqualified = df_unqualified.fillna(0, subset=["job_id", "publisher_id", "group_id", "campaign_id"])
    
    unqualified_output = df_unqualified.groupBy(
        col("job_id"), 
        to_date(col("ts")).alias("dates"), 
        hour(col("ts")).alias("hours"), 
        col("publisher_id"), 
        col("campaign_id"), 
        col("group_id")
    ).agg(
        count("*").alias("unqualified")
    )
    return unqualified_output

def process_final_data(clicks_output, conversion_output, qualified_output, unqualified_output):
    final_data = clicks_output.join(conversion_output,['job_id','dates','hours','publisher_id','campaign_id','group_id'],'full')\
        .join(qualified_output,['job_id','dates','hours','publisher_id','campaign_id','group_id'],'full')\
        .join(unqualified_output,['job_id','dates','hours','publisher_id','campaign_id','group_id'],'full')
    return final_data

def process_cassandra_data(df):
    clicks_output = calculating_clicks(df)
    conversion_output = calculating_conversion(df)
    qualified_output = calculating_qualified(df)
    unqualified_output = calculating_unqualified(df)
    final_data = process_final_data(clicks_output, conversion_output, qualified_output, unqualified_output)
    final_data = final_data.fillna(0, subset=["clicks","conversions","qualified","unqualified","bid_set","spend_hour"])
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
                           'unqualified','qualified','conversions','clicks','bid_set','spend_hour')
    output = output.withColumn('updated_at', current_timestamp())
    output = output.withColumnRenamed("qualified", "qualified_application").withColumnRenamed("unqualified", "disqualified_application")
    output = output.withColumnRenamed("conversions", "conversion")
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
        
    print(">>> RAW DATA FROM KAFKA:")
    batch_df.selectExpr("CAST(value AS STRING)").show(1, truncate=False)
    
    df_parsed = batch_df.select(from_json(col("value").cast("string"), schema_json).alias("data")) \
        .select("data.payload.after.*")
    
    df_clean = df_parsed.withColumn("ts", (col("ts") / 1000).cast("timestamp"))
    
    # Xem dữ liệu sau khi Parse
    print(">>> PARSED DATA (Before Filter):")
    df_clean.printSchema()
    df_clean.show(3, truncate=False)
    
    # Kiểm tra xem có bị NULL hết không
    count_total = df_clean.count()
    count_valid = df_clean.filter(col("job_id").isNotNull()).count()
    print(f"📊 Stats: Received {count_total} rows. Valid (Job_ID not null): {count_valid} rows.")

    if count_valid == 0:
        print("❌ ERROR: All data was filtered out! Check Schema or JSON format.")
        return

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
    .trigger(processingTime='1 seconds') \
    .start()

query.awaitTermination()