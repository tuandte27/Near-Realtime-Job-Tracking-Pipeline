from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import datetime
import time

spark = (
    SparkSession.builder
    .config("spark.jars", "/opt/jars/spark-cassandra-connector-assembly_2.12-3.4.0.jar")
    .getOrCreate()
)

def calculating_clicks(df):
    df_clicks = df.filter(col("custom_track")=="click")
    df_clicks = df_clicks.fillna(0, subset=["bid", "job_id", "publisher_id", "group_id", "campaign_id"])
    df_clicks.createOrReplaceTempView('clicks')
    clicks_output = spark.sql("""
        select job_id,
               date(ts) as dates,
               hour(ts) as hours,
               publisher_id,
               campaign_id,
               group_id,
               avg(bid) as bid_set,
               count(*) as clicks,
               sum(bid) as spend_hour
        from clicks
        group by job_id, date(ts), hour(ts), publisher_id, campaign_id, group_id
    """)
    return clicks_output

def calculating_conversion(df):
    df_conversion = df.filter(col("custom_track")=='conversion')
    df_conversion = df_conversion.fillna(0, subset=["job_id", "publisher_id", "group_id", "campaign_id"])
    df_conversion.createOrReplaceTempView('conversion')
    conversion_output = spark.sql("""
        select job_id,
               date(ts) as dates,
               hour(ts) as hours,
               publisher_id,
               campaign_id,
               group_id,
               count(*) as conversions
        from conversion
        group by job_id, date(ts), hour(ts), publisher_id, campaign_id, group_id
    """)
    return conversion_output

def calculating_qualified(df):
    df_qualified = df.filter(col("custom_track")=="qualified")
    df_qualified = df_qualified.fillna(0, subset=["job_id", "publisher_id", "group_id", "campaign_id"])
    df_qualified.createOrReplaceTempView("qualified")
    qualified_output = spark.sql("""
        select job_id,
               date(ts) as dates,
               hour(ts) as hours,
               publisher_id,
               campaign_id,
               group_id,
               count(*) as qualified
        from qualified
        group by job_id, date(ts), hour(ts), publisher_id, campaign_id, group_id
    """)
    return qualified_output

def calculating_unqualified(df):
    df_unqualified = df.filter(col("custom_track")=="unqualified")
    df_unqualified = df_unqualified.fillna(0, subset=["job_id", "publisher_id", "group_id", "campaign_id"])
    df_unqualified.createOrReplaceTempView("unqualified")
    qualified_output = spark.sql("""
        select job_id,
               date(ts) as dates,
               hour(ts) as hours,
               publisher_id,
               campaign_id,
               group_id,
               count(*) as unqualified
        from unqualified
        group by job_id, date(ts), hour(ts), publisher_id, campaign_id, group_id
    """)
    return qualified_output

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
    return print('Data imported successfully')

def main_task():
    print('-----------------------------')
    print('Retrieving data from Cassandra')
    print('-----------------------------')
    df = spark.read \
    .format("org.apache.spark.sql.cassandra") \
    .option("spark.cassandra.connection.host", "cassandra") \
    .option("keyspace", "cassandra_data") \
    .option("table", "tracking") \
    .load()
    
    print('-----------------------------')
    print('Selecting data from Cassandra')
    print('-----------------------------')
    df = df.select('ts','job_id','custom_track','bid','campaign_id','group_id','publisher_id')
    df = df.filter(df.job_id.isNotNull())
    
    print('-----------------------------')
    print('Processing Cassandra Output')
    print('-----------------------------')
    cassandra_output = process_cassandra_data(df)
    
    print('-----------------------------')
    print('Merge Company Data')
    print('-----------------------------')
    company = retrieve_company_data()
    
    print('-----------------------------')
    print('Finalizing Output')
    print('-----------------------------')
    final_output = cassandra_output.join(company, ['job_id'], 'left').drop(company.group_id).drop(company.campaign_id)
    final_output.show()
    print(final_output.count())
    
    print('-----------------------------')
    print('Import Output to MySQL')
    print('-----------------------------')
    import_to_mysql(final_output)
    
    return print('Task Finished')

def get_lastest_time_cassandra():
    df = spark.read \
    .format("org.apache.spark.sql.cassandra") \
    .option("spark.cassandra.connection.host", "cassandra") \
    .option("keyspace", "cassandra_data") \
    .option("table", "tracking") \
    .load()
    df = df.select('ts')
    df = df.orderBy(col('ts').desc())
    latest_time = df.first()['ts']
    return latest_time

def get_lastest_time_mysql():
    sql = """(SELECT max(updated_at) FROM events) events_table"""
    mysql_time = spark.read.format("jdbc") \
        .option("driver","com.mysql.cj.jdbc.Driver") \
        .option("url", "jdbc:mysql://mysql:3306/project_db") \
        .option("dbtable", sql) \
        .option("user", "root") \
        .option("password", "123") \
        .load()
    mysql_time = mysql_time.take(1)[0][0]
    if mysql_time is None:
        mysql_latest = '1998-01-01 23:59:59'
    else:
        mysql_latest = mysql_time.strftime('%Y-%m-%d %H:%M:%S')
    return mysql_latest

while True:
    start_time = datetime.datetime.now()
    cassandra_time = get_lastest_time_cassandra()
    print('Cassandra latest time is {}'.format(cassandra_time))
    
    mysql_time = get_lastest_time_mysql()
    print('MySQL latest time is {}'.format(mysql_time))
    
    if cassandra_time > mysql_time:
        print('New data found. Starting ETL process...')
        main_task()
    else:
        print('No new data found.')
    
    end_time = datetime.datetime.now()
    execution_time = (end_time - start_time).total_seconds()
    print('Execution time: {} seconds'.format(execution_time))
    time.sleep(10)
