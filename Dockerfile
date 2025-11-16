FROM apache/spark:3.4.0

# Copy script ETL và thư viện jars vào container
COPY ETL_script.py /app/ETL_script.py
COPY jars/*.jar /opt/jars/

WORKDIR /app

# Chạy file ETL khi container start, kèm JAR Cassandra connector
CMD ["/opt/spark/bin/spark-submit", "--jars", "/opt/jars/spark-cassandra-connector-assembly_2.12-3.4.0.jar,/opt/jars/mysql-connector-j-8.0.33.jar", "ETL_script.py"]