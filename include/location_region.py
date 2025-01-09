from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, when
from datetime import datetime
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType

spark = SparkSession.builder.appName("LocationRegion").getOrCreate()

schema = StructType([
    StructField("timestamp", TimestampType(), True),
    StructField("sending_address", StringType(), True),
    StructField("receiving_address", StringType(), True),
    StructField("amount", DoubleType(), True),
    StructField("transaction_type", StringType(), True),
    StructField("location_region", StringType(), True),
    StructField("ip_prefix", StringType(), True),
    StructField("login_frequency", IntegerType(), True),
    StructField("session_duration", DoubleType(), True),
    StructField("purchase_pattern", StringType(), True),
    StructField("age_group", StringType(), True),
    StructField("risk_score", DoubleType(), True),
    StructField("anomaly", StringType(), True)
])


df = spark.read.schema(schema).parquet('/opt/bitnami/spark/jobs/dados_limpos.parquet')

"""
    Qual a ideia aqui: eu vou usar o Spark temp view para conseguir fazer a consulta de forma mais facil, pois eu só preciso fazer uma média de
    risco
"""

df.createOrReplaceTempView("dados_limpos")

dados = spark.sql("""
    select 
        location_region,
        AVG(risk_score) AS avg_risk_score
    from dados_limpos
    group by location_region
    order by AVG(risk_score) DESC
""")

dados.write.parquet("/opt/bitnami/spark/jobs/location_region.parquet").mode("overwrite").save()

spark.stop()

