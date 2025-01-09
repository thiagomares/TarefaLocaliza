from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, when
from datetime import datetime
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType

spark = SparkSession.builder.appName("Transacoes").getOrCreate()

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
    Aqui eu ainda estou usando o Spark temp view para conseguir fazer a consulta de forma mais facil, e nesse caso, eu preciso ainda coletar as primeiras 3 vendas na data mais recente
"""

df.createOrReplaceTempView("dados_limpos")

dados = spark.sql("""
    select
        cast(timestamp as date) as data,
        amount,
        receiving_address
    from dados_limpos
    where 
        timestamp = (select max(timestamp) from dados_limpos)
        and transaction_type = 'sale'
    order by timestamp, amount desc
    limit 3
""")

dados.write.parquet("/opt/bitnami/spark/jobs/transactions.parquet").mode("overwrite").save()

spark.stop()

