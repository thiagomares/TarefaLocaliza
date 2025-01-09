from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, when
from datetime import datetime

spark = SparkSession.builder.appName("Transaction").getOrCreate()

df = read.parquet('/opt/bitnami/spark/jobs/dados_limpos.parquet')

"""
    Aqui eu ainda estou usando o Spark temp view para conseguir fazer a consulta de forma mais facil, pois eu posso fazer as consultas de forma mais facil,
    e nesse caso, eu preciso ainda coletar as primeiras 3 vendas na data mais recente
"""

df.createOrReplaceTempView("dados_limpos")

df = spark.sql("""
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

df.write.parquet("/opt/bitnami/spark/jobs/transactions.parquet").mode("overwrite").save()

spark.stop()

