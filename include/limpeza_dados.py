from pyspark.sql import SparkSession
from datetime import datetime

spark = SparkSession.builder.appName("LimpandoDados").getOrCreate()
df = spark.read.csv('/opt/bitnami/spark/jobs/dados.csv', header=True, inferSchema=True)


df = df.dropDuplicates()
df = df.dropna()
df = df.filter(df.amount > 0)
df = df.filter(col("amount").cast("double").isNotNull())

df.write.csv("/opt/bitnami/spark/jobs/dados_limpos.csv")

spark.stop()
