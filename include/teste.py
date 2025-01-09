from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("TesteDosDados").getOrCreate()

# Aqui nos estamos testando se o arquivo é lido corretamente, e aparentemente está tudo certo
csv_file_path = '/opt/bitnami/spark/jobs/dados.csv'
df = spark.read.csv(csv_file_path, header=True, inferSchema=True)

df.show()

spark.stop()
