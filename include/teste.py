from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("CSVDataProcessing").getOrCreate()

# Use o caminho absoluto para o arquivo CSV dentro do contêiner Docker
csv_file_path = '/opt/bitnami/spark/jobs/dados.csv'

df = spark.read.csv(csv_file_path, header=True, inferSchema=True)

df.show()
