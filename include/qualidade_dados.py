
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, when
from datetime import datetime

def insert_quality_metrics():

    # nota mental: O docker poderia ter uma forma melhor de lidar com diretórios compartilhados, seria muito mais útil coletar esses dados diretamente da fonte dele do que deixar esses dados sendo baixados e enviados para o container, talvez tenha alguma forma de lidar com isso

    """
        Voltando ao assunto, algumas ideias aqui foram tiradas diretamente do arquivo que foi enviado.

        A ideia é fazer uma análise de qualidade dos dados, e para isso, eu vou fazer uma análise de dados nulos, registros duplicados, registros inválidos e registros válidos.
        Estes dados são extremamente críticos para o dia a dia do analista de dados por alguns motivos:

        1- Dados nulos podem ser um problema para o analista de dados, pois ele pode não ter a informação necessária para fazer a análise correta, fora isso, um dado nulo, por mais que ele não tenha valor, ele ainda tem informações uteis

        2- Registros duplicados tem por natureza serem outliers, pois eles criam a ideia de que um cliente, por exemplo, pode estar fazendo multiplas compras, ou que um produto está sendo vendido mais de uma vez, alem disso, quando queremos entender se esse dado é referente a uma fraude, e o dado duplicado pode ser um indicativo disso
    """

    spark = SparkSession.builder.appName("DataQualityMonitoring").getOrCreate()
    df = spark.read.csv('/opt/bitnami/spark/jobs/dados.csv', header=True, inferSchema=True)
    null_counts = df.select([count(when(col(c).isNull(), c)).alias(c) for c in df.columns]).collect()[0].asDict()
    total_records = df.count()
    duplicate_count = df.count() - df.dropDuplicates().count()

    # O Invalid count é um pouco mais complexo, pois eu preciso verificar se o valor é menor que 0 e se o valor é um número, então eu preciso fazer um filtro duplo, primeiro que não existe valor de transferência negativo, e segundo que o valor é um número

    invalid_count = df.filter(col("amount") < 0).count() + df.filter(col("amount").cast("double").isNotNull()).count()
    valid_records = total_records - (sum(null_counts.values()) + duplicate_count + invalid_count)
    conformity_percentage = (valid_records / total_records) * 100

    colunas = ("total_records", "null_counts", "duplicate_count", "invalid_count", "valid_records", "conformity_percentage")
    valores = (total_records, sum(null_counts.values()), duplicate_count, invalid_count, valid_records, conformity_percentage)

    dados = spark.sparkContext.parallelize([valores]).toDF(colunas)


    # A ideia inicial seria exportar esses dados, mas para conveniência, eu vou jogar para um arquivo parquet
    """ dados.write.format("jdbc").options(
        url="jdbc:mysql://mysql:3306/airflow",
        driver="com.mysql.jdbc.Driver",
        dbtable="data_quality_logs",
        user="root",
        password="root_password"
    ).mode("append").save()
    mysql_hook.run(query)"""
    dados.write.parquet("/opt/bitnami/spark/jobs/quality_metrics.parquet").mode("overwrite").save()

    spark.stop()

if __name__ == "__main__":
    insert_quality_metrics()
