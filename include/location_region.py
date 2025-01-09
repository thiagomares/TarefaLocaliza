from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, when
from datetime import datetime

spark = SparkSession.builder.appName("LocationRegion").getOrCreate()

df = read.parquet('/opt/bitnami/spark/jobs/dados_limpos.parquet')

"""
    Qual a ideia aqui: eu vou usar o Spark temp view para conseguir fazer a consulta de forma mais facil, pois eu só preciso fazer uma média de
    risco
"""

df.createOrReplaceTempView("dados_limpos")

df = spark.sql("""
    with total as (
        select 
            count(*) as total
            , location_region
        from dados_limpos
        group by location_region
    )

    SELECT 
        location_region
        , count(*) / total.total as total
    FROM dados_limpos
    JOIN total
    ON dados_limpos.location_region = total.location_region
    GROUP BY location_region, total.total
""")

df.write.parquet("/opt/bitnami/spark/jobs/location_region.parquet").mode("overwrite").save()

spark.stop()

