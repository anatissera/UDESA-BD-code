from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession, functions

spark = SparkSession.builder.master('local') \
    .appName('test') \
    .getOrCreate()

# Extraemos el esquema
esquema = spark.read.parquet("/tmp/bicis/1/bicis0.parquet").schema

# Nos conectamos al dataset
df = spark.read.option("recursiveFileLookup","true").load("/tmp/bicis/", format='parquet', schema=esquema)

# Utilizamos Spark SQL, que nos brinda un DataFrame con dos formas de realizar consultas:
# - Utilizando SQL: https://spark.apache.org/docs/3.5.2/sql-getting-started.html#running-sql-queries-programmatically
# - Utilizando métodos propios de DataFrames: https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/dataframe.html

# agrego:
df.createOrReplaceTempView("bicis")


query = """
WITH extracciones AS (
    SELECT id_estacion_origen as id, nombre_estacion_origen, SUBSTR(fecha_origen_recorrido, 0, 10) as fecha, COUNT(*) as cant_extracciones
    FROM bicis
    GROUP BY id_estacion_origen, nombre_estacion_origen, fecha
)
SELECT *
FROM extracciones e
WHERE NOT EXISTS (
    SELECT *
    FROM extracciones e2
    WHERE e.id = e2.id AND e2.cant_extracciones > e.cant_extracciones
)

"""

spark.sql(query).show()