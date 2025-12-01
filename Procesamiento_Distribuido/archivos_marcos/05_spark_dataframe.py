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


# (id_recorrido, duracion_recorrido, fecha_origen_recorrido, id_estacion_origen, nombre_estacion_origen, direccion_estacion_origen, long_estacion_origen, lat_estacion_origen,fecha_destino_recorrido, id_estacion_destino, nombre_estacion_destino, direccion_estacion_destino, long_estacion_destino, lat_estacion_destino, id_usuario, modelo_bicicleta, genero)
# quiero
# id_estacion_origen, nombre_estacion_origen, fecha_origen_recorrido, cantidad

query = """
WITH extracciones AS (
    SELECT id_estacion_origen as id, nombre_estacion_origen, SUBSTR(fecha_origen_recorrido, 0, 10) as fecha, COUNT(*) as cant_extracciones
    FROM bicis
    GROUP BY id_estacion_origen, nombre_estacion_origen, fecha
),
devoluciones AS (
    SELECT id_estacion_destino as id, nombre_estacion_destino, SUBSTR(fecha_destino_recorrido, 0, 10) as fecha, COUNT(*) as cant_devoluciones
    FROM bicis
    GROUP BY id_estacion_destino, nombre_estacion_destino, fecha
)
SELECT *
FROM extracciones e INNER JOIN devoluciones d ON (e.id = d.id AND e.fecha = d.fecha)
"""

spark.sql(query).show()


