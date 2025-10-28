from pyspark import SparkContext, SparkConf

# Creamos un contexto de Spark
sc = SparkContext('local', 'test')

# Nos conectamos al cluster HDFS, obteniendo un RDD
rdd_archivo = sc.textFile("hdfs://namenode:8020/recorridos_realizados_2024_corregido.csv")

# Calcular para cada estación y cada día la cantidad de extracciones y de devoluciones de bicicletas que se hicieron en esa estación en ese día. El resultado del pipeline deberán ser tuplas de la forma (id_estacion, nombre_estacion, fecha, cantidad_extracciones, cantidad_devoluciones)

# Recordar que la estructura del .csv es:
# (id_recorrido, duracion_recorrido, fecha_origen_recorrido, id_estacion_origen, nombre_estacion_origen, direccion_estacion_origen, long_estacion_origen, lat_estacion_origen,fecha_destino_recorrido, id_estacion_destino, nombre_estacion_destino, direccion_estacion_destino, long_estacion_destino, lat_estacion_destino, id_usuario, modelo_bicicleta, genero)
