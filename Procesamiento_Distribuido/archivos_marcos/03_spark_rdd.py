from pyspark import SparkContext, SparkConf

# Creamos un contexto de Spark
sc = SparkContext('local', 'test')

# Nos conectamos al cluster HDFS, obteniendo un RDD
rdd_archivo = sc.textFile("/tmp/recorridos_realizados_2024_corregido.csv")

# Calcular para cada estación y cada día la cantidad de extracciones y de devoluciones de bicicletas que se hicieron en esa estación en ese día. El resultado del pipeline deberán ser tuplas de la forma (id_estacion, nombre_estacion, fecha, cantidad_extracciones, cantidad_devoluciones)

# Recordar que la estructura del .csv es:
# (id_recorrido, duracion_recorrido, fecha_origen_recorrido, id_estacion_origen, nombre_estacion_origen, direccion_estacion_origen, long_estacion_origen, lat_estacion_origen,fecha_destino_recorrido, id_estacion_destino, nombre_estacion_destino, direccion_estacion_destino, long_estacion_destino, lat_estacion_destino, id_usuario, modelo_bicicleta, genero)

def map1(linea : str):
    campos = linea.split(";") # lista de cada elemento del csv
    
    id_orig = campos[3]
    nom_orig = campos[4] 
    id_dest = campos[9]
    nom_dest = campos[10]
    fecha_orig = campos[2][:10]
    fecha_dest = campos[8][:10]
    
    extraccion = ((id_orig, nom_orig, fecha_orig), (1, 0))
    devolucion = ((id_dest, nom_dest, fecha_dest), (0, 1))
    return [extraccion, devolucion]

rdd_map1 = rdd_archivo.flatMap(map1)
rdd_reduce1 = rdd_map1.reduceByKey(lambda x, y: (x[0]+y[0], x[1]+y[1]))
# a reduce by key le paso 2 valores y los junta

rdd_reduce1.collect()