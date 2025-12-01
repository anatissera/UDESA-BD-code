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
    fecha_orig = campos[2][:10]
    
    extraccion = ((id_orig, nom_orig, fecha_orig), 1)

    return [extraccion]

rdd_map1 = rdd_archivo.flatMap(map1)
rdd_reduce1 = rdd_map1.reduceByKey(lambda v1, v2: v1+v2) # (key (id, fecha), value: cant))
# quiero key (id) value (fecha, cant)

print("\n reduced 1")
for x in rdd_reduce1.take(10):
    print(x)

# como la key es distinta a lo que necesitamos hago un map


def map2(x: tuple):
    (id_orig, nom_orig, fecha_orig), cant = x
    return [(id_orig, nom_orig), (fecha_orig, cant)]
    
rdd_map2 = rdd_reduce1.flatMap(map2)

print("\n reduced and map 2")
for x in rdd_map2.take(10):
    print(x)
# ahora ya tengo la forma y me falta el max

# rdd = rdd_map2.reduceByKey(lambda v1, v2 : v1 if v1[1] > v2[1] else v2)


# rdd_reduce1.collect()