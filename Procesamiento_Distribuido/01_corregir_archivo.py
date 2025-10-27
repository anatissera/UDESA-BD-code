from pyspark import SparkContext, SparkConf

#################################################
# Modo 1: Usamos la conexión en modo client (con el SparkContext en nuestra máquina)

# Creamos un contexto de Spark
sc = SparkContext('local', 'test')

# Nos conectamos al cluster HDFS
archivo = sc.textFile("hdfs://namenode:8020/recorridos_realizados_2024_corregido.csv")
#################################################


#################################################
# Modo 2 (alternativo): Usar el SparkContext en YARN. Requiere tener python3+pyspark en todos los nodos de HDFS, cosa que no tenemos
#
# Creamos un contexto de Spark
# sc = SparkContext(master='yarn')
#
# Nos conectamos al cluster HDFS
# archivo = sc.textFile("/recorridos_realizados_2024.csv")
#################################################


# Ejecutamos la tarea, en este caso contar la cantidad de líneas del archivo
archivo.count()

