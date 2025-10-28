from pyspark import SparkContext, SparkConf

# Creamos un contexto de Spark
sc = SparkContext('local', 'test')

# Abrimos el archivo
archivo = sc.textFile("/tmp/recorridos_realizados_2024_corregido.csv")

# Ejecutamos la tarea, en este caso contar la cantidad de líneas del archivo
archivo.count()

