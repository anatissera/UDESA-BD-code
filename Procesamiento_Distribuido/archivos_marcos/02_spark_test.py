from pyspark import SparkContext, SparkConf

# Creamos un contexto de Spark
# sc = SparkContext('local', 'test')
sc = SparkContext('local[*]', 'test')
 # el asterisco usa todos los nucelos disponibles, sino le pongo el numero que quiero

# Abrimos el archivo
archivo = sc.textFile("/tmp/recorridos_realizados_2024_corregido.csv")

# Ejecutamos la tarea, en este caso contar la cantidad de líneas del archivo
# archivo.count()
print(archivo.count())



# la interfaz de red es http://localhost:4040/