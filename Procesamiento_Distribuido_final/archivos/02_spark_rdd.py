from pyspark import SparkContext, SparkConf

INPUT = "/tmp/books_ratings_corregido.csv"

# Creamos un contexto de Spark
sc = SparkContext('local[*]', 'test')

# Nos conectamos al cluster HDFS, obteniendo un RDD
rdd_archivo = sc.textFile(INPUT)

    
""" 
Escribí un pipeline en Spark RDD que calcule el puntaje más frecuente (es decir, la moda) para cada libro, 
devolviendo tuplas que contengan (id_libro, título_libro, puntaje_más_frecuente). 
En caso de empate, podés devolver sólamente uno de los puntajes más frecuentes.
"""

# id = campos[0]
# title = campos[1] 
# price = campos[2]
# user_id = campos[3]
# profile_name = campos[4]
# review_helpfulness = campos[5]
# review_score = campos[6]
# review_time = campos[7]
# review_summary = campos[8]
# review_text = campos[9]
# review_date = campos[10]

def map1(linea : str):
    campos = linea.split(";") # lista de cada elemento del csv
    
    id = campos[0]
    title = campos[1] 
    price = campos[2]
    user_id = campos[3]
    profile_name = campos[4]
    review_helpfulness = campos[5]
    review_score = campos[6]
    review_time = campos[7]
    review_summary = campos[8]
    review_text = campos[9]
    review_date = campos[10]
    
    return ((id, title, review_score), 1)

rdd_map1 = rdd_archivo.map(map1)
print('\n1')
for x in rdd_map1.take(10):
    print(x)

rdd_reduced1 = rdd_map1.reduceByKey(lambda a, b: a + b)

print('\n2')
for x in rdd_reduced1.take(10):
    print(x)

def map2(x: tuple):
    (id, title, review_score), cant_veces_aparece = x
    return ((id, title), (review_score, cant_veces_aparece))

rdd_map2 = rdd_reduced1.map(map2)
print('\n3')
for x in rdd_map2.take(10):
    print(x)

def reduce2(a, b):
    if a[1] > b[1]:
        return (a[0], a[1])
    else:
        return (b[0], a[1])
    
rdd_reduced2 = rdd_map2.reduceByKey(reduce2)
print('\n4')
for x in rdd_reduced2.take(10):
    print(x)

def map3(x: tuple):
    (id, title), (review_score_mas_frecuente, max_cant_veces_aparece) = x
    return (id, title, review_score_mas_frecuente)

rdd_map3 = rdd_reduced2.map(map3)

print('\n5')
for x in rdd_map3.take(10):
    print(x)