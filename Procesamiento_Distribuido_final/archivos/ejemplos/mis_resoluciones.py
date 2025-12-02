
from pyspark import SparkContext, SparkConf

INPUT = "/tmp/books_ratings_corregido.csv"

# Creamos un contexto de Spark
sc = SparkContext('local[*]', 'test')

# Nos conectamos al cluster HDFS, obteniendo un RDD
rdd_archivo = sc.textFile(INPUT)

# 1. Cantidad de reseñas por libro
def map_rpl(linea : str):
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
    return ((id, title), 1)

rdd_map_rpl = rdd_archivo.map(map_rpl)

rdd_reduce_rpl = rdd_map_rpl.reduceByKey(lambda a, b: a + b)

    
# 2. Promedio de puntaje por libro
def map_ppl(linea : str):
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
    return ((id, title), (review_score, 1))

def red_ppl(x: tuple):
    (id, title), (review_score, cant) = x

def reduce_ppl(a, b):
    return (a[0] + b[0], a[1] + b[1])

rdd_map_ppl = rdd_archivo.map(map_ppl)
rdd_reduce_ppl = rdd_map_ppl.reduceByKey(reduce_ppl)

def map_promedio(x: tuple):
    (id, title), (sum_review_score, cant) = x
    return ((id, title), (sum_review_score/x))

rdd_map_ppl2 = rdd_reduce_ppl.reduceByKey(map_promedio)

# 3. Cantidad de reviews por mes
def map_month(linea : str):
    campos = linea.split(";")
    try:
        m = int(campos[10].split("-")[1])
    except:
        m = None
    return (m, 1)

rdd3 = rdd_archivo.map(map_month).reduceByKey(lambda a, b: a + b)