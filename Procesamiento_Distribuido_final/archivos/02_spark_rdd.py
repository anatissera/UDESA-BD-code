from pyspark import SparkContext, SparkConf

INPUT = "/tmp/books_ratings_corregido.csv"

# Creamos un contexto de Spark
sc = SparkContext('local[*]', 'test')

# Nos conectamos al cluster HDFS, obteniendo un RDD
rdd_archivo = sc.textFile(INPUT)

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
    