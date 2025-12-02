# =============================================================
# SPARK: Ejercicios 13 a 20 (RDD + DataFrame + SQL)
# Dataset: books_ratings_corregido.csv
# =============================================================

from pyspark import SparkContext
from pyspark.sql import SparkSession, functions as F
from datetime import datetime
import pandas as pd
import re

# Inicializar Spark
sc = SparkContext.getOrCreate()
spark = SparkSession(sc)

# RDD base
rdd = sc.textFile("/tmp/books_ratings_corregido.csv").map(lambda l: l.split(";"))

# DataFrame base
cols = ["id","title","price","user_id","profile_name","review_helpfulness",
        "review_score","review_time","review_summary","review_text","review_date"]

df = spark.read.csv("/tmp/books_ratings_corregido.csv", sep=";", header=False, inferSchema=True).toDF(*cols)

# =============================================================
# 13. Top 5 libros con más reviews
# =============================================================

# --- RDD ---
def map_reviews(row):
    return (row[0], 1)

rdd13 = rdd.map(map_reviews).reduceByKey(lambda a,b: a+b).takeOrdered(5, key=lambda x: -x[1])
print("RDD 13:", rdd13)

# --- DataFrame ---
df13 = df.groupBy("id").count().orderBy(F.col("count").desc()).limit(5)
df13.show()

# --- SQL ---
df.createOrReplaceTempView("books")

query13 = """
SELECT id, COUNT(*) AS reviews
FROM books
GROUP BY id
ORDER BY reviews DESC
LIMIT 5
"""
spark.sql(query13).show()

# =============================================================
# 14. Promedio de score por mes
# =============================================================

# --- RDD ---
def map_score_month(row):
    try:
        m = int(row[10].split("-")[1])
    except:
        m = None
    return (m, (float(row[7]), 1))

rdd14 = rdd.map(map_score_month) \
           .reduceByKey(lambda a,b:(a[0]+b[0], a[1]+b[1])) \
           .map(lambda x:(x[0], x[1][0]/x[1][1]))
print("RDD 14:", rdd14.collect())

# --- DataFrame ---
df14 = df.groupBy(F.month("review_date").alias("mes")).agg(F.avg("review_score").alias("promedio"))
df14.show()

# --- SQL ---
query14 = """
SELECT MONTH(review_date) AS mes,
       AVG(review_score) AS promedio
FROM books
GROUP BY MONTH(review_date)
"""
spark.sql(query14).show()

# =============================================================
# 15. Review más larga por libro
# =============================================================

# --- RDD ---
def map_length(row):
    return (row[0], (len(row[9]), row[9]))

rdd15 = rdd.map(map_length).reduceByKey(lambda a,b: a if a[0] > b[0] else b)
print("RDD 15 sample:", rdd15.take(5))

# --- DataFrame ---
df15 = df.withColumn("largo", F.length("review_text")).groupBy("id").agg(F.max("largo").alias("max_largo"))
df15.show()

# --- SQL ---
query15 = """
SELECT id, MAX(LENGTH(review_text)) AS max_largo
FROM books
GROUP BY id
"""
spark.sql(query15).show()

# =============================================================
# 16. Libros con mayor VARIANZA de score
# =============================================================

# --- RDD ---
def map_score(row):
    return (row[0], [float(row[6])])

rdd16 = rdd.map(map_score).reduceByKey(lambda a,b: a + b) \
            .map(lambda x:(x[0], float(pd.Series(x[1]).var()))) \
            .takeOrdered(10, key=lambda x: -x[1])
print("RDD 16:", rdd16)

# --- DataFrame ---
df16 = df.groupBy("id").agg(F.variance("review_score").alias("varianza")).orderBy(F.col("varianza").desc()).limit(10)
df16.show()

# --- SQL ---
query16 = """
SELECT id, VARIANCE(review_score) AS varianza
FROM books
GROUP BY id
ORDER BY varianza DESC
LIMIT 10
"""
spark.sql(query16).show()

# =============================================================
# 17. Usuarios con mejor helpfulness (solo usuarios con >20 reviews)
# =============================================================

# --- RDD ---
def map_help(row):
    try:
        yes, tot = row[5].split("/")
        return (row[3], (int(yes), int(tot), 1))
    except:
        return (row[3], (0, 0, 1))

rdd17 = rdd.map(map_help) \
           .reduceByKey(lambda a,b:(a[0]+b[0], a[1]+b[1], a[2]+b[2])) \
           .filter(lambda x: x[1][2] > 20) \
           .map(lambda x:(x[0], x[1][0] / x[1][1] if x[1][1] > 0 else None))
print("RDD 17 sample:", rdd17.take(10))

# --- DataFrame ---
df17 = (df.withColumn("yes", F.split("review_helpfulness","/").getItem(0).cast("int"))
          .withColumn("total", F.split("review_helpfulness","/").getItem(1).cast("int"))
          .groupBy("user_id")
          .agg(F.sum("yes").alias("yes_total"),
               F.sum("total").alias("total_total"),
               F.count("*").alias("reviews"))
          .filter("reviews > 20")
          .withColumn("ratio", F.col("yes_total")/F.col("total_total")))
df17.show()

# --- SQL ---
query17 = """
SELECT user_id,
       SUM(CAST(SPLIT(review_helpfulness,'/')[0] AS INT)) AS yes_total,
       SUM(CAST(SPLIT(review_helpfulness,'/')[1] AS INT)) AS total_total,
       COUNT(*) AS reviews,
       (SUM(CAST(SPLIT(review_helpfulness,'/')[0] AS INT)) /
       SUM(CAST(SPLIT(review_helpfulness,'/')[1] AS INT))) AS ratio
FROM books
GROUP BY user_id
HAVING reviews > 20
"""
spark.sql(query17).show()

# =============================================================
# 18. Reviews que contienen la palabra "Seuss"
# =============================================================

# --- RDD ---
rdd18 = rdd.filter(lambda row: "seuss" in row[9].lower())
print("RDD 18 sample:", rdd18.take(5))

# --- DataFrame ---
df18 = df.filter(F.lower(df.review_text).contains("seuss"))
df18.show()

# --- SQL ---
query18 = """
SELECT *
FROM books
WHERE LOWER(review_text) LIKE '%seuss%'
"""
spark.sql(query18).show()

# =============================================================
# 19. Score promedio por década
# =============================================================

# --- RDD ---
def map_decade(row):
    try:
        year = int(row[10][:4])
        dec = (year // 10) * 10
        return (dec, (float(row[6]), 1))
    except:
        return ("?", (0.0, 1))

rdd19 = rdd.map(map_decade) \
           .reduceByKey(lambda a,b:(a[0]+b[0], a[1]+b[1])) \
           .map(lambda x:(x[0], x[1][0]/x[1][1]))
print("RDD 19:", rdd19.collect())

# --- DataFrame ---
df19 = df.withColumn("year", F.year("review_date")) \
        .withColumn("decade", (F.col("year")/10).cast("int")*10) \
        .groupBy("decade").agg(F.avg("review_score"))
df19.show()

# --- SQL ---
query19 = """
SELECT FLOOR(YEAR(review_date)/10)*10 AS decada,
       AVG(review_score) AS promedio
FROM books
GROUP BY decada
"""
spark.sql(query19).show()

# =============================================================
# 20. Cantidad de summaries con '!'
# =============================================================

# --- RDD ---
rdd20 = rdd.filter(lambda row: "!" in row[8]).count()
print("RDD 20 count:", rdd20)

# --- DataFrame ---
df20 = df.filter(df.review_summary.contains("!")).count()
print("DF 20 count:", df20)

# --- SQL ---
query20 = """
SELECT COUNT(*) AS summaries_con_signo
FROM books
WHERE review_summary LIKE '%!%'
"""
spark.sql(query20).show()