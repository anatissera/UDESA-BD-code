# Spark RDD Exercises for Books Dataset
# Functions defined explicitly instead of lambdas

from pyspark import SparkContext
from datetime import datetime
import re

sc = SparkContext.getOrCreate()
rdd = sc.textFile("/tmp/books_ratings_corregido.csv").map(lambda l: l.split(";"))

# 1. Reviews per book

def map_reviews_per_book(row):
    return (row[0], 1)

rdd1 = rdd.map(map_reviews_per_book).reduceByKey(lambda a, b: a + b)

# 2. Average score per book

def map_score_per_book(row):
    return (row[0], (float(row[6]), 1))

rdd2 = rdd.map(map_score_per_book).reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1]))
rdd2_final = rdd2.map(lambda x: (x[0], x[1][0] / x[1][1]))

# 3. Reviews per month

def map_month(row):
    try:
        m = int(row[10].split("-")[1])
    except:
        m = None
    return (m, 1)

rdd3 = rdd.map(map_month).reduceByKey(lambda a, b: a + b)

# 4. Top 10 users with most reviews

def map_reviews_per_user(row):
    return (row[3], 1)

rdd4 = rdd.map(map_reviews_per_user).reduceByKey(lambda a, b: a + b)

top10_users = rdd4.takeOrdered(10, key=lambda x: -x[1])

# 5. Longest review per book

def map_longest_review(row):
    return (row[0], len(row[9]))

rdd5 = rdd.map(map_longest_review).reduceByKey(lambda a, b: max(a, b))

# 6. Helpfulness ratio distribution

def map_helpfulness(row):
    h = row[5]
    try:
        yes, tot = h.split("/")
        ratio = int(yes) / int(tot)
    except:
        ratio = None
    return (ratio, 1)

rdd6 = rdd.map(map_helpfulness).reduceByKey(lambda a, b: a + b)

# 7. Word count in titles

def map_title_words(row):
    title = row[1].lower()
    cleaned = re.sub(r"[^a-z ]", " ", title)
    words = cleaned.split()
    return [w for w in words if len(w) >= 4]

rdd7 = rdd.flatMap(map_title_words).map(lambda w: (w, 1)).reduceByKey(lambda a, b: a + b)

# 8. Total words in reviews per book

def map_review_words(row):
    return (row[0], len(row[9].split()))

rdd8 = rdd.map(map_review_words).reduceByKey(lambda a, b: a + b)

# 9. Maximum score per user

def map_max_score(row):
    return (row[3], float(row[6]))

rdd9 = rdd.map(map_max_score).reduceByKey(lambda a, b: max(a, b))

# 10. Reviews by time of day

def map_time_bucket(row):
    try:
        epoch = int(row[7])
        h = datetime.utcfromtimestamp(epoch).hour
    except:
        return ("desconocido", 1)

    if 6 <= h < 12:
        bucket = "mañana"
    elif 12 <= h < 18:
        bucket = "tarde"
    elif 18 <= h < 24:
        bucket = "noche"
    else:
        bucket = "madrugada"

    return (bucket, 1)

rdd10 = rdd.map(map_time_bucket).reduceByKey(lambda a, b: a + b)

# 11. Sentiment classification

def map_sentiment(row):
    score = float(row[6])
    if score >= 4:
        return ("positivo", 1)
    elif score <= 2:
        return ("negativo", 1)
    else:
        return ("neutral", 1)

rdd11 = rdd.map(map_sentiment).reduceByKey(lambda a, b: a + b)

# 12. Reviews per profile name

def map_profile_count(row):
    return (row[4], 1)

rdd12 = rdd.map(map_profile_count).reduceByKey(lambda a, b: a + b)