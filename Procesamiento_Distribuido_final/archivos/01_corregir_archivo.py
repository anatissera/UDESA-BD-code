import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from pyarrow import fs
from pyarrow import dataset
import re

INPUT = "/tmp/books_ratings.csv"
OUTPUT = "/tmp/books_ratings_corregido.csv"

df = pd.read_csv(
    INPUT,
    sep=';',
    header=None,
    names=[
        "id",
        "title",
        "price",
        "user_id",
        "profile_name",
        "review_helpfulness",
        "review_score",
        "review_time",
        "review_summary",
        "review_text"
    ],
    engine="python",
    quoting=3  # evita que pandas intente romper las comillas internas
)

df["price"] = df["price"].replace("", None)

# LIMPIEZA DE REVIEW SCORE
df["review_score"] = pd.to_numeric(df["review_score"], errors="coerce")

# CONVERTIR EPOCH → FECHA REAL
df["review_date"] = pd.to_datetime(df["review_time"], unit='s', errors='coerce')

# def split_helpfulness(value):
#     if pd.isna(value) or value == "":
#         return pd.Series([None, None])
#     try:
#         yes, total = value.split("/")
#         return pd.Series([int(yes), int(total)])
#     except:
#         return pd.Series([None, None])

# df[["help_yes", "help_total"]] = df["review_helpfulness"].apply(split_helpfulness)

df.to_csv(OUTPUT, sep=';', header=False, index=False)