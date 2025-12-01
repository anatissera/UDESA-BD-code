import pandas as pd
import pyarrow as pa
from pyarrow import dataset
import re

INPUT = "/tmp/books_ratings_corregido.csv"

df = pd.read_csv(INPUT, names=[
        "id",
        "title",
        "price",
        "user_id",
        "profile_name",
        "review_helpfulness",
        "review_score",
        "review_time",
        "review_summary",
        "review_text",
        "review_date"
    ],
    engine="python",
    quoting=3 , sep=';', header=None)

df["review_date"] = pd.to_datetime(df["review_date"], errors="coerce")
df["mes"] = df["review_date"].dt.month
# agregamos mes para hacer la partición

# Lo cargamos en memoria con PyArrow
tabla_pyarrow = pa.Table.from_pandas(df)

# Guardamos la tabla de PyArrow en formato .parquet en nuestro HDFS
pa.dataset.write_dataset(
    tabla_pyarrow,
    filesystem='/tmp',
    base_dir='/books',
    format='parquet',
    basename_template="books{i}.parquet",
    partitioning=["mes"]
)