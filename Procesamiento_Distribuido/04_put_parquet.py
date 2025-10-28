import pandas as pd
import pyarrow as pa
from pyarrow import dataset
import re

df = pd.read_csv('/tmp/recorridos_realizados_2024_corregido.csv', names=['id_recorrido', 'duracion_recorrido', 'fecha_origen_recorrido',
'id_estacion_origen', 'nombre_estacion_origen', 'direccion_estacion_origen', 
'long_estacion_origen', 'lat_estacion_origen', 'fecha_destino_recorrido', 'id_estacion_destino', 'nombre_estacion_destino', 'direccion_estacion_destino', 'long_estacion_destino', 'lat_estacion_destino', 'id_usuario', 'modelo_bicicleta', 'genero'], sep=';', header=None)

df["mes"] = [f.month for f in pd.to_datetime(df['fecha_origen_recorrido'])]

# Lo cargamos en memoria con PyArrow
tabla_pyarrow = pa.Table.from_pandas(df)

# Guardamos la tabla de PyArrow en formato .parquet en nuestro HDFS
pa.dataset.write_dataset(
    tabla_pyarrow,
    filesystem='/tmp',
    base_dir='/bicis',
    format='parquet',
    basename_template="bicis{i}.parquet",
    partitioning=["mes"]
)
