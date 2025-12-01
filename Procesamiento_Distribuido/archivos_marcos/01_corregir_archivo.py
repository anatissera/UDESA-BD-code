import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from pyarrow import fs
from pyarrow import dataset
import re

# Leemos el .csv a un DataFrame de Pandas
df = pd.read_csv('/tmp/badata_ecobici_recorridos_realizados_2024.csv', names=['id_recorrido', 'duracion_recorrido', 'fecha_origen_recorrido',
'id_estacion_origen', 'nombre_estacion_origen', 'direccion_estacion_origen', 
'long_estacion_origen', 'lat_estacion_origen', 'fecha_destino_recorrido', 'id_estacion_destino', 'nombre_estacion_destino', 'direccion_estacion_destino', 'long_estacion_destino', 'lat_estacion_destino', 'id_usuario', 'modelo_bicicleta', 'genero'], header=1)

# Eliminando '.' en duracion_recorrido para que Pandas pueda guardarlo como float.
df['duracion_recorrido'] = [ re.sub('.0^', '', str(x)) for x in df['duracion_recorrido'] ]
df['duracion_recorrido'] = df['duracion_recorrido'].str.replace('.', '')
df.astype({'duracion_recorrido': 'float'})

df.to_csv('/tmp/recorridos_realizados_2024_corregido.csv', sep=';', header=False, index=False)
