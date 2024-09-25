import pandas as pd
import matplotlib.pyplot as plt

from db_conexion import establecer_conexion

conn, cursor = establecer_conexion()

# Consulta SQL para seleccionar los datos
query = "SELECT * FROM grammy_awards"

# Leer los datos en un DataFrame de pandas
df = pd.read_sql_query(query, conn)
df