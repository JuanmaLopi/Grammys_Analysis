import pandas as pd

from db_conexion import establecer_conexion

conn, cursor = establecer_conexion()

# Consulta SQL para seleccionar los datos
query = "SELECT * FROM grammy_awards"

# Leer los datos en un DataFrame de pandas
grammy = pd.read_sql_query(query, conn)
#grammy

# Reemplazar los valores nulos de Worker, por trabajador desconocido
grammy['workers'] = grammy['workers'].fillna("Unknown Worker")
#print(grammy['workers'].isnull().sum())

# Reemplazar los valor nulos de nominee, por desconocido
grammy['nominee'] = grammy['nominee'].fillna("Unknown")
#print(grammy['nominee'].isnull().sum())

grammy['img'] = grammy['img'].fillna("https://soundimaging.com/wp-content/uploads/2020/12/coming.jpeg")
#print(grammy.isnull().sum())

import pandas as pd
import spotipy
from spotipy.oauth2 import SpotifyClientCredentials

# Credenciales de la API de Spotify (debes reemplazar estos valores por tus credenciales)
client_id = '30403d22103345d2be2672722e18dd36'
client_secret = '5bbc2135df4a4f77afe7489d7b3e7c4c'

# Autenticación con la API de Spotify
auth_manager = SpotifyClientCredentials(client_id=client_id, client_secret=client_secret)
sp = spotipy.Spotify(auth_manager=auth_manager)

# Función para buscar el artista en Spotify usando el nombre de la canción
def obtener_artista_de_cancion(nombre_cancion):
    try:
        # Realizar búsqueda en Spotify
        resultados = sp.search(q=f'track:{nombre_cancion}', type='track', limit=1)
        if resultados['tracks']['items']:
            # Retornar el nombre del artista
            return resultados['tracks']['items'][0]['artists'][0]['name']
        else:
            return None
    except Exception as e:
        print(f"Error buscando la canción {nombre_cancion}: {e}")
        return None

# Rellenar valores nulos en la columna 'artist' usando el nombre del artista extraído de Spotify
grammy['artist'] = grammy.apply(lambda row: obtener_artista_de_cancion(row['nominee']) if pd.isnull(row['artist']) else row['artist'], axis=1)

grammy['artist'] = grammy['artist'].fillna("Unknown Artist or Various Artists")

print(grammy.isnull().sum())

# Guardar el archivo CSV actualizado
grammy.to_csv('../data/grammys_updated.csv', index=False)

print("Archivo actualizado y guardado.")
