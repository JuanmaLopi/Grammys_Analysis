import pandas as pd
import logging
from extract import read_db
from spotipy.oauth2 import SpotifyClientCredentials
from dotenv import load_dotenv
import spotipy
from concurrent.futures import ThreadPoolExecutor
import os
import math

def transform_db():
    # Cargar las credenciales de Spotify y autenticar
    load_dotenv()
    client_id = os.getenv('SPOTIFY_CLIENT_ID')
    client_secret = os.getenv('SPOTIFY_CLIENT_SECRET')
    auth_manager = SpotifyClientCredentials(client_id=client_id, client_secret=client_secret)
    sp = spotipy.Spotify(auth_manager=auth_manager)

    grammy = read_db()

    if grammy is None or not isinstance(grammy, pd.DataFrame):
        print("Error: La función read_db() no retornó datos válidos.")
        return

    # Relleno de valores nulos
    grammy['workers'] = grammy['workers'].fillna("Unknown Worker")
    grammy['nominee'] = grammy['nominee'].fillna("Unknown")
    grammy['img'] = grammy['img'].fillna("https://soundimaging.com/wp-content/uploads/2020/12/coming.jpeg")
    
    print("Valores nulos en 'workers', 'nominee', e 'img' rellenos")

    # Función para obtener artistas de una lista de canciones usando la API de Spotify
    def obtener_artistas_de_canciones(nombres_canciones):
        artistas = []
        for nombre_cancion in nombres_canciones:
            try:
                # Buscar hasta 50 canciones en una sola consulta (límite de la API de Spotify)
                resultados = sp.search(q=f'track:{nombre_cancion}', type='track', limit=1)
                if resultados['tracks']['items']:
                    artistas.append(resultados['tracks']['items'][0]['artists'][0]['name'])
                else:
                    artistas.append(None)
            except Exception as e:
                print(f"Error buscando la canción {nombre_cancion}: {e}")
                artistas.append(None)
        return artistas

    # Uso de ThreadPoolExecutor para paralelizar las búsquedas de artistas en grupos
    def rellenar_artistas(grammy, batch_size=50):
        artistas = []
        # Agrupar las búsquedas en lotes para optimizar llamadas a la API
        for i in range(0, len(grammy), batch_size):
            batch = grammy['nominee'].iloc[i:i+batch_size].tolist()
            with ThreadPoolExecutor(max_workers=10) as executor:
                artistas_batch = list(executor.map(obtener_artistas_de_canciones, [batch]))
            artistas.extend(artistas_batch)
        return artistas

    print("Comienza proceso de relleno de artistas")
    logging.info("Comienza proceso de relleno de artistas")
    
    # Rellenar artistas
    if 'artist' in grammy.columns:
        grammy['artist'] = grammy['artist'].fillna("Unknown")
    else:
        grammy['artist'] = ["Unknown"] * len(grammy)

    # Solo rellenar los valores nulos en la columna 'artist'
    mask = grammy['artist'] == "Unknown"
    grammy.loc[mask, 'artist'] = rellenar_artistas(grammy[mask])

    # Reemplazar valores nulos restantes en 'artist'
    grammy['artist'] = grammy['artist'].fillna("Unknown Artist or Various Artists")
    
    print("Termina proceso de relleno")
    logging.info("Termina proceso de relleno")
    
    # Mostrar cantidad de valores nulos restantes
    print(grammy.isnull().sum())

    # Guardar el archivo CSV actualizado (opcional)
    # grammy.to_csv('data/grammys_updated.csv', index=False)
    print("Archivo actualizado y guardado.")

    
    return grammy

if __name__ == "__main__":
    transform_db()
