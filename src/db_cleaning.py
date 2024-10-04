def transform_db():
    import pandas as pd

    from extract import read_db

    grammy = read_db()

    if grammy is None:
        print("Error: La función read_db() retornó None.")
        return  # Termina la ejecución de la función si no hay datos válidos

    # Asegurarse de que sea un DataFrame
    if not isinstance(grammy, pd.DataFrame):
        print("Error: El resultado de read_db() no es un DataFrame.")
        return

    # Reemplazar los valores nulos de Worker, por trabajador desconocido
    grammy['workers'] = grammy['workers'].fillna("Unknown Worker")
    print("Workers Filled")

    # Reemplazar los valor nulos de nominee, por desconocido
    grammy['nominee'] = grammy['nominee'].fillna("Unknown")
    print("Nominee Filled")

    grammy['img'] = grammy['img'].fillna("https://soundimaging.com/wp-content/uploads/2020/12/coming.jpeg")
    print("Image Filled")

    import pandas as pd
    import os
    import spotipy
    from spotipy.oauth2 import SpotifyClientCredentials
    from dotenv import load_dotenv

    load_dotenv()

    # Credenciales de la API de Spotify (debes reemplazar estos valores por tus credenciales)
    client_id = os.getenv('SPOTIFY_CLIENT_ID')
    client_secret = os.getenv('SPOTIFY_CLIENT_SECRET')

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
    grammy.to_csv('data/grammys_updated.csv', index=False)

    print("Archivo actualizado y guardado.")

if __name__ == "__main__":
    transform_db()