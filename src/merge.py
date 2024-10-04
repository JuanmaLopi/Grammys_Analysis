def merge():
    import os
    from dotenv import load_dotenv
    import pandas as pd
    import re
    from rapidfuzz import process, fuzz
    import spotipy
    from spotipy.oauth2 import SpotifyOAuth
    import time  # Para añadir pausas

    # Cargar las variables de entorno desde el archivo .env
    load_dotenv()

    # Configura las credenciales desde tu archivo .env
    sp = spotipy.Spotify(auth_manager=SpotifyOAuth(client_id=os.getenv('SPOTIFY_CLIENT_ID'),
                                                    client_secret=os.getenv('SPOTIFY_CLIENT_SECRET'),
                                                    redirect_uri=os.getenv('SPOTIFY_REDIRECT_URI'),
                                                    scope='user-library-read'))

    # Función para normalizar texto
    def normalize_text(text):
        if isinstance(text, str):
            text = text.lower()
            text = re.sub(r'[^a-zA-Z0-9\s]', '', text)  # Eliminar caracteres especiales
            text = re.sub(r'\s+', ' ', text).strip()  # Eliminar espacios adicionales
            text = re.sub(r'\b(feat|ft|featuring|remix|version)\b', '', text)  # Eliminar términos problemáticos
        return text

    # Función para hacer merge directo y luego coincidencia difusa
    def merge_datasets(grammy_merge, spotify_merge):
        # Normalizar los nombres de los artistas, canciones y álbumes en ambos datasets
        grammy_merge['artist_clean'] = grammy_merge['artist'].apply(normalize_text)
        grammy_merge['nominee_clean'] = grammy_merge['nominee'].apply(normalize_text)
        spotify_merge['artists_clean'] = spotify_merge['artists'].apply(normalize_text)
        spotify_merge['track_name_clean'] = spotify_merge['track_name'].apply(normalize_text)
        spotify_merge['album_name_clean'] = spotify_merge['album_name'].apply(normalize_text)

        # Exploding para que cada artista en una lista tenga su propia fila
        spotify_merge = spotify_merge.explode('artists_clean')

        # Merge directo basado en 'spotify_id' y 'track_id'
        merged_df = pd.merge(grammy_merge, spotify_merge, left_on='spotify_id', right_on='track_id', how='left')

        # Filas sin match en el merge basado en spotify_id
        no_match_df = merged_df[merged_df['track_id'].isnull()]

        # Si hay filas sin match, aplicar coincidencia difusa
        song_threshold = 70
        artist_threshold = 70
        album_threshold = 60

        matches = []
        for i, row in no_match_df.iterrows():
            song_match = process.extractOne(row['nominee_clean'], spotify_merge['track_name_clean'], scorer=fuzz.ratio)

            # Buscar coincidencia con track_name
            if song_match and song_match[1] >= song_threshold:
                matched_rows = spotify_merge[spotify_merge['track_name_clean'] == song_match[0]]
                artist_matches = []

                for _, matched_row in matched_rows.iterrows():
                    artist_match = process.extractOne(row['artist_clean'], [matched_row['artists_clean']], scorer=fuzz.ratio)
                    if artist_match and artist_match[1] >= artist_threshold:
                        artist_matches.append(matched_row)  # Solo añadir la fila completa

                if artist_matches:
                    matches.extend(artist_matches)
                else:
                    matches.append(row)
            else:
                # Buscar coincidencia con album_name
                album_match = process.extractOne(row['nominee_clean'], spotify_merge['album_name_clean'], scorer=fuzz.ratio)
                if album_match and album_match[1] >= album_threshold:
                    matched_rows = spotify_merge[spotify_merge['album_name_clean'] == album_match[0]]
                    artist_matches = []

                    for _, matched_row in matched_rows.iterrows():
                        artist_match = process.extractOne(row['artist_clean'], [matched_row['artists_clean']], scorer=fuzz.ratio)
                        if artist_match and artist_match[1] >= artist_threshold:
                            artist_matches.append(matched_row)

                    if artist_matches:
                        matches.extend(artist_matches)
                    else:
                        matches.append(row)
                else:
                    matches.append(row)

            # Mostrar progreso cada 100 filas
            if (i + 1) % 100 == 0:
                print(f"Procesadas {i + 1} filas de {len(no_match_df)}")

        # Crear DataFrame con coincidencias difusas
        fuzzy_matched_df = pd.DataFrame(matches)

        # Concatenar el merge directo y las coincidencias difusas
        final_merged_df = pd.concat([merged_df.reset_index(drop=True), fuzzy_matched_df.reset_index(drop=True)], ignore_index=True)

        # Eliminar columnas auxiliares
        final_merged_df = final_merged_df.drop(columns=['artist_clean', 'nominee_clean', 'artists_clean', 'track_name_clean', 'album_name_clean'], errors='ignore')

        # Eliminar duplicados
        final_merged_df = final_merged_df.drop_duplicates(subset=['nominee', 'artist', 'category'])

        print(f"Cantidad de filas después del merge: {final_merged_df.shape[0]}")
        return final_merged_df

    # Función para obtener datos de un track de Spotify
    def obtener_datos_track(track_id):
        try:
            track_info = sp.track(track_id)
            return track_info
        except spotipy.SpotifyException as e:
            if e.http_status == 401:  # Token expirado
                sp.auth_manager.refresh_access_token()
                track_info = sp.track(track_id)  # Intenta nuevamente con el nuevo token
                return track_info
            else:
                print(f"Error al obtener información del track {track_id}: {e}")
                return None

    # Función para rellenar los valores faltantes usando la API de Spotify
    def fill_missing_data(final_merged_df):
        for index, row in final_merged_df[final_merged_df['track_id'].isnull()].iterrows():
            spotify_id = row['spotify_id']
            
            if pd.notna(spotify_id):
                try:
                    track_info = obtener_datos_track(spotify_id)
                    if track_info:
                        final_merged_df.loc[index, 'track_id'] = track_info.get('id')
                        final_merged_df.loc[index, 'artists'] = ', '.join([artist['name'] for artist in track_info.get('artists', [])])
                        final_merged_df.loc[index, 'album_name'] = track_info.get('album', {}).get('name')
                        final_merged_df.loc[index, 'track_name'] = track_info.get('name')
                        final_merged_df.loc[index, 'popularity'] = track_info.get('popularity')
                        final_merged_df.loc[index, 'duration_ms'] = track_info.get('duration_ms')

                        audio_features = obtener_datos_track(spotify_id)  # Cambia esto si quieres otra función para obtener características de audio
                        if audio_features:
                            final_merged_df.loc[index, 'danceability'] = audio_features.get('danceability')
                            final_merged_df.loc[index, 'energy'] = audio_features.get('energy')
                            final_merged_df.loc[index, 'tempo'] = audio_features.get('tempo')

                    if index % 100 == 0:
                        print(f"Información rellenada en fila {index}")

                    # Añadir una pequeña pausa entre cada iteración
                    time.sleep(1)

                except Exception as e:
                    print(f"Error al procesar fila {index}: {e}")

        return final_merged_df

    def main():
        # Cargar los archivos CSV
        from db_cleaning import transform_db
        from csv_cleaning import transform_csv

        spotify_merge = transform_csv()
        grammy_merge = transform_db()

        # Hacer el merge de los datasets
        final_merged_df = merge_datasets(grammy_merge, spotify_merge)

        # Rellenar datos faltantes usando la API de Spotify
        final_merged_df = fill_missing_data(final_merged_df)

        # Eliminar columnas innecesarias: Unnamed: 0 y track_id
        columns_to_drop = ['Unnamed: 0', 'track_id']

        # Eliminar las columnas seleccionadas
        final_merged_df = final_merged_df.drop(columns=columns_to_drop, errors='ignore')

        # Guardar el DataFrame final en un archivo CSV
        output_path = os.path.join('data', 'Database_Merged.csv')
        final_merged_df.to_csv(output_path, index=False)
        print(f"Archivo CSV final guardado en: {output_path}")

    if __name__ == "__main__":
        main()

if __name__ == "__main__":
    merge()