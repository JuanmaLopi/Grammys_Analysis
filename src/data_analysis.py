import pandas as pd

spotify_merge = pd.read_csv('D:/QuintoSemestre/ETL/Grammy_Analysis/data/spotify_updated.csv')
grammy_merge = pd.read_csv('D:/QuintoSemestre/ETL/Grammy_Analysis/data/grammys_updated.csv')

spotify_merge = pd.read_csv('../data/spotify_updated.csv')
grammy_merge = pd.read_csv('../data/grammys_updated.csv')

# Limpiar y normalizar los nombres de las canciones
grammy_merge['nominee_clean'] = grammy_merge['nominee'].str.strip().str.lower()
spotify_merge['track_name_clean'] = spotify_merge['track_name'].str.strip().str.lower()

# Realizar un left join basado en los nombres de las canciones limpias
merged_df = grammy_merge.merge(spotify_merge, how='left', left_on='nominee_clean', right_on='track_name_clean')

# Comprobar la cantidad de filas después del merge
merged_count = merged_df.shape[0]
print(f"Cantidad de filas después del merge: {merged_count}")

# Si no se alcanzan 4000 filas, cambiar a outer join
if merged_count < 4000:
    merged_df = grammy_merge.merge(spotify_merge, how='outer', left_on='nominee_clean', right_on='track_name_clean')
    merged_count = merged_df.shape[0]
    print(f"Cantidad de filas después del outer join: {merged_count}")

# Eliminar las columnas auxiliares
merged = merged_df.drop(columns=['nominee_clean', 'track_name_clean', 'id_x','id_y'])

merged['id'] = range(1, len(merged) + 1)

# Mover la columna 'id' al principio
cols = ['id'] + [col for col in merged.columns if col != 'id']
merged = merged[cols]

merged = merged.dropna()

print(merged.isnull().sum())
print(f"La cantidad de datos depues de borrar los nulos es de: {merged.shape[0]}")


# Guardar el DataFrame resultante
merged.to_csv('../data/merged.csv', index=False)
print("Merge completado y archivo guardado como 'grammy_spotify_merged.csv'.")



# DATA ANALYSIS


# Get the list of most popular artists
popular_artists = merged['artist'].value_counts().head(10)
#print(popular_artists)

# Get the list of most popular songs
popular_songs = merged['track_name'].value_counts().head(10)
#print(popular_songs)

# Get the average duration of songs
average_duration = merged['duration_ms'].mean()
#print(average_duration)

# Get the average tempo of songs
average_tempo = merged['tempo'].mean()
#print(average_tempo)

# Get the list of most common genres
common_genres = merged['track_genre'].value_counts().head(10)
#print(common_genres)

# Get the list of most popular artists by genre
popular_artists_by_genre = merged.groupby('track_genre')['artist'].value_counts().head(10)
#print(popular_artists_by_genre)

# Get the list of most popular songs by genre
popular_songs_by_genre = merged.groupby('track_genre')['track_name'].value_counts().head(10)
#print(popular_songs_by_genre)

# Get the average duration of songs by genre
average_duration_by_genre = merged.groupby('track_genre')['duration_ms'].mean()
#print(average_duration_by_genre)

# Get the average tempo of songs by genre
average_tempo_by_genre = merged.groupby('track_genre')['tempo'].mean()
#print(average_tempo_by_genre)

# Get the list of most common genres by year
common_genres_by_year = merged.groupby('year')['track_genre'].value_counts().head(10)
#print(common_genres_by_year)

# Get the list of most popular artists by year
popular_artists_by_year = merged.groupby('year')['artist'].value_counts().head(10)
#print(popular_artists_by_year)

# Get the list of most popular songs by year
popular_songs_by_year = merged.groupby('year')['track_name'].value_counts().head(10)
#print(popular_songs_by_year)

# Get the average duration of songs by year
average_duration_by_year = merged.groupby('year')['duration_ms'].mean()
#print(average_duration_by_year)

# Get the average tempo of songs by year
average_tempo_by_year = merged.groupby('year')['tempo'].mean()
#print(average_tempo_by_year)

# Get the list of songs that have won awards by year
award_winning_songs_by_year = merged[merged['winner'] == True].groupby('year')['track_name'].unique()
#print(award_winning_songs_by_year)

# Get the list of artists who have won awards by year
award_winning_artists_by_year = merged[merged['winner'] == True].groupby('year')['artist'].unique()
#print(award_winning_artists_by_year)

# Get the list of most common genres by artist
common_genres_by_artist = merged.groupby('artist')['track_genre'].value_counts().head(10)
#print(common_genres_by_artist)

# Get the list of most popular artists by genre and year
popular_artists_by_genre_and_year = merged.groupby(['track_genre', 'year'])['artist'].value_counts().head(10)
#print(popular_artists_by_genre_and_year)

# Get the list of most popular songs by genre and year
popular_songs_by_genre_and_year = merged.groupby(['track_genre', 'year'])['track_name'].value_counts()
print(popular_songs_by_genre_and_year)