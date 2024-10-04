import pandas as pd

def read_csv():
    spotify = pd.read_csv('/home/juan/Descargas/spotify_dataset.csv', index_col=0)
    return spotify

def transform_csv():
    spotify = read_csv()
    # Borrar la fila 65900
    spotify.drop(spotify[spotify.isnull().any(axis=1)].index,inplace=True)
    spotify = spotify.drop_duplicates()

    nulls = spotify.isnull().sum()
    duplicated = spotify.duplicated().sum()
    spotify['id'] = range(1, len(spotify) + 1)

    # Mover la columna 'id' al principio
    cols = ['id'] + [col for col in spotify.columns if col != 'id']
    spotify = spotify[cols]

    spotify['duration_ms']=pd.to_numeric(spotify['duration_ms'])
    spotify['popularity']=pd.to_numeric(spotify['popularity'])
    spotify['danceability']=pd.to_numeric(spotify['danceability'])
    spotify['energy']=pd.to_numeric(spotify['energy'])
    spotify['acousticness']=pd.to_numeric(spotify['acousticness'])
    spotify['instrumentalness']=pd.to_numeric(spotify['instrumentalness'])
    spotify['valence']=pd.to_numeric(spotify['valence'])
    spotify['speechiness']=pd.to_numeric(spotify['speechiness'])
    spotify['loudness']=pd.to_numeric(spotify['loudness'])

    spotify['duration_sec']=spotify['duration_ms']/1000
    spotify.drop(['duration_ms'], axis=1, inplace=True)

    nulls = spotify.isnull().sum()
    duplicated = spotify.duplicated().sum()

    # Saber cual es el valor nulo
    print("Null Data: \n",nulls)
    print("There are ",duplicated, " duplicated data")
    print(f"The dataset has {spotify.shape[0]} data")

    valor_a_buscar = "Sunflower"

    # Filtrar DataFrame y obtener filas donde 'track_name' sea igual a valor_a_buscar
    resultado = spotify[spotify['track_name'] == valor_a_buscar]

    # Imprimir el resultado
    print(resultado)

    spotify.to_csv('data/spotify_updated.csv', index=False)

if __name__ == "__main__":
    transform_csv()