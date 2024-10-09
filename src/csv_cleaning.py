import pandas as pd

def read_csv():
    spotify = pd.read_csv('/home/juan/Descargas/spotify_dataset.csv', index_col=0)
    return spotify

def transform_csv():
    spotify = read_csv()
    # Delete row 65900
    spotify.drop(spotify[spotify.isnull().any(axis=1)].index, inplace=True)
    spotify = spotify.drop_duplicates()

    nulls = spotify.isnull().sum()
    duplicated = spotify.duplicated().sum()
    spotify['id'] = range(1, len(spotify) + 1)

    # Move the 'id' column to the beginning
    cols = ['id'] + [col for col in spotify.columns if col != 'id']
    spotify = spotify[cols]

    spotify['duration_ms'] = pd.to_numeric(spotify['duration_ms'])
    spotify['popularity'] = pd.to_numeric(spotify['popularity'])
    spotify['danceability'] = pd.to_numeric(spotify['danceability'])
    spotify['energy'] = pd.to_numeric(spotify['energy'])
    spotify['acousticness'] = pd.to_numeric(spotify['acousticness'])
    spotify['instrumentalness'] = pd.to_numeric(spotify['instrumentalness'])
    spotify['valence'] = pd.to_numeric(spotify['valence'])
    spotify['speechiness'] = pd.to_numeric(spotify['speechiness'])
    spotify['loudness'] = pd.to_numeric(spotify['loudness'])

    spotify['duration_sec'] = spotify['duration_ms'] / 1000
    spotify.drop(['duration_ms'], axis=1, inplace=True)

    nulls = spotify.isnull().sum()
    duplicated = spotify.duplicated().sum()

    # Display the null values
    print("Null Data: \n", nulls)
    print("There are ", duplicated, " duplicated data")
    print(f"The dataset has {spotify.shape[0]} data")

    value_to_search = "Sunflower"

    # Filter DataFrame and get rows where 'track_name' is equal to value_to_search
    result = spotify[spotify['track_name'] == value_to_search]

    # Print the result
    print(result)

    spotify.to_csv('data/spotify_updated.csv', index=False)
    return spotify

if __name__ == "__main__":
    transform_csv()
