import pandas as pd

spotify = pd.read_csv('c:/Users/juanm/Downloads/spotify_dataset.csv')

# Borrar la fila 65900
spotify.drop(spotify[spotify.isnull().any(axis=1)].index,inplace=True)

nulls = spotify.isnull().sum()
duplicated = spotify.duplicated().sum()

# Saber cual es el valor nulo
print("Null Data: \n",nulls)
print("There are ",duplicated, " duplicated data")