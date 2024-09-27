import pandas as pd

spotify = pd.read_csv('c:/Users/juanm/Downloads/spotify_dataset.csv', index_col=0)

# Borrar la fila 65900
spotify.drop(spotify[spotify.isnull().any(axis=1)].index,inplace=True)

nulls = spotify.isnull().sum()
duplicated = spotify.duplicated().sum()
spotify['id'] = range(1, len(spotify) + 1)

# Mover la columna 'id' al principio
cols = ['id'] + [col for col in spotify.columns if col != 'id']
spotify = spotify[cols]

# Saber cual es el valor nulo
print("Null Data: \n",nulls)
print("There are ",duplicated, " duplicated data")

spotify.to_csv('../data/spotify_updated.csv', index=False)
