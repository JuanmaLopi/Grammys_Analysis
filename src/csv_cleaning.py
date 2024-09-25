import pandas as pd

df = pd.read_csv('c:/Users/juanm/Downloads/spotify_dataset.csv')

# Borrar la fila 65900
df.drop(df[df.isnull().any(axis=1)].index,inplace=True)

nulls = df.isnull().sum()
duplicated = df.duplicated().sum()

# Saber cual es el valor nulo
print("Null Data: \n",nulls)
print("There are ",duplicated, " duplicated data")