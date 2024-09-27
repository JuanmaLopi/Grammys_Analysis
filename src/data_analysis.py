import pandas as pd

spotify_merge = pd.read_csv('D:\QuintoSemestre\ETL\Grammy_Analysis\data\spotify_updated.csv')
grammy_merge = pd.read_csv('D:\QuintoSemestre\ETL\Grammy_Analysis\data\grammys_updated.csv')

# Realizamos el merge de los dos DataFrames usando las columnas de artistas
merged_df = pd.merge(grammy_merge, spotify_merge, left_on='artist', right_on='artists', how='inner')

# Mostramos las primeras filas del DataFrame combinado para inspeccionar
merged_df.head()
