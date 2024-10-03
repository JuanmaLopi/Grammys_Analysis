import pandas as pd

# DATA ANALYSIS

csv_path = r'D:\QuintoSemestre\ETL\Grammy_Analysis\data\Grammy_And_Spotify_Merged.csv'
merged = pd.read_csv(csv_path)

# Get the list of most popular artists
popular_artists = merged['artist'].value_counts().head(10)
#print(popular_artists)

# Get the list of most popular songs
popular_songs = merged['track_name'].value_counts().head(10)
#print(popular_songs)

# Get the average duration of songs
average_duration = merged['duration_ms'].mean()
#print(average_duration)

# Get the list of most popular artists by year
popular_artists_by_year = merged.groupby('year')['artist'].value_counts().head(10)
#print(popular_artists_by_year)

# Get the average duration of songs by year
average_duration_by_year = merged.groupby('year')['duration_ms'].mean()
#print(average_duration_by_year)

# Get the list of artists who have won awards by year
award_winning_artists_by_year = merged[merged['winner'] == True].groupby('year')['artist'].unique()
#print(award_winning_artists_by_year)
