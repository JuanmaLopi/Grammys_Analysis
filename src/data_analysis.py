import pandas as pd
import matplotlib.pyplot as plt

# DATA ANALYSIS

csv_path = r'/home/juan/Documentos/Grammy_Analysis/data/Grammy_And_Spotify_Merged.csv'
merged = pd.read_csv(csv_path)

# Distribution of popularity of nominated artists.
merged['popularity'].hist(bins=20, edgecolor='black')
plt.title('Distribution of Song Popularity')
plt.xlabel('Popularity')
plt.ylabel('Frequency')
plt.show()

# Number of Nominations per year
merged.groupby('year').size().plot(kind='bar', figsize=(10, 6))
plt.title('Number of Nominations by Year')
plt.xlabel('Year')
plt.ylabel('Number of Nominations')
plt.show()

# Average length of songs per artist
top_artists = merged.groupby('artist')['popularity'].mean().nlargest(10)
top_artists.plot(kind='bar', color='skyblue')
plt.title('Top 10 Artists by Average Popularity')
plt.ylabel('Average Popularity')
plt.show()

# Average popularity of songs by artist
top_artists = merged.groupby('artist')['popularity'].mean().nlargest(10)
top_artists.plot(kind='bar', color='skyblue')
plt.title('Top 10 Artists by Average Popularity')
plt.ylabel('Average Popularity')
plt.show()

# Distribution of winning songs by category
merged[merged['winner'] == 'True']['category'].value_counts().plot(kind='bar', figsize=(12, 6))
plt.title('Winning Songs by Category')
plt.xlabel('Category')
plt.ylabel('Number of Wins')
plt.show()

# Relationship between popularity and length of the song
merged.plot.scatter(x='popularity', y='duration_ms', alpha=0.5)
plt.title('Popularity vs Song Duration')
plt.xlabel('Popularity')
plt.ylabel('Duration (ms)')
plt.show()

# Proportion of winning songs
merged['winner'].value_counts().plot(kind='pie', autopct='%1.1f%%', labels=['Not Winner', 'Winner'], colors=['lightcoral', 'lightgreen'])
plt.title('Proportion of Winning Songs')
plt.show()

# Distribution of length of songs by year
merged.groupby('year')['duration_ms'].mean().plot(kind='line', marker='o')
plt.title('Average Song Duration Over the Years')
plt.xlabel('Year')
plt.ylabel('Average Duration (ms)')
plt.show()

# Top 10 categories with the most nominations
merged['category'].value_counts().nlargest(10).plot(kind='bar', color='purple')
plt.title('Top 10 Categories by Number of Nominations')
plt.xlabel('Category')
plt.ylabel('Number of Nominations')
plt.show()

# Analysis of the most popular songs
top_songs = merged.nlargest(10, 'popularity')[['track_name', 'artist', 'popularity']]
top_songs.plot(kind='barh', x='track_name', y='popularity', color='lightblue', figsize=(10, 6))
plt.title('Top 10 Most Popular Songs')
plt.xlabel('Popularity')
plt.ylabel('Track Name')
plt.show()

# Popularity comparison between most nominated artists
top_nominated_artists = merged['artist'].value_counts().nlargest(10).index
merged[merged['artist'].isin(top_nominated_artists)].boxplot(column='popularity', by='artist', figsize=(10, 8), rot=90)
plt.title('Popularity by Top Nominated Artists')
plt.suptitle('')
plt.show()

# Album nominations
merged['album_name'].value_counts().nlargest(10).plot(kind='barh', color='lightgreen')
plt.title('Top 10 Albums by Number of Nominations')
plt.xlabel('Number of Nominations')
plt.ylabel('Album Name')
plt.show()







