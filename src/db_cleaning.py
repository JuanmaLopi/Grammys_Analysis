import pandas as pd
import logging
from extract import read_db
from spotipy.oauth2 import SpotifyClientCredentials
from dotenv import load_dotenv
import spotipy
from concurrent.futures import ThreadPoolExecutor
import os
import math

def transform_db():
    # Load Spotify credentials and authenticate
    load_dotenv()
    client_id = os.getenv('SPOTIFY_CLIENT_ID')
    client_secret = os.getenv('SPOTIFY_CLIENT_SECRET')
    auth_manager = SpotifyClientCredentials(client_id=client_id, client_secret=client_secret)
    sp = spotipy.Spotify(auth_manager=auth_manager)

    grammy = read_db()

    if grammy is None or not isinstance(grammy, pd.DataFrame):
        print("Error: The read_db() function did not return valid data.")
        return

    # Filling missing values
    grammy['workers'] = grammy['workers'].fillna("Unknown Worker")
    grammy['nominee'] = grammy['nominee'].fillna("Unknown")
    grammy['img'] = grammy['img'].fillna("https://soundimaging.com/wp-content/uploads/2020/12/coming.jpeg")
    
    print("Missing values in 'workers', 'nominee', and 'img' filled.")

    # Function to get artists for a list of songs using the Spotify API
    def get_artists_from_songs(song_names):
        artists = []
        for song_name in song_names:
            try:
                # Search up to 50 songs in a single query (Spotify API limit)
                results = sp.search(q=f'track:{song_name}', type='track', limit=1)
                if results['tracks']['items']:
                    artists.append(results['tracks']['items'][0]['artists'][0]['name'])
                else:
                    artists.append(None)
            except Exception as e:
                print(f"Error searching for song {song_name}: {e}")
                artists.append(None)
        return artists

    # Use ThreadPoolExecutor to parallelize artist lookups in batches
    def fill_artists(grammy, batch_size=50):
        artists = []
        # Group searches into batches to optimize API calls
        for i in range(0, len(grammy), batch_size):
            batch = grammy['nominee'].iloc[i:i + batch_size].tolist()
            with ThreadPoolExecutor(max_workers=10) as executor:
                artists_batch = list(executor.map(get_artists_from_songs, [batch]))
            artists.extend(artists_batch)
        return artists

    print("Starting artist filling process")
    logging.info("Starting artist filling process")
    
    # Fill artists
    if 'artist' in grammy.columns:
        grammy['artist'] = grammy['artist'].fillna("Unknown")
    else:
        grammy['artist'] = ["Unknown"] * len(grammy)

    # Only fill missing values in the 'artist' column
    mask = grammy['artist'] == "Unknown"
    grammy.loc[mask, 'artist'] = fill_artists(grammy[mask])

    # Replace remaining missing values in 'artist'
    grammy['artist'] = grammy['artist'].fillna("Unknown Artist or Various Artists")
    
    print("Artist filling process completed.")
    logging.info("Artist filling process completed.")
    
    # Display the remaining null values
    print(grammy.isnull().sum())

    # Save the updated CSV file (optional)
    # grammy.to_csv('data/grammys_updated.csv', index=False)
    print("Updated file saved.")

    return grammy

if __name__ == "__main__":
    transform_db()
