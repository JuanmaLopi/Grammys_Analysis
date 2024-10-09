def merge():
    import os
    from dotenv import load_dotenv
    import pandas as pd
    import re
    from rapidfuzz import process, fuzz
    import spotipy
    from spotipy.oauth2 import SpotifyOAuth
    import time  # To add pauses

    # Load environment variables from the .env file
    load_dotenv()

    # Set up credentials from your .env file
    sp = spotipy.Spotify(auth_manager=SpotifyOAuth(client_id=os.getenv('SPOTIFY_CLIENT_ID'),
                                                    client_secret=os.getenv('SPOTIFY_CLIENT_SECRET'),
                                                    redirect_uri=os.getenv('SPOTIFY_REDIRECT_URI'),
                                                    scope='user-library-read'))

    # Function to normalize text
    def normalize_text(text):
        if isinstance(text, str):
            text = text.lower()
            text = re.sub(r'[^a-zA-Z0-9\s]', '', text)  # Remove special characters
            text = re.sub(r'\s+', ' ', text).strip()  # Remove extra spaces
            text = re.sub(r'\b(feat|ft|featuring|remix|version)\b', '', text)  # Remove problematic terms
        return text

    # Function for direct merge and then fuzzy matching
    def merge_datasets(grammy_merge, spotify_merge):
        # Normalize artist names, song titles, and album names in both datasets
        grammy_merge['artist_clean'] = grammy_merge['artist'].apply(normalize_text)
        grammy_merge['nominee_clean'] = grammy_merge['nominee'].apply(normalize_text)
        spotify_merge['artists_clean'] = spotify_merge['artists'].apply(normalize_text)
        spotify_merge['track_name_clean'] = spotify_merge['track_name'].apply(normalize_text)
        spotify_merge['album_name_clean'] = spotify_merge['album_name'].apply(normalize_text)

        # Explode so that each artist in a list has its own row
        spotify_merge = spotify_merge.explode('artists_clean')

        # Direct merge based on 'spotify_id' and 'track_id'
        merged_df = pd.merge(grammy_merge, spotify_merge, left_on='spotify_id', right_on='track_id', how='left')

        # Rows without matches in the merge based on spotify_id
        no_match_df = merged_df[merged_df['track_id'].isnull()]

        # If there are rows without matches, apply fuzzy matching
        song_threshold = 70
        artist_threshold = 70
        album_threshold = 60

        matches = []
        for i, row in no_match_df.iterrows():
            song_match = process.extractOne(row['nominee_clean'], spotify_merge['track_name_clean'], scorer=fuzz.ratio)

            # Search for a match with track_name
            if song_match and song_match[1] >= song_threshold:
                matched_rows = spotify_merge[spotify_merge['track_name_clean'] == song_match[0]]
                artist_matches = []

                for _, matched_row in matched_rows.iterrows():
                    artist_match = process.extractOne(row['artist_clean'], [matched_row['artists_clean']], scorer=fuzz.ratio)
                    if artist_match and artist_match[1] >= artist_threshold:
                        artist_matches.append(matched_row)  # Only add the complete row

                if artist_matches:
                    matches.extend(artist_matches)
                else:
                    matches.append(row)
            else:
                # Search for a match with album_name
                album_match = process.extractOne(row['nominee_clean'], spotify_merge['album_name_clean'], scorer=fuzz.ratio)
                if album_match and album_match[1] >= album_threshold:
                    matched_rows = spotify_merge[spotify_merge['album_name_clean'] == album_match[0]]
                    artist_matches = []

                    for _, matched_row in matched_rows.iterrows():
                        artist_match = process.extractOne(row['artist_clean'], [matched_row['artists_clean']], scorer=fuzz.ratio)
                        if artist_match and artist_match[1] >= artist_threshold:
                            artist_matches.append(matched_row)

                    if artist_matches:
                        matches.extend(artist_matches)
                    else:
                        matches.append(row)
                else:
                    matches.append(row)

            # Show progress every 100 rows
            if (i + 1) % 100 == 0:
                print(f"Processed {i + 1} rows out of {len(no_match_df)}")

        # Create DataFrame with fuzzy matches
        fuzzy_matched_df = pd.DataFrame(matches)

        # Concatenate the direct merge and the fuzzy matches
        final_merged_df = pd.concat([merged_df.reset_index(drop=True), fuzzy_matched_df.reset_index(drop=True)], ignore_index=True)

        # Remove auxiliary columns
        final_merged_df = final_merged_df.drop(columns=['artist_clean', 'nominee_clean', 'artists_clean', 'track_name_clean', 'album_name_clean'], errors='ignore')

        # Remove duplicates
        final_merged_df = final_merged_df.drop_duplicates(subset=['nominee', 'artist', 'category'])

        print(f"Number of rows after the merge: {final_merged_df.shape[0]}")
        return final_merged_df

    # Function to get data from a Spotify track
    def get_track_data(track_id):
        try:
            track_info = sp.track(track_id)
            return track_info
        except spotipy.SpotifyException as e:
            if e.http_status == 401:  # Token expired
                sp.auth_manager.refresh_access_token()
                track_info = sp.track(track_id)  # Try again with the new token
                return track_info
            else:
                print(f"Error getting information for track {track_id}: {e}")
                return None

    # Function to fill missing values using the Spotify API
    def fill_missing_data(final_merged_df):
        for index, row in final_merged_df[final_merged_df['track_id'].isnull()].iterrows():
            spotify_id = row['spotify_id']
            
            if pd.notna(spotify_id):
                try:
                    track_info = get_track_data(spotify_id)
                    if track_info:
                        final_merged_df.loc[index, 'track_id'] = track_info.get('id')
                        final_merged_df.loc[index, 'artists'] = ', '.join([artist['name'] for artist in track_info.get('artists', [])])
                        final_merged_df.loc[index, 'album_name'] = track_info.get('album', {}).get('name')
                        final_merged_df.loc[index, 'track_name'] = track_info.get('name')
                        final_merged_df.loc[index, 'popularity'] = track_info.get('popularity')
                        final_merged_df.loc[index, 'duration_ms'] = track_info.get('duration_ms')

                        audio_features = get_track_data(spotify_id)  # Change this if you want another function to get audio features
                        if audio_features:
                            final_merged_df.loc[index, 'danceability'] = audio_features.get('danceability')
                            final_merged_df.loc[index, 'energy'] = audio_features.get('energy')
                            final_merged_df.loc[index, 'tempo'] = audio_features.get('tempo')

                    if index % 100 == 0:
                        print(f"Information filled in row {index}")

                    # Add a small pause between each iteration
                    time.sleep(1)

                except Exception as e:
                    print(f"Error processing row {index}: {e}")

        return final_merged_df

    def main():
        # Load the CSV files
        from db_cleaning import transform_db
        from csv_cleaning import transform_csv

        spotify_merge = transform_csv()
        grammy_merge = transform_db()

        # Merge the datasets
        final_merged_df = merge_datasets(grammy_merge, spotify_merge)

        # Fill missing data using the Spotify API
        final_merged_df = fill_missing_data(final_merged_df)

        # Remove unnecessary columns: Unnamed: 0 and track_id
        columns_to_drop = ['Unnamed: 0', 'track_id']

        # Drop the selected columns
        final_merged_df = final_merged_df.drop(columns=columns_to_drop, errors='ignore')

        # Save the final DataFrame to a CSV file
        output_path = os.path.join('data', 'Database_Merged.csv')
        final_merged_df.to_csv(output_path, index=False)
        print(f"Final CSV file saved at: {output_path}")

    if __name__ == "__main__":
        main()

if __name__ == "__main__":
    merge()
