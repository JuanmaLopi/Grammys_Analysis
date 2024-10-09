def upload_to_google_drive():
    from pydrive.auth import GoogleAuth
    from pydrive.drive import GoogleDrive

    def upload_to_drive(file_path, file_name):
        # Authentication with Google
        gauth = GoogleAuth()

        # Load the credentials file from a specific path
        gauth.LoadClientConfigFile("credentials/client_secret.json")  # Change the path here

        # Authenticate in the browser
        gauth.LocalWebserverAuth()

        # Create Google Drive client
        drive = GoogleDrive(gauth)

        # Create the file to upload
        gfile = drive.CreateFile({'title': file_name})

        # Set the content of the file
        gfile.SetContentFile(file_path)

        # Upload the file
        gfile.Upload()

        print(f"The file '{file_name}' has been successfully uploaded to Google Drive.")

    # Call the function with your file path
    csv_file_path = 'data/Grammy_And_Spotify_Merged.csv'
    upload_to_drive(csv_file_path, 'merged.csv')

if __name__ == "__main__":
    upload_to_google_drive()

