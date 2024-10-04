def upload_to_google_drive():
    from pydrive.auth import GoogleAuth
    from pydrive.drive import GoogleDrive

    def upload_to_drive(file_path, file_name):
        # Autenticación con Google
        gauth = GoogleAuth()

        # Cargar el archivo de credenciales desde una ruta específica
        gauth.LoadClientConfigFile("credentials/client_secret.json")  # Cambia la ruta aquí

        # Autenticación en el navegador
        gauth.LocalWebserverAuth()

        # Crear cliente de Google Drive
        drive = GoogleDrive(gauth)

        # Crear el archivo a subir
        gfile = drive.CreateFile({'title': file_name})

        # Establecer el contenido del archivo
        gfile.SetContentFile(file_path)

        # Subir el archivo
        gfile.Upload()

        print(f"El archivo '{file_name}' ha sido subido con éxito a Google Drive.")


    # Llamar a la función con la ruta de tu archivo
    csv_file_path = 'data/Grammy_And_Spotify_Merged.csv'
    upload_to_drive(csv_file_path, 'merged.csv')

if __name__ == "__main__":
    upload_to_google_drive()
