def upload_to_postgres():
    import pandas as pd
    from sqlalchemy import create_engine
    import os
    from dotenv import load_dotenv

    load_dotenv()

    # Paso 1: Lee el CSV
    df = pd.read_csv(os.path.join('data', 'Grammy_And_Spotify_Merged.csv'))  # Lee el CSV en un DataFrame

    # Paso 2: Configura la conexión a PostgreSQL
    usuario = "postgres"  # Reemplaza con tu usuario de PostgreSQL
    password = "root"  # Reemplaza con tu contraseña
    host = "localhost"  # Reemplaza si tu base de datos está en otro servidor
    puerto = 5432  # Puerto por defecto de PostgreSQL
    db = "WorkShop_02"  # Reemplaza con el nombre de tu base de datos

    # Crea la cadena de conexión
    engine = create_engine(f'postgresql://{usuario}:{password}@{host}:{puerto}/{db}')

    # Paso 3: Subir los datos del CSV a una tabla de PostgreSQL
    # Puedes especificar el nombre de la tabla y si deseas reemplazar o añadir los datos
    nombre_tabla = "merged"   # Reemplaza con el nombre de tu tabla
    df.to_sql(nombre_tabla, engine, if_exists='replace', index=False)

    print(f'Datos subidos a la tabla {nombre_tabla} correctamente')

if __name__ == "__main__":
    upload_to_postgres()

