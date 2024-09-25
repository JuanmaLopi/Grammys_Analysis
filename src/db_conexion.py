import psycopg2

def establecer_conexion():
    dbname='WorkShop_02'
    user='postgres'
    password='root'
    host='localhost'
    port='3000'

    conn = psycopg2.connect(dbname=dbname, user=user, password=password, host=host, port=port)
    print("Conexion exitosa a la base de datos")
    cursor = conn.cursor()

    return conn, cursor

def cerrar_conexion(conn):
    conn.close()
    print("Conexion cerrada a la base de datos")
