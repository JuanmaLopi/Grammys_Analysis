import psycopg2
import sys
import os
sys.path.append(os.path.abspath(os.path.join('..', 'credentials')))
from Credentials import usuario, Password, Host, puerto, db

def establecer_conexion():
    dbname=db
    user=usuario
    password=Password
    host=Host
    port=puerto

    conn = psycopg2.connect(dbname=dbname, user=user, password=password, host=host, port=port)
    print("Conexion exitosa a la base de datos")
    cursor = conn.cursor()

    return conn, cursor

def cerrar_conexion(conn):
    conn.close()
    print("Conexion cerrada a la base de datos")


