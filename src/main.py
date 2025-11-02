
from pathlib import Path
#from extract import DataExtract
from load import DataLoader
import pandas as pd
from db_oltp import DatabaseInitializer

initializer = DatabaseInitializer()
initializer.init_db()


### Creación de DIM's

dim_aerolinea = """ CREATE TABLE dim_aerolinea (
    id_aerolinea INTEGER PRIMARY KEY,
    nombre TEXT
); """

dim_ciudad = """ CREATE TABLE dim_ciudad (
    id_ciudad INTEGER PRIMARY KEY,
    nombre TEXT
); """

dim_aeropuerto = """ CREATE TABLE dim_aeropuerto (
    id_aeropuerto INTEGER PRIMARY KEY,
    nombre TEXT,
    id_ciudad INTEGER
); """

dim_modelo = """ CREATE TABLE dim_modelo (
    id_modelo INTEGER PRIMARY KEY,
    nombre TEXT
);
 """
dim_avion = """ CREATE TABLE dim_avion (
    id_avion INTEGER PRIMARY KEY,
    nombre TEXT,
    id_aerolinea INTEGER,
    id_modelo INTEGER
); """

dim_usuario = """ CREATE TABLE dim_usuario (
id_usuario INTEGER PRIMARY KEY,
nombre TEXT,
apellido TEXT,
email TEXT,
id_ciudad INTEGER
); """

dim_calendar= """ CREATE TABLE dim_calendar (
    id_tiempo INTEGER PRIMARY KEY,
    fecha DATE,
    anio INTEGER,
    mes INTEGER,
    dia INTEGER
);
 """

tables = [
    ("dim_aerolinea", dim_aerolinea),
    ("dim_ciudad", dim_ciudad),
    ("dim_aeropuerto", dim_aeropuerto),
    ("dim_modelo", dim_modelo),
    ("dim_avion", dim_avion),
    ("dim_calendar", dim_calendar),
    ("dim_usuario", dim_usuario)
]


path_oltp = "../data/IATA.db"
path_dm = "../data/DataMart.db"

conn_oltp = DataLoader.init_db(path_db = path_oltp)
conn_dm = DataLoader.init_db(path_db = path_dm)

""" src = conn_oltp.cursor() 
dst = conn_dm.cursor() """

for name, create_sql in tables:
    DataLoader.create_table(conn= conn_dm, create_table_sql = create_sql, table_name=name)

tablas = [
    ("dim_aerolinea", "SELECT id_aerolinea, nombre FROM aerolineas;"),
    ("dim_ciudad", "SELECT id_ciudad, nombre FROM ciudades;"),
    ("dim_aeropuerto", "SELECT id_aeropuerto, nombre, id_ciudad FROM aeropuertos;"),
    ("dim_modelo", "SELECT id_modelo, nombre FROM modelos;"),
    ("dim_avion", "SELECT id_avion, nombre, id_aerolinea, id_modelo FROM aviones;"),
    ("dim_usuario", "SELECT cedula AS id_usuario, nombre, apellido, email, id_ciudad FROM usuarios;")
]

for nombre_tabla, query in tablas:
    df = pd.read_sql_query(query, conn_oltp)
    df.to_sql(nombre_tabla, conn_dm, if_exists="replace", index=False)

qry_rango_fecha = """
SELECT 
    MIN(fecha_salida) AS fecha_min, 
    MAX(fecha_salida) AS fecha_max
FROM itinerarios;
"""
df_rango_fecha = pd.read_sql_query(qry_rango_fecha, conn_oltp)
fecha_inicio = pd.to_datetime(df_rango_fecha["fecha_min"][0])
fecha_fin = pd.to_datetime(df_rango_fecha["fecha_max"][0])

fechas = pd.date_range(start=fecha_inicio, end=fecha_fin, freq='D').normalize()

df_calendar = pd.DataFrame({
    "id_tiempo": [int(fecha.strftime("%Y%m%d")) for fecha in fechas],
    "fecha": fechas.date,
})
df_calendar["anio"] = df_calendar["fecha"].apply(lambda x: x.year)
df_calendar["mes"] = df_calendar["fecha"].apply(lambda x: x.month)
df_calendar["dia"] = df_calendar["fecha"].apply(lambda x: x.day)

df_calendar.to_sql("dim_calendar", conn_dm, if_exists="replace", index=False)



print("Dimensiones creadas y cargadas correctamente.")

##Creación de Fac

create_fact = """
CREATE TABLE IF NOT EXISTS fact_vuelos (
    id_vuelos INTEGER,
    id_avion INTEGER,
    id_usuario INTEGER,
    id_tiempo_salida INTEGER,
    id_tiempo_llegada INTEGER,
    id_aerolinea INTEGER,
    id_ciudad_usuario INTEGER,
    id_ciudad_aeropuerto_origen INTEGER,
    id_ciudad_aeropuerto_destino INTEGER,
    id_modelo INTEGER,
    costo REAL,
    FOREIGN KEY (id_avion) REFERENCES dim_avion(id_avion),
    FOREIGN KEY (id_usuario) REFERENCES dim_usuario(id_usuario),
    FOREIGN KEY (id_tiempo_salida) REFERENCES dim_calendar(id_tiempo),
    FOREIGN KEY (id_tiempo_llegada) REFERENCES dim_calendar(id_tiempo),
    FOREIGN KEY (id_aerolinea) REFERENCES dim_aerolinea(id_aerolinea),
    FOREIGN KEY (id_ciudad_usuario) REFERENCES dim_ciudad(id_ciudad),
    FOREIGN KEY (id_ciudad_aeropuerto_origen) REFERENCES dim_ciudad(id_ciudad),
    FOREIGN KEY (id_ciudad_aeropuerto_destino) REFERENCES dim_ciudad(id_ciudad),
);
"""
DataLoader.create_table(conn= conn_dm, create_table_sql = create_fact, table_name="fact_vuelos")

# Insertar Datos Fact
insert_fact = """
    SELECT 
        vuelos.id_itinerario as id_vuelos,
        vuelos.id_avion,
        vuelos.id_usuario,
        CAST(STRFTIME('%Y%m%d', itinerarios.fecha_salida) AS INTEGER) AS id_tiempo_salida,
        CAST(STRFTIME('%Y%m%d', itinerarios.fecha_llegada) AS INTEGER) AS id_tiempo_llegada,
        aviones.id_aerolinea,
        usuarios.id_ciudad as id_ciudad_usuario,
        aeropuertoorigen.id_ciudad as id_ciudad_aeropuerto_origen,
        aeropuertodestino.id_ciudad as id_ciudad_aeropuerto_destino,
        modelos.id_modelo,
        vuelos.costo
    FROM vuelos
        LEFT JOIN itinerarios
            ON vuelos.id_itinerario = itinerarios.id_itinerario
        LEFT JOIN aviones
            ON vuelos.id_avion = aviones.id_avion
        LEFT JOIN usuarios
            ON vuelos.id_usuario = usuarios.cedula
        LEFT JOIN modelos
            ON aviones.id_modelo = modelos.id_modelo 
        LEFT JOIN aeropuertos AS aeropuertoorigen
            ON itinerarios.id_aeropuerto_origen = aeropuertoorigen.id_aeropuerto
        LEFT JOIN aeropuertos AS aeropuertodestino
            ON itinerarios.id_aeropuerto_destino = aeropuertodestino.id_aeropuerto 
"""
df_fac = pd.read_sql_query(insert_fact, conn_oltp)
df_fac.to_sql("fact_vuelos", conn_dm, if_exists="replace", index=False)
