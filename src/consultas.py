import sqlite3
import pandas as pd
from load import DataLoader

path_oltp = "../data/IATA.db"
path_dm = "../data/DataMart.db"

conn_oltp = DataLoader.init_db(path_db = path_oltp)
conn_dm = DataLoader.init_db(path_db = path_dm)

qry = """
    SELECT 
        dim_calendar.anio,
        dim_aerolinea.nombre AS Aerolinea,
        sum(fact_vuelos.costo) AS Total_Semestre
    FROM fact_vuelos
    LEFT JOIN dim_avion ON fact_vuelos.id_avion = dim_avion.id_avion
    LEFT JOIN dim_aerolinea ON fact_vuelos.id_aerolinea = dim_aerolinea.id_aerolinea
    LEFT JOIN dim_calendar ON fact_vuelos.id_tiempo_salida = dim_calendar.id_tiempo
    LEFT JOIN dim_ciudad ON fact_vuelos.id_ciudad_aeropuerto_destino = dim_ciudad.id_ciudad 
    WHERE dim_calendar.mes IN (1, 2, 3, 4, 5, 6)
    and dim_calendar.anio IN (2019, 2020)
    GROUP BY dim_calendar.anio, dim_aerolinea.nombre
    ORDER BY dim_calendar.anio, Total_Semestre desc
 """


df = pd.read_sql_query(qry, conn_dm)

colum = df.columns
print(df.head(10))