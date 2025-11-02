import sqlite3
import pandas as pd
import os


class DataLoader:

    @staticmethod
    def insert_data(conn, table_name: str, df: pd.DataFrame):
        try:
            total_rows = len(df)
            df.to_sql(table_name, conn, if_exists="replace", index=False)

            return print(f"{total_rows} registros insertados en tabla '{table_name}'")

        except Exception as e:
            
            return print(f"[ERROR] Error insertando en tabla '{table_name}': {e}")
    
    @staticmethod
    def init_db(path_db: str):
        try:
            if os.path.exists(path_db):
                print(f"La BD ya existe: {path_db}")
                conn = sqlite3.connect(path_db)
                return conn
            else:
                conn = sqlite3.connect(path_db)
                log = f"BD creada: {path_db}"
                return conn
        except Exception as e:
            print(f"[ERROR] Error creando la bd '{path_db}': {e}")   

    @staticmethod
    def create_table(conn, create_table_sql: str, table_name: str):
        try:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT name FROM sqlite_master 
                WHERE type='table' AND name=?;
            """, (table_name,))
            table_exists = cursor.fetchone()

            if table_exists:
                return print(f"La tabla '{table_name}' ya existe.")
            
            cursor.execute("PRAGMA foreign_keys = ON;")
            cursor.execute(create_table_sql)
            conn.commit()
            cursor.close()
            return print(f"Tabla '{table_name}' creada correctamente.")

        except Exception as e:
            return print(f"[ERROR] Error creando tabla '{table_name}': {e}")

    @staticmethod
    def run_query(conn, query: str) -> pd.DataFrame:

        try:

            df = pd.read_sql_query(query, conn)
            print(f"Consulta ejecutada correctamente sobre")
            return df

        except Exception as e:
            print(f"[ERROR] No se pudo ejecutar el query: {e}")
            return pd.DataFrame()
        
    @staticmethod
    def ejecutar_sentencia(conn, query: str, fetch: bool = False) ->  None:
        try:
            cursor = conn.cursor()
            cursor.execute(query)

            if fetch:
                columnas = [desc[0] for desc in cursor.description]
                datos = cursor.fetchall()
                return pd.DataFrame(datos, columns=columnas)
            else:
                conn.commit()

        except sqlite3.Error as e:
            print(f"Error al ejecutar la sentencia:\n{query}\nDetalles: {e}")
            conn.rollback()
            return None
        finally:
            cursor.close()

