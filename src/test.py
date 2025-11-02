
from pathlib import Path
#from extract import DataExtract
from load import DataLoader
import pandas as pd

path_db = "..\\data\\IATA.db"

aerolineas = """ CREATE TABLE aerolineas (
    id_aerolinea INTEGER NOT NULL PRIMARY KEY,
    nombre TEXT NOT NULL
); """

ciudades = """ CREATE TABLE ciudades (
    id_ciudad INTEGER NOT NULL PRIMARY KEY,
    nombre TEXT NOT NULL
); """

aeropuertos = """ CREATE TABLE aeropuertos (
    id_aeropuerto INTEGER NOT NULL PRIMARY KEY,
    nombre TEXT NOT NULL,
    id_ciudad INTEGER NOT NULL,
    FOREIGN KEY (id_ciudad) REFERENCES ciudades (id_ciudad)
); """

modelos = """ CREATE TABLE modelos (
    id_modelo INTEGER NOT NULL PRIMARY KEY,
    nombre TEXT NOT NULL
); """

aviones = """ CREATE TABLE aviones (
    id_avion INTEGER NOT NULL PRIMARY KEY,
    nombre TEXT NOT NULL,
    id_aerolinea INTEGER NOT NULL,
    id_modelo INTEGER NOT NULL,
    FOREIGN KEY (id_aerolinea) REFERENCES aerolineas (id_aerolinea),
    FOREIGN KEY (id_modelo) REFERENCES modelos (id_modelo)
); """

itinerarios = """ CREATE TABLE itinerarios (
    id_itinerario INTEGER NOT NULL PRIMARY KEY,
    fecha_salida DATETIME NOT NULL,
    fecha_llegada DATETIME NOT NULL,
    id_aeropuerto_origen INTEGER NOT NULL,
    id_aeropuerto_destino INTEGER NOT NULL,
    FOREIGN KEY (id_aeropuerto_origen) REFERENCES aeropuertos (id_aeropuerto),
    FOREIGN KEY (id_aeropuerto_destino) REFERENCES aeropuertos (id_aeropuerto)
); """

usuarios = """ CREATE TABLE usuarios (
    cedula INTEGER NOT NULL PRIMARY KEY,
    nombre TEXT NOT NULL,
    apellido TEXT NOT NULL,
    email TEXT NOT NULL,
    id_ciudad INTEGER NOT NULL,
    FOREIGN KEY (id_ciudad) REFERENCES ciudades (id_ciudad)
); """

vuelos = """ CREATE TABLE vuelos (
    costo INTEGER NOT NULL,
    id_itinerario INTEGER NOT NULL,
    id_avion INTEGER NOT NULL,
    id_usuario INTEGER NOT NULL,
    PRIMARY KEY (id_itinerario, id_avion, id_usuario),
    FOREIGN KEY (id_itinerario) REFERENCES itinerarios (id_itinerario),
    FOREIGN KEY (id_avion) REFERENCES aviones (id_avion),
    FOREIGN KEY (id_usuario) REFERENCES usuarios (cedula)
); """

tables = [
    ("aerolineas", aerolineas),
    ("ciudades", ciudades),
    ("aeropuertos", aeropuertos),
    ("modelos", modelos),
    ("aviones", aviones),
    ("itinerarios", itinerarios),
    ("usuarios", usuarios),
    ("vuelos", vuelos)
]

conn = DataLoader.init_db(path_db = path_db)
for name, create_sql in tables:
    DataLoader.create_table(conn= conn, create_table_sql = create_sql, table_name=name)

data_aerolineas = """ INSERT INTO aerolineas (id_aerolinea, nombre) VALUES
(1, 'Avianca'),
(2, 'Latam'),
(3, 'Wingo'); """

data_ciudades = """ INSERT INTO ciudades (id_ciudad, nombre) VALUES
(1, 'Atenas'),
(2, 'Barcelona'),
(3, 'Bogotá'),
(4, 'Londres'),
(5, 'Madrid'),
(6, 'Medellín'),
(7, 'Montevideo'),
(8, 'Nueva York'),
(9, 'París'),
(10, 'Roma'),
(11, 'Santiago de Cali'),
(12, 'Valencia'); """

data_aeropuertos = """ INSERT INTO aeropuertos (id_aeropuerto, nombre, id_ciudad) VALUES
(1, 'Eleftherios Venizelos', 1),
(2, 'Josep Tarradellas Barcelona-El Prat', 2),
(3, 'El Dorado', 3),
(4, 'London City Airport', 4),
(5, 'Adolfo Suárez Madrid-Barajas', 5),
(6, 'José María Cordova', 6),
(7, 'General Cesáreo Berisso', 7),
(8, 'John F. Kennedy', 8),
(9, 'Charles de Gaulle', 9),
(10, 'Leonardo Da Vinci', 10),
(11, 'Alfonso Bonilla Aragón', 11),
(12, 'Valencia-Manises', 12); """

data_modelos = """ INSERT INTO modelos (id_modelo, nombre) VALUES
(1, 'Airbus 320'),
(2, 'Boeing 747'); """

data_aviones = """ INSERT INTO aviones (id_avion, nombre, id_aerolinea, id_modelo) VALUES
(1, 'Avión 1', 1, 1),
(2, 'Avión 2', 2, 1),
(3, 'Avión 3', 1, 2),
(4, 'Avión 4', 3, 1),
(5, 'Avión 5', 1, 2),
(6, 'Avión 6', 1, 1),
(7, 'Avión 7', 2, 1),
(8, 'Avión 8', 2, 2),
(9, 'Avión 9', 2, 2),
(10, 'Avión 10', 3, 2); """

data_usuarios = """ INSERT INTO usuarios (cedula, nombre, apellido, email, id_ciudad) VALUES
(1143678987, 'Isabela', 'Ochoa', 'isabela.ochoa@gmail.com', 6),
(1122345908, 'Isaac', 'Martínez', 'isaac.martinez@gmail.com', 3),
(1139897656, 'Mariano', 'Gómez', 'mariano.gomez@gmail.com', 6),
(1176890789, 'Erica', 'Cardona', 'erica.cardona@gmail.com', 11),
(1180567232, 'John', 'Kent', 'john.kent@gmail.com', 8),
(1144589787, 'Antonella', 'Esposito', 'antonella.esposito@gmail.com', 10),
(1154387478, 'Howard', 'Lovecraft', 'howard.lovecraft@gmail.com', 4),
(1148906356, 'Rafael', 'Nieto', 'rafael.nieto@gmail.com', 3),
(1157687452, 'Helena', 'Henao', 'helena.henao@gmail.com', 6),
(1123897612, 'Laura', 'Cortés', 'laura.cortes@gmail.com', 3),
(1134689007, 'Isabela', 'García', 'isabela.garcia@gmail.com', 6),
(1140700021, 'Angie', 'Torres', 'angie.torres@gmail.com', 11),
(1147888932, 'David', 'Caicedo', 'david.caicedo@gmail.com', 3),
(1135334567, 'Rafael', 'Calderón', 'rafael.calderon@gmail.com', 3),
(1128890089, 'Leonardo', 'Russo', 'leonardo.russo@gmail.com', 10),
(1134786666, 'Alfredo', 'López', 'alfredo.lopez@gmail.com', 6),
(1179999443, 'Daniela', 'Giraldo', 'daniela.giraldo@gmail.com', 11),
(1167897675, 'Daniel', 'Henao', 'daniel.henao@gmail.com', 6),
(1145790779, 'Marie-Claire', 'Díaz', 'mariec.diaz@gmail.com', 11),
(1130876754, 'Santiago', 'Vargas', 'santiago.vargas@gmail.com', 11); """

data_itinerarios = """ INSERT INTO itinerarios (id_itinerario, fecha_salida, fecha_llegada, id_aeropuerto_origen, id_aeropuerto_destino) VALUES
(1, '2019-03-30 05:00:00', '2019-03-30 05:30:00', 11, 3),
(2, '2019-03-30 08:00:00', '2019-03-30 14:00:00', 3, 5),
(3, '2019-03-30 09:00:00', '2019-03-30 15:00:00', 3, 5),
(4, '2019-04-15 14:00:00', '2019-04-15 20:30:00', 6, 2),
(5, '2019-04-16 08:00:00', '2019-04-16 14:30:00', 6, 2),
(6, '2019-04-16 08:30:00', '2019-04-16 09:00:00', 6, 3),
(7, '2019-04-16 09:00:00', '2019-04-16 15:30:00', 6, 10),
(8, '2019-05-04 07:00:00', '2019-05-04 13:30:00', 3, 2),
(9, '2019-05-10 08:15:00', '2019-05-10 14:30:00', 3, 12),
(10, '2019-05-10 11:00:00', '2019-05-10 17:00:00', 3, 5),
(11, '2019-05-10 13:00:00', '2019-05-10 13:30:00', 11, 3),
(12, '2019-05-20 10:00:00', '2019-05-20 12:00:00', 2, 10),
(13, '2019-05-20 13:00:00', '2019-05-20 19:00:00', 11, 5),
(14, '2019-05-21 08:30:00', '2019-05-20 14:30:00', 11, 5),
(15, '2019-06-15 09:00:00', '2019-06-15 11:00:00', 9, 10),
(16, '2019-06-18 09:00:00', '2019-06-18 15:00:00', 3, 5),
(17, '2019-08-13 07:40:00', '2019-08-13 13:40:00', 3, 5),
(18, '2019-08-13 08:15:00', '2019-08-13 09:15:00', 2, 5),
(19, '2019-08-21 10:00:00', '2019-08-21 16:30:00', 6, 2),
(20, '2019-12-10 14:30:00', '2019-12-10 20:30:00', 3, 12),
(21, '2019-12-12 08:00:00', '2019-12-12 12:10:00', 3, 8),
(22, '2019-12-15 12:00:00', '2019-12-15 16:00:00', 3, 8),
(23, '2019-12-16 08:50:00', '2019-12-16 14:50:00', 3, 8),
(24, '2019-12-20 07:45:00', '2019-12-20 09:45:00', 3, 7),
(25, '2019-12-27 15:00:00', '2019-12-27 21:00:00', 3, 5),
(26, '2020-01-10 08:00:00', '2020-01-10 08:30:00', 11, 3),
(27, '2020-01-14 09:45:00', '2020-01-14 10:15:00', 11, 3),
(28, '2020-01-22 04:45:00', '2020-01-22 05:15:00', 3, 6),
(29, '2020-01-25 10:00:00', '2020-01-25 10:30:00', 3, 11),
(30, '2020-01-30 09:10:00', '2020-01-30 09:40:00', 6, 11),
(31, '2020-02-02 05:50:00', '2020-02-02 06:20:00', 3, 11),
(32, '2020-02-10 09:00:00', '2020-02-10 09:30:00', 3, 11),
(33, '2020-02-17 13:00:00', '2020-02-17 13:30:00', 3, 6),
(34, '2020-02-24 20:00:00', '2020-02-24 20:30:00', 6, 11),
(35, '2020-02-24 21:30:00', '2020-02-24 22:00:00', 3, 11); """

data_vuelos = """ INSERT INTO vuelos (costo, id_itinerario, id_avion, id_usuario) VALUES
(600000, 1, 1, 1180567232),
(650000, 1, 1, 1179999443),
(650000, 1, 1, 1134786666),
(750000, 1, 1, 1144589787),
(650000, 1, 1, 1128890089),
(650000, 1, 1, 1122345908),
(600000, 1, 1, 1157687452),
(600000, 1, 1, 1135334567),
(750000, 1, 1, 1148906356),
(600000, 1, 1, 1167897675),
(3500500, 2, 7, 1123897612),
(3000550, 2, 7, 1134689007),
(3500500, 2, 7, 1143678987),
(3000550, 2, 7, 1176890789),
(3500500, 2, 7, 1140700021),
(3000550, 2, 7, 1147888932),
(3500500, 2, 7, 1145790779),
(3500500, 2, 7, 1139897656),
(3500500, 2, 7, 1130876754),
(3000550, 2, 7, 1154387478),
(4115450, 5, 2, 1139897656),
(4115450, 5, 2, 1123897612),
(4115450, 5, 2, 1134689007),
(4115450, 5, 2, 1176890789),
(4115450, 5, 2, 1135334567),
(4115450, 5, 2, 1180567232),
(4115450, 5, 2, 1154387478),
(4115450, 6, 9, 1128890089),
(600000, 6, 9, 1144589787),
(600000, 6, 9, 1134786666),
(750000, 6, 9, 1147888932),
(600000, 6, 9, 1157687452),
(750000, 6, 9, 1148906356),
(600000, 6, 9, 1167897675),
(3225000, 7, 6, 1140700021),
(3225000, 7, 6, 1122345908),
(3225000, 7, 6, 1145790779),
(3225000, 7, 6, 1143678987),
(3225000, 7, 6, 1130876754),
(3225000, 7, 6, 1179999443),
(4115450, 8, 10, 1122345908),
(4115450, 8, 10, 1147888932),
(4115450, 8, 10, 1135334567),
(4115450, 8, 10, 1143678987),
(4115450, 8, 10, 1144589787),
(4115450, 8, 10, 1179999443),
(4115450, 8, 10, 1176890789),
(4115450, 8, 10, 1145790779),
(2789000, 9, 5, 1139897656),
(2789000, 9, 5, 1140700021),
(2789000, 9, 5, 1180567232),
(2789000, 9, 5, 1167897675),
(3000000, 9, 5, 1148906356),
(2789000, 9, 5, 1134786666),
(2789000, 9, 5, 1154387478),
(3500500, 10, 3, 1130876754),
(3500500, 10, 3, 1128890089),
(3500500, 10, 3, 1123897612),
(750000, 11, 2, 1134689007),
(750000, 11, 2, 1157687452),
(1200000, 12, 1, 1143678987),
(1200000, 12, 1, 1176890789),
(1200000, 12, 1, 1180567232),
(1200000, 12, 1, 1148906356),
(1200000, 12, 1, 1157687452),
(3500500, 13, 5, 1140700021),
(3500500, 13, 5, 1147888932),
(3500500, 13, 5, 1134786666),
(3500500, 13, 5, 1179999443),
(3500500, 13, 5, 1130876754),
(3500500, 13, 5, 1145790779),
(3500500, 13, 5, 1167897675),
(3500500, 13, 5, 1128890089),
(3500500, 14, 2, 1135334567),
(3500500, 14, 2, 1134689007),
(3500500, 14, 2, 1123897612),
(1545000, 15, 4, 1154387478),
(1545000, 15, 4, 1144589787),
(1545000, 15, 4, 1139897656),
(1545000, 15, 4, 1122345908),
(3500500, 16, 10, 1154387478),
(3500500, 16, 10, 1148906356),
(3500500, 17, 7, 1134786666),
(3500500, 17, 7, 1122345908),
(3500500, 17, 7, 1179999443),
(3500500, 17, 7, 1130876754),
(1200000, 18, 8, 1167897675),
(1200000, 18, 8, 1128890089),
(1200000, 18, 8, 1157687452),
(1200000, 18, 8, 1143678987),
(1200000, 18, 8, 1135334567),
(4115450, 19, 9, 1176890789),
(4115450, 19, 9, 1145790779),
(4115450, 19, 9, 1139897656),
(4115450, 19, 9, 1147888932),
(2789000, 20, 1, 1180567232),
(2789000, 20, 1, 1134689007),
(2789000, 20, 1, 1123897612),
(2789000, 20, 1, 1140700021),
(2789000, 20, 1, 1144589787),
(2200500, 21, 6, 1140700021),
(2200500, 21, 6, 1139897656),
(2200500, 21, 6, 1134689007),
(2200500, 21, 6, 1123897612),
(2200500, 21, 6, 1128890089),
(2200500, 22, 7, 1122345908),
(2200500, 22, 7, 1157687452),
(2200500, 22, 7, 1135334567),
(2200500, 22, 7, 1180567232),
(2200500, 23, 7, 1134786666),
(2200500, 23, 7, 1148906356),
(2200500, 23, 7, 1167897675),
(1950999, 24, 3, 1176890789),
(1950999, 24, 3, 1145790779),
(1950999, 24, 3, 1179999443),
(1950999, 24, 3, 1147888932),
(1950999, 24, 3, 1130876754),
(1950999, 24, 3, 1144589787),
(3500500, 25, 4, 1143678987),
(3500500, 25, 4, 1154387478),
(750000, 26, 10, 1123897612),
(750000, 26, 10, 1122345908),
(750000, 26, 10, 1135334567),
(750000, 27, 6, 1148906356),
(750000, 27, 6, 1143678987),
(750000, 27, 6, 1128890089),
(750000, 27, 6, 1134689007),
(750000, 27, 6, 1154387478),
(750000, 27, 6, 1134786666),
(750000, 28, 9, 1176890789),
(750000, 28, 9, 1145790779),
(750000, 28, 9, 1140700021),
(750000, 28, 9, 1157687452),
(750000, 29, 1, 1179999443),
(750000, 29, 1, 1139897656),
(750000, 30, 2, 1167897675),
(750000, 30, 2, 1147888932),
(750000, 30, 2, 1144589787),
(750000, 30, 2, 1130876754),
(750000, 30, 2, 1180567232),
(750000, 31, 5, 1122345908),
(750000, 31, 5, 1148906356),
(750000, 31, 5, 1128890089),
(750000, 32, 8, 1139897656),
(750000, 32, 8, 1157687452),
(750000, 32, 8, 1135334567),
(750000, 32, 8, 1154387478),
(750000, 32, 8, 1179999443),
(750000, 32, 8, 1167897675),
(750000, 33, 4, 1143678987),
(750000, 33, 4, 1145790779),
(750000, 33, 4, 1123897612),
(750000, 33, 4, 1130876754),
(750000, 33, 4, 1144589787),
(820000, 34, 1, 1140700021),
(820000, 34, 1, 1147888932),
(820000, 35, 7, 1180567232),
(820000, 35, 7, 1134786666),
(820000, 35, 7, 1134689007),
(820000, 35, 7, 1176890789); """

datasets = [data_aerolineas, data_ciudades,data_aeropuertos, data_modelos, data_aviones, data_usuarios, data_itinerarios, data_vuelos]

for data in datasets:
    DataLoader.ejecutar_sentencia(conn= conn, query= data)


if __name__ == "__main__":
    initializer = DatabaseInitializer()
    initializer.init_db()
