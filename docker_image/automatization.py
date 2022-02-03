import pymysql
import requests
import pandas as pd
import numpy as np

"""
Este algoritmo automatiza la extracción de datos de la página http://energiaabierta.cl/ la cual brinda la fuente de información
respecto a los precios de los combustible del país. esta data se actualiza cada cierto tiempo, el cual depende del gobierno. 

esta página entrega un excel y a través de este código de python se extrae, limpia y se conecta con la base de datos de camionGO
para cargar la data de forma automática, utilizando contenedores docker para hacerlo más portable y reducir los tiempos de implementación.

librerías:
    pymysql = para realizar la conexión y poder realizar las querys a la base de datos
    requests = para poder captar el url y obtener su contenido
    pandas y numpy = para crear el dataframe y operar con la data (transformar a lista, operar con los arreglos, etc) 

A. (LINEA 22-40) se configura la conexión a la base de datos
B. (LINEA 44-66) se crean los casos de uso correspondientes a borrar e insertar combustible
C. (LINEA 77-79) se captura la url que contiene el archivo con los datos
D. (LINEA 81-83) se crea un documento excel y se inserta los datos capturados del URL
E. (LINEA 87-104) se remueven los campos que no nos sirven 
F. (LINEA 108-109) se reasigna una id numérica ya que la id que contienen son tipo alfanumérico
G. (LINEA 110-111) se transforma el dataframe a una lista de datos
H. (LINEA 117-119) se instancia la clase DataBase y se llama la función para insertar y cerrar la base de datos


"""





class DataBase:


    def __init__(self):
        self.connection = pymysql.connect(
            host='some_host',
            user='some_user',
            password='some_password',
            db='some_db'
        )

        self.cursor = self.connection.cursor()
        print("CONEXIÓN ESTABLECIDA")
    

    def close(self):
        self.connection.close()
        print("CONEXIÓN CERRADA")

    

    def insert_fuel(self,data,status):
        sql = "INSERT INTO fuel (last_update, street, number, city, state, hour_operation,distribuitor, fuel_93_price, fuel_97_price, diesel_price, fuel_95_price,  latitude_distribuitor, logitude_distribuitor,id) VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
        try:
            if status == 200:
                self.erase_fuel()
                self.cursor.executemany(sql,data)
                self.connection.commit()
                print("inserción exitosa")
            else:
                print("el servidor no tiene status 200")
        except Exception as e:
            raise


    def erase_fuel(self):
        sql = 'DELETE FROM fuel'
        try:
            self.cursor.execute(sql)
            self.connection.commit()
            print("borrado exitoso")
        except Exception as e:
            raise
    

#captura del excel de la web 
url = 'http://datos.energiaabierta.cl/rest/datastreams/257028/data.xls?applyFormat=1'
r = requests.get(url, allow_redirects=True)

#creacion del excel fuel_data.xlsx y asignación a un dataframe
open('fuel_data.xlsx', 'wb').write(r.content)
df = pd.read_excel('fuel_data.xlsx' , keep_default_na=False)


# extracción de los campos innecesarios
df.drop([
    'ID',
    'Razón Social',
    'ID Comuna', 
    'ID Región', 
    'Distribuidor Logo', 
    'Distribuidor Logo SVG', 
    'Distribuidor Logo SVG Horizontal', 
    'GLP Vehicular $/m3', 
    'GNC $/m3', 'Tienda', 
    'Farmacia', 
    'Mantención', 
    'Autoservicio', 
    'Pago Efectivo', 
    'Cheque', 
    'Tarjetas Bancarias', 
    'Tarjeta Grandes Tiendas'],
    axis = 'columns', inplace=True)



# agregar el id y sumar 1 para que no empiece de 0 ya que la bd borra el primer registro con 0 
df['id'] = df.index.values + 1
# transformar los valores una lista normalizada para que se entienda en executemany
data = df.values.tolist()


#llamada a la instancia de la base de datos para insertar el dataframe limpio
db = DataBase()
db.insert_fuel(data,r.status_code)
db.close()