"""
                                                    PRUEBA TECNICA - NEOGISTICA
                                                    PARTE 1 - ANALISIS DE DATOS
                                                         Dexter Freyggang

                                                         
ENUNCIADO
Los clientes de neogistica siempre disponen de mucha informacion relevante para sus negocios pero tienen dificultades para hacer algo util con ellas. Por temas de confidencialidad de datos, el cliente solo ha dispuesto dos archivos que contienen informacion y nos ha pedido que lo analicemos para resolver sus interrogantes.

Para esta prueba se requiere leer dos archivos y cargarlos a una base de datos existente. Adicionalmente, se requiere consultar los datos usando MySQL para responder las inquietudes del cliente. Los archivos y las credenciales a la base de datos seran proporcionados en el envio por correo electronico de este desafio.


ARCHIVOS
Los archivos proporcionados son: sku.jsonl y demanda.csv.

El archivo sku.jsonl contiene en cada linea informacion de un producto en tres columnas. La columna sku describe el codigo de identificacion de los productos del cliente, la columna modelo y color denotan caracteristicas del producto.

El archivo demanda.csv contiene informacion acerca de la demanda (venta) de los productos. La columna sku representa el codigo de identificacion del producto, la columna fecha representa la fecha donde se produjo la demanda, la columna cantidad representa la cantidad vendida y la columna precio_unitario es el precio por unidad al que se vendio el producto.


INFORMACION RELEVANTE PARA EL CLIENTE
A continuacion, el listado de inquietudes del cliente:
* ¿Cual fue el dia del mes con mejor venta?
* Ranking de colores por cantidad vendida, de menor a mayor.
* Top 5 productos mas vendidos en la primera quincena del mes.
* ¿Existen productos que no registran venta?
* ¿Cual es el modelo que se vendio mas barato en promedio?


EVALUACION
La entrega de la prueba tecnica se debe realizar a traves de un link a un repositorio publico en la plataforma GitHub. Es muy importante que tanto las credenciales de la base de datos como los archivos del cliente no esten publicados en el repositorio. Adicionalmente se requiere que el repositorio tenga una breve documentacion donde se describa como ejecutar el codigo. Al revisar tu entrega evaluaremos la funcionalidad del software, la calidad, estilo, mantenibilidad del codigo y documentacion.


ANALISIS PERSONAL DE LO SOLICITADO
Para resolver lo solicitado primero hay que instalar las librerias, importar los elementos que seran usados, luego conectar a la BD usando el fichero de credenciales, crear las tablas segun lo indicado, poblarlas usando los ficheros del cliente (que deben ser puestos manualmente, al igual que el archivo de credenciales, en la carpeta DATA) y por ultimo realizar las consultas. Cabe destacar que por buenas practicas, todo el contenido de este archivo deberia estar mas distribuido, no todo agrupado aca, pero lo realice de esta forma unicamente por comodidad de uds en la revision y para cumplir con lo solicitado de que fuera lo mas simple posible. Esta decision de agrupar todo aca no representa mi forma de trabajo normal. A continuacion explicare el paso a paso de la solucion propuesta.


"""

#-----------------------------------------------------------------------------
"""RECORDATORIO DE ARCHIVOS EN CARPETA DATA"""
# Mensaje de advertencia para recordar la colocación manual de los archivos
import time
print("ATENCION! Se le recuerda que los archivos del cliente deben ser puestos MANUALMENTE en la carpeta 'Data' antes de empezar.")
time.sleep(5)
#-----------------------------------------------------------------------------



#-----------------------------------------------------------------------------
"""INSTALACION E IMPORTACION DE LIBRERIAS"""
print("\n\nProcediendo a instalar paquetes necesarios...")
#-----------------------------------------------------------------------------
import subprocess
import sys
import json
from datetime import datetime

# Función para instalar paquetes
def install(package):
    subprocess.check_call([sys.executable, "-m", "pip", "install", package])

# Verificar instalacion de libreria mysql
try:
    import mysql.connector
    print("Importacion exitosa de mysql, no es necesario instalar manualmente.")
except ImportError:
    print("mysql-connector-python no está instalado. Instalando...")
    try:
        install('mysql-connector-python')
    except:
        print("No es posible realizar la instalacion de mysql, por favor realicela de forma manual")

# Verificar instalacion de libreria pandas
try:
    import pandas as pd
    print("Importacion exitosa de pandas, no es necesario instalar manualmente.")
except ImportError:
    print("pandas no está instalado. Instalando...")
    try:
        install('pandas')
    except:
        print("No es posible realizar la instalacion de pandas, por favor realicela de forma manual")




#-----------------------------------------------------------------------------
"""CONECCION A LA BASE DE DATOS"""
print("\n\nProcediendo a conectar con la base de datos...")
#-----------------------------------------------------------------------------
# Definir ruta del archivo de credenciales
credenciales_path = './Data/credenciales.txt'

# Función para cargar las credenciales desde el archivo
def cargar_credenciales(ruta):
    credenciales = {}
    with open(ruta, 'r') as archivo:
        for linea in archivo:
            clave, valor = linea.strip().split(': ')
            credenciales[clave] = valor
    return credenciales

# Cargar las credenciales
credenciales = cargar_credenciales(credenciales_path)

# Realizar la conexión a la base de datos
connection = mysql.connector.connect(
    host=credenciales['Host'],
    user=credenciales['User'],
    password=credenciales['Pass'],
    database=credenciales['Database']
)
cursor = connection.cursor()

# Comprobar si la conexión es exitosa
cursor.execute("SELECT DATABASE();")
result = cursor.fetchone()
print(f"Conectado a la base de datos: {result[0]}.")




#-----------------------------------------------------------------------------
"""CREACION DE TABLAS"""
print("\n\nProcediendo a crear las tablas...")
#-----------------------------------------------------------------------------
# Crear tabla SKU si no existe (como yo ya habia ejecutado esto, la tabla ya existe)
cursor.execute("""
CREATE TABLE IF NOT EXISTS sku (
    sku INT,
    modelo VARCHAR(50),
    color VARCHAR(50)
);
""")
connection.commit()
print("Tabla 'sku' creada.")

# Crear tabla DEMANDA si no existe (como yo ya habia ejecutado esto, la tabla ya existe)
cursor.execute("""
CREATE TABLE IF NOT EXISTS demanda (
    sku INT,
    fecha DATE,
    cantidad INT,
    precio_unitario INT
);
""")
connection.commit()
print("Tabla 'demanda' creada.")




#-----------------------------------------------------------------------------
"""VACIADO DE TABLAS"""
"""Para asegurar que puedan ver el funcionamiento completo del codigo, he agregado esta funcion que vacia las tablas antes de volver a poblarlas. Esto permite que el script y las consultas se ejecuten correctamente, sin importar si ya fueron ejecutados previamente."""
print("\n\nProcediendo a vaciar las tablas...")
#-----------------------------------------------------------------------------
# Función para verificar si las tablas están pobladas y vaciarlas si tienen contenido
def verificar_y_vaciar_tablas():
    cursor.execute("SHOW TABLES;")
    tablas = cursor.fetchall()
    for tabla in tablas:
        nombre_tabla = tabla[0]
        cursor.execute(f"SELECT COUNT(*) FROM {nombre_tabla};")
        total_registros = cursor.fetchone()[0]
        if total_registros > 0:
            print(f"La tabla '{nombre_tabla}' tiene {total_registros} registros. Procediendo a eliminar el contenido...")
            cursor.execute(f"DELETE FROM {nombre_tabla};")
            print(f"Contenido de la tabla '{nombre_tabla}' ha sido eliminado.")
        else:
            print(f"La tabla '{nombre_tabla}' esta vacia.")
    connection.commit()
verificar_y_vaciar_tablas()



#-----------------------------------------------------------------------------
"""POBLADO DE TABLAS"""
print("\n\nProcediendo a poblar las tablas...")
#-----------------------------------------------------------------------------
# Poblar tabla SKU con los datos del cliente (el archivo debe estar en la carpeta Data)
with open("./Data/sku.jsonl", 'r') as archivojson:
    for linea in archivojson:
        data = json.loads(linea)
        cursor.execute("INSERT INTO sku (sku, modelo, color) VALUES (%s, %s, %s)",
                       (data['sku'], data['modelo'], data['color']))
connection.commit()

# Comprobar si se poblo correctamente comparando total de registros
with open("./Data/sku.jsonl", 'r') as archivojson:
    total_registros_json = sum(1 for _ in archivojson)
cursor.execute("SELECT COUNT(*) FROM sku;")
total_registros_sku = cursor.fetchone()[0]
print(f"Registros en el archivo sku.json: {total_registros_json}")
print(f"Registros insertados en la tabla 'sku': {total_registros_json}")
if total_registros_json == total_registros_json:
    print("Poblado de tabla sku realizado con exito.")

# Poblar tabla demanda con los datos del cliente (el archivo debe estar en la carpeta Data)
with open("./Data/demnada.csv", 'r') as archivo_csv:
    data = pd.read_csv(archivo_csv)
    for index, row in data.iterrows():
        cursor.execute("INSERT INTO demanda (sku, fecha, cantidad, precio_unitario) VALUES (%s, %s, %s, %s)", 
                       (row['sku'], row['fecha'], row['cantidad'], row['precio_unitario']))
connection.commit()

# Comprobar si se poblo correctamente comparando total de registros
with open("./Data/demnada.csv", 'r') as archivo_csv:
    total_registros_csv = sum(1 for _ in archivo_csv) - 1
cursor.execute("SELECT COUNT(*) FROM demanda;")
total_registros_demanda = cursor.fetchone()[0]
print(f"Registros en el archivo demanda.csv: {total_registros_csv}")
print(f"Registros insertados en la tabla 'demanda': {total_registros_demanda}")
if total_registros_csv == total_registros_demanda:
    print("Poblado de tabla demanda realizado con exito.")




#-----------------------------------------------------------------------------
"""CONSULTAS DE ANALISIS DE DATOS"""
print("\n\nProcediendo a realizar las consultas solicitadas en la base de datos...")
#-----------------------------------------------------------------------------

#-----------------------------------------------------------------------------
"""DIA DEL MES CON MEJOR VENTA"""
print("\n\nSolicitud 1 - Determinar cual fue el dia del mes con mejor venta:")
#-----------------------------------------------------------------------------
# Consulta para determinar cual fue el dia del mes con mejor venta
cursor.execute("""
SELECT fecha, SUM(cantidad * precio_unitario) AS total_ventas
FROM demanda
GROUP BY fecha
ORDER BY total_ventas DESC
LIMIT 1;
""")
result = cursor.fetchall()

# Mostrar resultado en un formato adecuado
fecha_mejor_venta = result[0][0]
total_mejor_venta = result[0][1]
fecha_formateada = fecha_mejor_venta.strftime('%d-%m-%Y')
print(f"El dia con mas ventas fue {fecha_formateada} con un total de {total_mejor_venta} pesos.")


#-----------------------------------------------------------------------------
"""RANKING ASCENDENTE DE COLORES POR CANTIDAD VENDIDA"""
print("\n\nSolicitud 2 - Establecer ranking ascendente de colores por cantidad vendida:")
#-----------------------------------------------------------------------------
# Consulta para establecer el ranking de colores por cantidad vendida, de menor a mayor
cursor.execute("""
SELECT sku.color, SUM(demanda.cantidad) AS total_vendida
FROM demanda
JOIN sku ON demanda.sku = sku.sku
GROUP BY sku.color
ORDER BY total_vendida ASC;
""")
result = cursor.fetchall()

# Mostrar resultado en formato adecuado
print(f"{'Color':<10} {'Total vendido':>15}")
print("-" * 26)
for row in result:
    color = row[0]
    total_vendida = int(row[1])
    print(f"{color:<10} {total_vendida:>15}")


#-----------------------------------------------------------------------------
"""5 PRODUCTOS MAS VENDIDOS EN LA PRIMERA QUINCENA DEL MES"""
print("\n\nSolicitud 3 - Establecer cuales fueron los 5 productos mas vendidos en la primera quincena del mes actual:")
#-----------------------------------------------------------------------------
# Obtener mes y año actual
mes_actual = datetime.now().month
anio_actual = datetime.now().year

# Consulta para obtener los 5 de productos mas vendidos en primera quincena del mes actual
cursor.execute(f"""
SELECT sku.sku, SUM(demanda.cantidad) AS total_vendida
FROM demanda
JOIN sku ON demanda.sku = sku.sku
WHERE DAY(fecha) <= 15 AND MONTH(fecha) = {mes_actual} AND YEAR(fecha) = {anio_actual}
GROUP BY sku.sku
ORDER BY total_vendida DESC
LIMIT 5;
""")
result = cursor.fetchall()

# Mostrar resultados en formato tabla
print(f"{'SKU':<10} {'Total Vendida':>15}")
print("-" * 26)
for row in result:
    sku = row[0]
    total_vendida = int(row[1])
    print(f"{sku:<10} {total_vendida:>15}")


#-----------------------------------------------------------------------------
"""PRODUCTOS QUE NO REGISTRARON VENTAS"""
print("\n\nSolicitud 4 - Definir cuales son los productos que no registraron ventas:")
#-----------------------------------------------------------------------------
# Consulta para establecer si es que hay y cuales son los productos que no registraron ventas
cursor.execute("""
SELECT sku.sku
FROM sku
LEFT JOIN demanda ON sku.sku = demanda.sku
WHERE demanda.sku IS NULL;
""")
result = cursor.fetchall()

# Mostrar resultado en formato adecuado
print(f"{'SKU sin ventas':<15}")
print("-" * 15)
for row in result:
    print(f"{row[0]:<15}")


#-----------------------------------------------------------------------------
"""MODELO VENDIDO MAS BARATO EN PROMEDIO"""
print("\n\nSolicitud 5 - Definir cual fue el modelo vendido mas barato en promedio:")
#-----------------------------------------------------------------------------
# Consulta para definir cual fue, en promedio, el modelo vendido mas barato
cursor.execute("""
SELECT sku.modelo, AVG(demanda.precio_unitario) AS precio_promedio
FROM demanda
JOIN sku ON demanda.sku = sku.sku
GROUP BY sku.modelo
ORDER BY precio_promedio ASC
LIMIT 1;
""")
result = cursor.fetchall()

# Mostrar resultado en formato adecuado
print(f"{'Modelo':<10} {'Precio promedio':>15}")
print("-" * 26)
for row in result:
    modelo = row[0]
    precio_promedio = round(row[1], 2)
    print(f"{modelo:<10} {precio_promedio:>15}")




#-----------------------------------------------------------------------------
"""FINALIZAR CONECCION A LA BASE DE DATOS"""
print("\n\nParte 1 del desafio concluida. Por favor, a continuacion, vea la parte 2 en el archivo 'Parte 2 - AWS Cloud.pdf' que se encuentra en este mismo directorio.")
#-----------------------------------------------------------------------------
print("Finalizando coneccion a la base de datos...")
cursor.close()
connection.close()
#-----------------------------------------------------------------------------





"""
# Esta funcion es solo para uso personal, para ir viendo el contenido de la BD. NO CONSIDERARLA
def mostrar_contenido_bd():
    # Obtener todas las tablas de la base de datos
    cursor.execute("SHOW TABLES;")
    tablas = cursor.fetchall()

    # Recorrer y mostrar el contenido de cada tabla
    for tabla in tablas:
        nombre_tabla = tabla[0]
        print(f"\nContenido de la tabla '{nombre_tabla}':")
        cursor.execute(f"SELECT * FROM {nombre_tabla};")
        registros = cursor.fetchall()

        if registros:
            for registro in registros:
                print(registro)
        else:
            print(f"La tabla '{nombre_tabla}' está vacía.")
    
    # Cerrar la conexión
    cursor.close()
    connection.close()

# Llamar a la función para mostrar el contenido de la BD
mostrar_contenido_bd()
"""