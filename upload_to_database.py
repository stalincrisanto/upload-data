import pandas as pd
import os
import psycopg2
import time

# Configuración de la conexión a PostgreSQL
conexion = psycopg2.connect(
    host="tuservidor",  # Cambia esto por tu host de PostgreSQL
    database="tubasedatos",  # Nombre de la base de datos
    user="tuusuario",  # Usuario de PostgreSQL
    password="tucontraseña",  # Contraseña de PostgreSQL
)


# Función para ejecutar el comando COPY en PostgreSQL
def insertar_csv_postgresql(conexion, archivo_csv, tabla):
    start_time = time.time()  # Iniciar tiempo
    with conexion.cursor() as cursor:
        with open(archivo_csv, "r", encoding="latin1") as f:
            cursor.copy_expert(
                f"COPY {tabla} (id, dateDocModificado) FROM STDIN WITH CSV HEADER DELIMITER ','",
                f,
            )
        conexion.commit()
    end_time = time.time()  # Finalizar tiempo
    elapsed_time = end_time - start_time
    print(
        f"Datos insertados desde {archivo_csv} en la tabla {tabla}. Tiempo tomado: {elapsed_time:.2f} segundos."
    )


# Cargar el CSV original
file_path = r"C:\Users\stalin.crisanto\Desktop\srimvmn_202406.csv"
df = pd.read_csv(file_path, encoding="latin1", sep=";", low_memory=False)

# Selecciona las columnas que quieres mantener
columnas_a_conservar = ["id", "dateDocModificado"]
df_filtrado = df[columnas_a_conservar]

# Definir el número de registros por archivo temporal
registros_por_archivo = 500000  # Puedes ajustar este valor

# Carpeta para almacenar los archivos temporales
carpeta_temporales = "archivos_temporales"
os.makedirs(carpeta_temporales, exist_ok=True)

# Nombre de la tabla en PostgreSQL
tabla_destino = "nombre_de_tu_tabla"

# Medir el tiempo total del proceso
tiempo_inicio_total = time.time()

# Dividir y cargar los archivos
for i, chunk in enumerate(range(0, len(df_filtrado), registros_por_archivo)):
    df_temporal = df_filtrado.iloc[chunk : chunk + registros_por_archivo]

    # Guardar cada chunk en un archivo CSV separado
    archivo_temporal = os.path.join(carpeta_temporales, f"archivo_temporal_{i + 1}.csv")
    df_temporal.to_csv(archivo_temporal, index=False)
    print(f"Archivo generado: {archivo_temporal}")

    # Insertar el archivo temporal en PostgreSQL
    insertar_csv_postgresql(conexion, archivo_temporal, tabla_destino)

# Calcular el tiempo total de ejecución
tiempo_fin_total = time.time()
tiempo_total = tiempo_fin_total - tiempo_inicio_total
print(f"Proceso completado. Tiempo total de inserción: {tiempo_total:.2f} segundos.")

# Cerrar la conexión a la base de datos
conexion.close()
