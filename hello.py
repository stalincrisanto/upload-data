import pandas as pd
import chardet

# Cargar el CSV
file_path = r'C:\Users\stalin.crisanto\Desktop\srimvmn_202406.csv'
df = pd.read_csv(file_path, encoding='latin1', sep=";", low_memory=False)

# Verifica los primeros registros del archivo
# print(df.head())
# print(df.columns)

# Selecciona las columnas que quieres mantener
columnas_a_conservar = ['id','dateDocModificado']  # Especifica tus columnas
df_filtrado = df[columnas_a_conservar]

# Guardar el DataFrame filtrado en un nuevo archivo CSV
df_filtrado.to_csv('archivo_filtrado.csv', index=False)
