#%%Paso 2.- Creación de perfiles de datos e informes de calidad
import pandas as pd  # Librería para manipulación y análisis de datos en estructuras como DataFrames.
from ydata_profiling import ProfileReport  # Herramienta para generar informes exploratorios automáticos de datos.
import os  # Librería para interactuar con el sistema de archivos

# Lista de archivos CSV a procesar
csv_files = [
    "city_day.csv",
    "city_hour.csv",
    "station_day.csv",
    "station_hour.csv",
    "stations.csv"
]

# Ruta de la carpeta landing_zone y raw_zone
landing_zone_path = "C:\\Users\\jalvarezg1601\\Desktop\\ESCOM\\CUARTO SEMESTRE\\Bases de Datos Avanzadas Recurse\\Proyecto\\Proyecto Semestral\\landing_zone\\"
raw_zone_path = "C:\\Users\\jalvarezg1601\\Desktop\\ESCOM\\CUARTO SEMESTRE\\Bases de Datos Avanzadas Recurse\\Proyecto\\Proyecto Semestral\\raw_zone\\"

# Ruta de la carpeta donde se guardarán los reportes de calidad de datos
data_quality_reports_path = os.path.join(raw_zone_path, "data_quality_reports")

# Crear la carpeta de data_quality_reports si no existe
os.makedirs(data_quality_reports_path, exist_ok=True)

# Procesar cada archivo CSV en la lista
for file in csv_files:
    # Construir la ruta completa del archivo
    file_path = os.path.join(landing_zone_path, file)

    # Verificar si el archivo existe
    if os.path.exists(file_path):
        # Leer el archivo CSV como un DataFrame
        df = pd.read_csv(file_path)

        # Generar el informe de Data Profiling
        profile = ProfileReport(df, title=f"Data Profiling Report for {file}", explorative=True)

        # Ruta para guardar el reporte HTML
        output_path = os.path.join(data_quality_reports_path, f"{file.replace('.csv', '_report.html')}")

        # Guardar el informe en formato HTML
        profile.to_file(output_path)

        print(f"Reporte generado y guardado en: {output_path}")
    else:
        print(f"El archivo {file} no se encuentra en la carpeta {landing_zone_path}.")


#%%Paso 3.- Transformación y almacenamiento de datos
import os  # Proporciona funciones para interactuar con el sistema operativo, como manipulación de archivos y directorios.
import pandas as pd  # Librería para manipulación y análisis de datos en estructuras como DataFrames.

# Rutas de las carpetas
landing_zone_path = "C:\\Users\\jalvarezg1601\\Desktop\\ESCOM\\CUARTO SEMESTRE\\Bases de Datos Avanzadas Recurse\\Proyecto\\Proyecto Semestral\\landing_zone\\"
raw_zone_path = "C:\\Users\\jalvarezg1601\\Desktop\\ESCOM\\CUARTO SEMESTRE\\Bases de Datos Avanzadas Recurse\\Proyecto\\Proyecto Semestral\\raw_zone\\"

# Crear la carpeta raw-zone si no existe
os.makedirs(raw_zone_path, exist_ok=True)

# Archivos CSV a procesar
csv_files = [
    "city_day.csv",
    "city_hour.csv",
    "station_day.csv",
    "station_hour.csv",
    "stations.csv"
]

# Leer y procesar cada archivo específico
for file in csv_files:
    file_path = os.path.join(landing_zone_path, file)
    
    # Verificar si el archivo existe en la carpeta landing_zone
    if os.path.exists(file_path):
        print(f"Procesando archivo: {file}")
        
        # Leer archivo CSV
        df = pd.read_csv(file_path)
        
        # Reemplazar valores nulos en columnas numéricas con la media
        numeric_columns = df.select_dtypes(include=["number"]).columns
        for col in numeric_columns:
            df[col].fillna(df[col].mean(), inplace=True)
        
        # Reemplazar valores nulos en columnas categóricas con "Unknown"
        categorical_columns = df.select_dtypes(include=["object"]).columns
        for col in categorical_columns:
            df[col].fillna("Unknown", inplace=True)
        
        # Convertir la columna Date a formato de fecha si existe
        if "Date" in df.columns:
            df["Date"] = pd.to_datetime(df["Date"], errors="coerce")
            df.dropna(subset=["Date"], inplace=True)  # Eliminar filas con fechas no válidas
        
        # Guardar el DataFrame en formato Parquet en raw-zone
        parquet_file_path = os.path.join(raw_zone_path, file.replace(".csv", ".parquet"))
        df.to_parquet(parquet_file_path, index=False)
        print(f"Archivo guardado en formato Parquet: {parquet_file_path}")
    else:
        print(f"El archivo {file} no se encuentra en la carpeta {landing_zone_path}.")
