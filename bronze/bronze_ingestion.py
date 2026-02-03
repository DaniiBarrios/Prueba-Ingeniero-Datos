#Hay que tener en cuenta que esta capa Bronze, almacena los datos crudos provenientes del dataset HVFHS sin aplicar transformaciones de negocio.

#Capa Bronze:

#- Almacena datos crudos del dataset HVFHS
#- No aplica transformaciones de negocio
#- Agrega metadatos de ingesta
#- Almacena en formato Delta Lake

#Se agregan metadatos de ingesta para facilitar trazabilidad, auditoría y reprocesos futuros.
#Los datos se almacenan en formato Delta Lake para permitir versionamiento y confiabilidad.

from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, lit

# --------------
# Spark Session
# --------------
spark = SparkSession.builder \
    .appName("Bronze Ingestion - NYC TLC HVFHS") \
    .getOrCreate()
# ----------------------------------------
# Aquí tenemos la Configuración de rutas.
# ----------------------------------------
input_path = "path/to/hvfhs_2025_01.csv"   # CSV de origen
bronze_path = "path/to/datalake/bronze/hvfhs/"
# -----------------------------------------------
# En esta sesión hacemos lectura de datos crudos.
# -----------------------------------------------
df_raw = spark.read \
    .option("header", True) \
    .option("inferSchema", True) \
    .csv(input_path)
# --------------------------
# Luego Agregamos metadatos
# --------------------------
df_bronze = df_raw \
    .withColumn("ingestion_timestamp", current_timestamp()) \
    .withColumn("source_file", lit("hvfhs_2025_01.csv"))
# ---------------------------------------------
# Generamos la Escritura en Bronze (Delta Lake)
# ---------------------------------------------
df_bronze.write \
    .format("delta") \
    .mode("append") \
    .save(bronze_path)
