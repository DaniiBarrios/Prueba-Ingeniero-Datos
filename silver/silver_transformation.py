#Capa Silver:

#- Aquí se lee datos desde la capa Bronze (Delta Lake)
#- Luego se Corrige tipos de datos
#- Luego lo que hacemos es Normalizar timestamps
#- Se Maneja valores nulos
#- Y aquí se Prepara datos limpios y confiables para análisis

#-------------------------------------------------------------------

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    to_timestamp,
    when,
    lit
)

spark = SparkSession.builder \
    .appName("Silver Transformation - NYC TLC HVFHS") \
    .getOrCreate()

# Rutas (pueden variar según el entorno: dev / qa / prod)
bronze_path = "path/to/datalake/bronze/hvfhs/"
silver_path = "path/to/datalake/silver/hvfhs/"

#Aquí ya no leemos CSV, leemos Delta

df_bronze = spark.read \
    .format("delta") \
    .load(bronze_path)

#Corrección de tipos

df_silver = df_bronze \
    .withColumn("trip_distance", col("trip_distance").cast("double")) \
    .withColumn("base_passenger_fare", col("base_passenger_fare").cast("double")) \
    .withColumn("tolls", col("tolls").cast("double")) \
    .withColumn("sales_tax", col("sales_tax").cast("double"))

#Normalizar timestamps

df_silver = df_silver \
    .withColumn(
        "pickup_datetime",
        to_timestamp(col("pickup_datetime"))
    ) \
    .withColumn(
        "dropoff_datetime",
        to_timestamp(col("dropoff_datetime"))
    )


#Manejo básico de nulos

#------------------
#Ejemplo sencillo
#------------------

df_silver = df_silver \
    .withColumn(
        "trip_distance",
        when(col("trip_distance").isNull(), lit(0)).otherwise(col("trip_distance"))
    )

#Escribir en Silver (Delta Lake)

df_silver.write \
    .format("delta") \
    .mode("append") \
    .save(silver_path)

#------------------------------------------------------------------

# Hay que tener en cuenta: 
# -La capa Silver estandariza los datos provenientes de Bronze, asegurando calidad, tipos correctos y consistencia temporal, manteniendo históricos para análisis posteriores.
