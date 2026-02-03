#"""
#Capa Gold:
#- Lee datos limpios desde la capa Silver
#- Agrupa información por día
#- Calcula KPIs diarios para análisis de negocio
#"""

#---------------------------
  #Imports y Spark Session
#---------------------------

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    to_date,
    count,
    sum as spark_sum,
    avg,
    unix_timestamp,
    lit
)

spark = SparkSession.builder \
    .appName("Gold KPIs - NYC TLC HVFHS") \
    .getOrCreate()

#----------------
     #Rutas
#----------------

silver_path = "path/to/datalake/silver/hvfhs/"
gold_path = "path/to/datalake/gold/hvfhs/"

#---------------------------
 #Leemos datos desde Silver
#---------------------------

df_silver = spark.read \
    .format("delta") \
    .load(silver_path)

#----------------------------------------
 #Crear columna de fecha (Esto es CLAVE)
#----------------------------------------

df_gold_base = df_silver.withColumn(
    "trip_date",
    to_date(col("pickup_datetime"))
)

#----------------------------------------
   #Aquí vamos a Calcular KPIs diarios
#----------------------------------------

df_kpis = df_gold_base.groupBy("trip_date").agg(
    count("*").alias("total_trips"),
    avg("trip_distance").alias("avg_trip_distance"),
    spark_sum(
        col("base_passenger_fare") +
        col("tolls") +
        col("sales_tax")
    ).alias("total_revenue"),
    avg(
        (unix_timestamp("dropoff_datetime") - unix_timestamp("pickup_datetime")) / 60
    ).alias("avg_trip_duration_minutes")
)

#----------------------------
  #Viajes promedio por hora
#----------------------------

df_kpis = df_kpis.withColumn(
    "avg_trips_per_hour",
    col("total_trips") / lit(24)
)

#----------------------------
  #Guardar en Gold (Delta)
#----------------------------

df_kpis.write \
    .format("delta") \
    .mode("append") \
    .save(gold_path)


# La capa Gold expone KPIs diarios listos para consumo analítico, permitiendo análisis de negocio y consultas eficientes.











