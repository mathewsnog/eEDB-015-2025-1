from pyspark.sql import SparkSession
from pyspark.sql.functions import lit

spark = SparkSession.builder.getOrCreate()

# Lista de tipos de corrida disponíveis no dataset
ride_types = [
    ("yellow", "Traditional Yellow Taxi"),
    ("green", "Boro Green Taxi"),
    ("fhv", "For-Hire Vehicle"),
    ("fhvhv", "High Volume For-Hire Vehicle (e.g., Uber, Lyft)")
]

# Criação do DataFrame com os tipos
df_ride_type = spark.createDataFrame(ride_types, ["type_id", "type_description"])

# Escrita no S3
df_ride_type.write.mode("overwrite").parquet("s3://gpe-ingestao/dimensao/dim_ride_type/")