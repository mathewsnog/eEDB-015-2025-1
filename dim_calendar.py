from pyspark.context import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit
from datetime import datetime
import pandas as pd

# Criar contexto Spark
sc = SparkContext()
spark = SparkSession(sc)

# Definindo período
start_date = datetime(2018, 1, 1)
end_date = datetime(2025, 12, 31)
dates = pd.date_range(start=start_date, end=end_date)

# Lista de feriados fixos
fixed_holidays = ["01-01", "07-04", "12-25", "11-11", "01-20", "02-14"]

# Período de pandemia
pandemic_start = datetime(2020, 3, 1)
pandemic_end = datetime(2021, 6, 30)

# Criar DataFrame Pandas
calendar_df = pd.DataFrame({
    "calendar_id": dates.strftime("%Y-%m-%d"),
    "date": dates + pd.to_timedelta(12, unit='h'),
    "day": dates.day,
    "month": dates.month,
    "year": dates.year,
    "week_day": dates.strftime("%A"),
    "holiday": dates.strftime("%m-%d").isin(fixed_holidays),
    "work_shift": pd.cut(
        dates.hour,
        bins=[-1, 5, 11, 17, 23],
        labels=["Madrugada", "Manhã", "Tarde", "Noite"],
        include_lowest=True
    ),
    "pandemic": dates.to_series().between(pandemic_start, pandemic_end)
})

# Converter para Spark DataFrame
df_calendar = spark.createDataFrame(calendar_df)

# Gravar no S3
df_calendar.write.mode("overwrite") \
    .partitionBy("year") \
    .parquet("s3://gpe-ingestao/curated/dim_calendar/")

print("Dimensão calendário gravada com sucesso na camada Curated.")