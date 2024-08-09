from pyspark.sql import SparkSession
import sys

APP_NAME = sys.argv[1]
LOAD_DT = sys.argv[2]

spark = SparkSession.builder.appName(APP_NAME).getOrCreate()

df_join = spark.read.parquet(f"/home/sujin/data/movie/hive/load_dt={LOAD_DT}")
df1.createOrReplaceTempView("join_table")

df_join=spark.sql(f"""
    SELECT COUNT()

        """)
print('*'*1000)

df_join.write.partitionBy("load_dt", "multiMovieYn", "repNationCd").parquet("/home/sujin/data/movie/hive/")



spark.stop()
