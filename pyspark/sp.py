"""sp.py"""
from pyspark.sql import SparkSession
import sys

APP_NAME = sys.argv[1]
LOAD_DT = sys.argv[2]

spark = SparkSession.builder.appName(APP_NAME).getOrCreate()

df1 = spark.read.parquet(f"/home/sujin/data/movie/repartition/load_dt={LOAD_DT}")
df1.createOrReplaceTempView("df1_table")

spark.sql(f"""
    SELECT
        movieCd, -- 영화의 대표코드
        movieNm,
        salesAmt, -- 매출액
        audiCnt, -- 관객수
        showCnt, -- 상영횟수
        multiMovieYn, -- 다양성 영화/상업영화를 구분할 수 있습니다. 'Y' : 다양성 영화, 'N' : 상업영화
        repNationCd, -- 한국/외국 영화별로 조회할 수 있습니다. 'K' : 한국 영화, 'F' : 외국 영화
        {LOAD_DT} AS load_dt
    FROM
        df1_table
""")



spark.stop()
