"""sp.py"""
from pyspark.sql import SparkSession
import sys

APP_NAME = sys.argv[1]
LOAD_DT = sys.argv[2]

spark = SparkSession.builder.appName(APP_NAME).getOrCreate()

df1 = spark.read.parquet(f"/Users/sujinya/data/movie_data/re_data/load_dt={LOAD_DT}")
df1.createOrReplaceTempView("df1_table")

df_multi=spark.sql(f"""
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

    WHERE multiMovieYn IS NOT NULL
""")
df_multi.createOrReplaceTempView("multi_table")

df_nation=spark.sql(f"""
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

    WHERE repNationCd IS NOT NULL
""")
df_nation.createOrReplaceTempView("nation_table")

df_join = spark.sql(f"""
SELECT
    COALESCE(m.movieCd, n.movieCd) AS movieCd,
    COALESCE(m.movieNm, n.movieNm) AS movieNm,
    COALESCE(m.salesAmt, n.salesAmt) AS salesAmt, -- 매출액
    COALESCE(m.audiCnt, n.audiCnt) AS audiCnt, -- 관객수
    COALESCE(m.showCnt, n.showCnt) AS showCnt, --상영횟수
    m.multiMovieYn, -- 다양성 영화/상업영화를 구분지어 조회할 수 있습니다. “Y” : 다양성 영화 “N”
    n.repNationCd, -- 한국/외국 영화별로 조회할 수 있습니다. “K: : 한국영화 “F” : 외국영화
    '{LOAD_DT}' AS load_dt
FROM multi_table m FULL OUTER JOIN nation_table n
ON m.movieCd = n.movieCd""")

print('*'*1000)

df_join.write.mode('overwrite').partitionBy("multiMovieYn", "repNationCd").parquet(f"/Users/sujinya/data/movie_data/hive/load_dt={LOAD_DT}")
#df_join.write.mode('append').partitionBy("load_dt", "multiMovieYn", "repNationCd").parquet("/Users/sujinya/data/movie_data/hive")


spark.stop()
