# Databricks notebook source

df = spark.read.format("csv") \
    .option("header", "true") \
    .option("encoding", "CP949") \
    .option("delimiter", ",") \
    .option("multiLine", "true") \
    .option("inferSchema", "true") \
    .load("dbfs:/FileStore/tables/meta/gyeonggi_meta.csv")
    
display(df)


# COMMAND ----------


print(df.columns)


# COMMAND ----------

# # 열 이름 변환 함수
# def clean_column_names(columns):
#     return [col.replace(" ", "_").replace("(", "").replace(")", "").replace("\n", "").replace("\t", "").replace(".", "_") for col in columns]

# # 열 이름 수정
# df = df.toDF(*clean_column_names(df.columns))

# # 수정된 열 이름 확인
# print(df.columns)


# COMMAND ----------

import os  # os 모듈 임포트
from pyspark.sql import SparkSession

# CSV 파일 경로
file_path = "dbfs:/FileStore/tables/meta/gyeonggi_meta.csv"

# CSV 파일명에서 테이블 이름 추출
csv_file_name = os.path.basename(file_path)  # 파일명 추출 (예: public_meta_F.csv)
table_name = os.path.splitext(csv_file_name)[0]  # 확장자 제거 (예: public_meta_F)

# Unity Catalog 경로 설정
catalog_name = "bronze"       # Unity Catalog의 카탈로그 이름
schema_name = "meta_gyeonggi"    # Unity Catalog의 스키마 이름

# 데이터프레임 컬럼 이름 클리닝
df = df.toDF(*[col.replace(" ", "_").replace(",", "_").replace(";", "_").replace("{", "_")
                  .replace("}", "_").replace("(", "_").replace(")", "_")
                  .replace("\n", "_").replace("\t", "_").replace("=", "_")
               for col in df.columns])

# 전체 테이블 이름 (백틱으로 감싸기)
full_table_name = f"{catalog_name}.{schema_name}.`{table_name}`"

# Delta 테이블로 저장
df.write.format("delta").mode("overwrite").saveAsTable(full_table_name)

print(f"Delta table successfully saved as {full_table_name}")

# COMMAND ----------

# 테이블 목록
tables = [
    "bronze.meta_gyeonggi.`경기데이터드림_메타데이터`"
]

# 컬럼 추가 및 업데이트 작업
for table in tables:
    # 테이블 스키마 확인
    schema = spark.sql(f"DESCRIBE {table}").collect()
    columns = [row['col_name'] for row in schema]

    # '업로드날짜' 컬럼이 없으면 추가
    if "업로드날짜" not in columns:
        spark.sql(f"""
        ALTER TABLE {table}
        ADD COLUMNS (
            `업로드날짜` DATE,
            `업데이트날짜` DATE
        )
        """)

    # '업로드날짜' 컬럼 업데이트
    spark.sql(f"""
    UPDATE {table}
    SET `업로드날짜` = CURRENT_DATE
    WHERE `업로드날짜` IS NULL
    """)

