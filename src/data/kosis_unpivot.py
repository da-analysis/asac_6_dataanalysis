# Databricks notebook source
# MAGIC %md
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC

# COMMAND ----------

from pyspark.sql import SparkSession
import pandas as pd

# SparkSession 생성
spark = SparkSession.builder.getOrCreate()

# 파일 경로
file_path = "dbfs:/FileStore/tables/file/kosis/평택사랑_상품권_지역경제_활성화_도움_정도_경기_평택_210331.csv"

# Spark로 파일 읽기
df_spark = spark.read.format("csv") \
    .option("header", "false") \
    .option("inferSchema", "true") \
    .option("encoding", "cp949") \
    .load(file_path)

# Spark DataFrame을 Pandas DataFrame으로 변환
df_pandas = df_spark.toPandas()

# COMMAND ----------

# MAGIC %md
# MAGIC 코드 1: [대, 중, 소, 속성1,  연도, 값]

# COMMAND ----------

def unpivot_layout_2(df_pandas, start_row, start_col):
    try:
        # 데이터 분리
        row_headers = df_pandas.iloc[start_row:, :start_col].reset_index(drop=True)

        # 동적으로 열 이름 설정
        if row_headers.shape[1] == 3:
            row_headers.columns = ["분류_대분류", "분류_중분류", "분류_소분류"]
        elif row_headers.shape[1] == 2:
            row_headers.columns = ["분류_대분류", "분류_중분류"]
        else:
            raise ValueError(f"예상하지 못한 열 개수: {row_headers.shape[1]}")

        # 데이터 값 처리
        data_values = df_pandas.iloc[start_row:, start_col:].reset_index(drop=True)
        연도_values = df_pandas.iloc[0, start_col:].reset_index(drop=True).fillna("").tolist()
        속성1_values = df_pandas.iloc[1, start_col:].reset_index(drop=True).fillna("").tolist()

        # 데이터 값과 열 이름 길이 검증
        if len(연도_values) != len(속성1_values) or len(연도_values) != data_values.shape[1]:
            raise ValueError("열 이름의 길이와 데이터 값 열 수가 일치하지 않습니다.")

        # 열 이름 설정
        data_values.columns = [f"{연도_values[i]}_{속성1_values[i]}" for i in range(len(연도_values))]

        # Unpivot 수행
        unpivoted = pd.melt(data_values.reset_index(), id_vars=["index"], var_name="속성", value_name="값")
        unpivoted = pd.concat([row_headers.iloc[unpivoted["index"]].reset_index(drop=True), unpivoted], axis=1)

        # 속성 분리
        unpivoted["연도"] = unpivoted["속성"].str.split("_").str[0]  # 연도 추출
        unpivoted["속성_유형"] = unpivoted["속성"].str.split("_").str[1]  # 속성 유형 추출

        # 필요 없는 열 제거
        unpivoted = unpivoted.drop(columns=["index", "속성"])

          # 데이터 타입 변환 (연도와 값 열을 int로 변환)
        unpivoted["연도"] = pd.to_numeric(unpivoted["연도"], errors="coerce").fillna(0).astype(int)
        unpivoted["값"] = pd.to_numeric(unpivoted["값"], errors="coerce").fillna(0).astype(float)

        # 열 순서 재정렬
        cols = ["분류_대분류", "분류_중분류", "분류_소분류", "속성_유형", "연도", "값"]
        unpivoted = unpivoted[[col for col in cols if col in unpivoted.columns]]

        return unpivoted

    except Exception as e:
        print(f"오류 발생: {e}")
        return None


# COMMAND ----------

# MAGIC %md
# MAGIC 코드 2: [대, 중, 소, 속성1, 속성2, 연도, 값]

# COMMAND ----------

def unpivot_layout_2(df_pandas, start_row, start_col):
    try:
        # 데이터 분리
        row_headers = df_pandas.iloc[start_row:, :start_col].reset_index(drop=True)
        header_columns = ["분류_대분류", "분류_중분류"]

        # 소분류 열 추가 (존재할 경우에만)
        if row_headers.shape[1] > 2:
            header_columns.append("분류_소분류")
        row_headers.columns = header_columns[:row_headers.shape[1]]

        # 데이터 값 처리
        data_values = df_pandas.iloc[start_row:, start_col:].reset_index(drop=True)
        연도_values = df_pandas.iloc[0, start_col:].reset_index(drop=True).tolist()
        속성1_values = df_pandas.iloc[1, start_col:].reset_index(drop=True).tolist()
        속성2_values = (
            df_pandas.iloc[2, start_col:].reset_index(drop=True).tolist()
            if len(df_pandas) > 2 and not all(isinstance(x, (int, float)) for x in df_pandas.iloc[2, start_col:].tolist())
            else None
        )

        # 속성2가 없으면 열 이름에서 제외
        if 속성2_values is None:
            data_values.columns = [f"{연도_values[i]}_{속성1_values[i]}" for i in range(len(연도_values))]
        else:
            data_values.columns = [
                f"{연도_values[i]}_{속성1_values[i]}_{속성2_values[i]}" for i in range(len(연도_values))
            ]

        # Unpivot 수행
        unpivoted = pd.melt(data_values.reset_index(), id_vars=["index"], var_name="속성", value_name="값")
        unpivoted = pd.concat([row_headers.iloc[unpivoted["index"]].reset_index(drop=True), unpivoted], axis=1)

        # 속성 분리
        unpivoted["연도"] = unpivoted["속성"].str.split("_").str[0]
        unpivoted["속성_유형"] = unpivoted["속성"].str.split("_").str[1]
        if 속성2_values is not None:
            unpivoted["속성_유형상세"] = unpivoted["속성"].str.split("_").str[2]

        # 필요 없는 열 제거
        unpivoted = unpivoted.drop(columns=["index", "속성"])

          # 데이터 타입 변환 (연도와 값 열을 int로 변환)
        unpivoted["연도"] = pd.to_numeric(unpivoted["연도"], errors="coerce").fillna(0).astype(int)
        unpivoted["값"] = pd.to_numeric(unpivoted["값"], errors="coerce").fillna(0).astype(float)

        # 열 순서 재정렬
        cols = ["분류_대분류", "분류_중분류", "분류_소분류", "속성_유형", "속성_유형상세", "연도", "값"]
        unpivoted = unpivoted[[col for col in cols if col in unpivoted.columns]]

        return unpivoted
    except Exception as e:
        print(f"오류 발생: {e}")
        return None


# COMMAND ----------

# MAGIC %md
# MAGIC 코드 3: [대, 중, 속성1, 속성2, 연도, 값]
# MAGIC

# COMMAND ----------

def unpivot_layout_2(df_pandas, start_row, start_col):
    try:
        # 데이터 분리
        row_headers = df_pandas.iloc[start_row:, :start_col].reset_index(drop=True)
        row_headers.columns = ["분류_대분류", "분류_중분류"]

        # 데이터 값 처리
        data_values = df_pandas.iloc[start_row:, start_col:].reset_index(drop=True)
        연도_values = df_pandas.iloc[0, start_col:].reset_index(drop=True).tolist()
        속성1_values = df_pandas.iloc[1, start_col:].reset_index(drop=True).tolist()
        속성2_values = (
            df_pandas.iloc[2, start_col:].reset_index(drop=True).tolist()
            if len(df_pandas) > 2 and not all(isinstance(x, (int, float)) for x in df_pandas.iloc[2, start_col:].tolist())
            else None
        )

        # 속성2가 없으면 열 이름에서 제외
        if 속성2_values is None:
            data_values.columns = [f"{연도_values[i]}_{속성1_values[i]}" for i in range(len(연도_values))]
        else:
            data_values.columns = [
                f"{연도_values[i]}_{속성1_values[i]}_{속성2_values[i]}" for i in range(len(연도_values))
            ]

        # Unpivot 수행
        unpivoted = pd.melt(data_values.reset_index(), id_vars=["index"], var_name="속성", value_name="값")
        unpivoted = pd.concat([row_headers.iloc[unpivoted["index"]].reset_index(drop=True), unpivoted], axis=1)

        unpivoted["연도"] = unpivoted["속성"].str.split("_").str[0]  # 연도 추출
        unpivoted["속성_유형"] = unpivoted["속성"].str.split("_").str[1]  # 속성 유형 추출
        unpivoted["속성_유형상세"] = unpivoted["속성"].str.split("_").str[2]  # 속성 상세 추출


        # 필요 없는 열 제거
        unpivoted = unpivoted.drop(columns=["index", "속성"])

          # 데이터 타입 변환 (연도와 값 열을 int로 변환)
        unpivoted["연도"] = pd.to_numeric(unpivoted["연도"], errors="coerce").fillna(0).astype(int)
        unpivoted["값"] = pd.to_numeric(unpivoted["값"], errors="coerce").fillna(0).astype(float)

        cols = ["분류_대분류", "분류_중분류", "분류_소분류", "속성_유형", "속성_유형상세", "연도", "값"]
        unpivoted = unpivoted[[col for col in cols if col in unpivoted.columns]]


        return unpivoted
    except Exception as e:
        print(f"오류 발생: {e}")
        return None


# COMMAND ----------

# MAGIC %md
# MAGIC 코드 4: [대, 중, 속성1, 연도, 값]

# COMMAND ----------

def unpivot_layout_2(df_pandas, start_row, start_col):
    try:
        # 데이터 분리
        row_headers = df_pandas.iloc[start_row:, :start_col].reset_index(drop=True)
        row_headers.columns = ["분류_대분류", "분류_중분류"]

        # 데이터 값 처리
        data_values = df_pandas.iloc[start_row:, start_col:].reset_index(drop=True)
        연도_values = df_pandas.iloc[0, start_col:].reset_index(drop=True).tolist()
        속성1_values = df_pandas.iloc[1, start_col:].reset_index(drop=True).tolist()

        data_values.columns = [f"{연도_values[i]}_{속성1_values[i]}" for i in range(len(연도_values))]

        # Unpivot 수행
        unpivoted = pd.melt(data_values.reset_index(), id_vars=["index"], var_name="속성", value_name="값")
        unpivoted = pd.concat([row_headers.iloc[unpivoted["index"]].reset_index(drop=True), unpivoted], axis=1)

        unpivoted["연도"] = unpivoted["속성"].str.split("_").str[0]  # 연도 추출
        unpivoted["속성_유형"] = unpivoted["속성"].str.split("_").str[1]  # 속성 유형 추출

        # 필요 없는 열 제거
        unpivoted = unpivoted.drop(columns=["index", "속성"])

          # 데이터 타입 변환 (연도와 값 열을 int로 변환)
        unpivoted["연도"] = pd.to_numeric(unpivoted["연도"], errors="coerce").fillna(0).astype(int)
        unpivoted["값"] = pd.to_numeric(unpivoted["값"], errors="coerce").fillna(0).astype(float)

        # 열 순서 재정렬
        cols = ["분류_대분류", "분류_중분류", "분류_소분류", "속성_유형", "연도", "값"]
        unpivoted = unpivoted[[col for col in cols if col in unpivoted.columns]]

        return unpivoted
    except Exception as e:
        print(f"오류 발생: {e}")
        return None


# COMMAND ----------

# 실행
print("Executing Layout 2")
start_row = 2  # 데이터가 시작되는 행 번호
start_col = 2  # 데이터가 시작되는 열 번호

result_layout_2 = unpivot_layout_2(df_pandas, start_row, start_col)

import os
# Delta 테이블 저장 코드 추가
if result_layout_2 is not None:
    # Unity Catalog에 Delta 테이블 저장
    catalog_name = "dataanalysis"
    schema_name = "temp_ch"
    table_name = os.path.basename(file_path).split(".")[0]  # 파일명에서 테이블 이름 추출

    # 백틱으로 감싼 테이블 이름
    quoted_table_name = f"`{catalog_name}`.`{schema_name}`.`{table_name}`"

    # Pandas DataFrame -> Spark DataFrame 변환
    df_spark_result = spark.createDataFrame(result_layout_2)

# 데이터를 보여주는 부분 추가 (저장 전에 확인)
    print("저장 전에 데이터 확인 (Pandas DataFrame):")
    print(result_layout_2.head(10))  # Pandas DataFrame에서 상위 10개 행 출력




# COMMAND ----------

# Delta 테이블 저장
try:
    df_spark_result.write.format("delta") \
        .mode("overwrite") \
        .saveAsTable(quoted_table_name)
    print(f"테이블이 {quoted_table_name}에 저장되었습니다.")
except Exception as e:
    print(f"오류 발생: {e}")

