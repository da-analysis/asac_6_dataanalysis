# Databricks notebook source
# MAGIC %md
# MAGIC 1. 카탈로그/스키마 경로 확인

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW CATALOGS;

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW SCHEMAS IN bronze;

# COMMAND ----------

# MAGIC %md
# MAGIC 2. Main

# COMMAND ----------

import requests
url = "https://infuser.odcloud.kr/oas/docs?namespace=15067973/v1"
response = requests.get(url)
response.json()

# COMMAND ----------

base_path = "/15067973/v1/"
api_paths = [path for path in response.json()["paths"] if path.startswith(base_path)]
print(api_paths)

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql.functions import lit
from datetime import datetime, timezone, timedelta
import requests
import time

# PySpark 세션 생성
spark = SparkSession.builder \
    .appName("API Data Collection with Multiple Paths") \
    .getOrCreate()

# 기본 API 정보
base_url = "https://api.odcloud.kr/api"
service_key = dbutils.secrets.get(scope="asac_6", key="datago")
catalog_table = "bronze.api_public.`인천e음_군구별_업종별_일별_결제금액_인천_20241031`"

params = {
    'serviceKey': service_key,
    'type': 'json',
    'page': 1,
    'perPage': 1000
}

# 데이터 스키마 정의
schema = StructType([
    StructField("일자", StringType(), True),
    StructField("지역", StringType(), True),
    StructField("문화취미-영화관", StringType(), True),
    StructField("숙박", StringType(), True),
    StructField("약국", StringType(), True),
    StructField("연료", StringType(), True),
    StructField("유통업영리-편의점", StringType(), True),
    StructField("일반휴게", StringType(), True),
    StructField("학원", StringType(), True),
    StructField("기타", StringType(), True),
    StructField("전체금액", StringType(), True),
    StructField("collected_time", StringType(), True),  # 수집시간 필드
    StructField("id", IntegerType(), True),            # 넘버링을 위한 ID 필드
    StructField("page", IntegerType(), True)           # 페이지 정보 필드
])

# 마지막 ID 가져오기
def get_last_id():
    try:
        existing_data = spark.table(catalog_table)
        if existing_data.count() == 0:
            return 0  # Delta 테이블이 비어 있으면 0부터 시작
        last_id = existing_data.agg({"id": "max"}).collect()[0][0]
        return int(last_id)
    except Exception as e:
        print(f"Delta 테이블에서 마지막 ID를 가져오는 데 실패: {e}")
        return 0

# 데이터 저장
def save_to_delta(data, page, path):
    if data:
        df = spark.createDataFrame(data, schema=schema)
        df = df.withColumn("page", lit(page))  # page 정보 추가
        df = df.withColumn("api_path", lit(path))  # 호출한 API 경로 추가
        df.write.format("delta").mode("append").saveAsTable(catalog_table)
        print(f"Delta 테이블에 저장 완료: {len(data)}개 데이터 저장됨. (Page: {page}, Path: {path})")

# Delta 테이블에서 기존 정보 로드
last_id = get_last_id()
current_id = last_id  # 기존 데이터 누적 ID
batch_size = 10  # 10페이지(10,000개) 단위로 저장

for path in api_paths:
    all_data = []
    page = 1
    while True:
        params['page'] = page
        retries = 0

        # API 호출 및 데이터 수집
        while retries < 3:
            try:
                url = f"{base_url}{path}"
                response = requests.get(url, params=params, timeout=10)
                response.raise_for_status()
                items = response.json().get("data", [])
                break
            except requests.exceptions.RequestException as e:
                retries += 1
                print(f"Path {path}, Page {page}: 오류 발생 - {e}. 재시도 중 ({retries}/3)...")
                time.sleep(2)

        if retries == 3 or not items:
            print(f"Path {path}, Page {page}: 데이터 없음 또는 최대 재시도 초과. 종료.")
            break

        # 수집시간 및 순차 ID 추가
        collected_time = datetime.now(timezone(timedelta(hours=9))).strftime('%Y-%m-%d %H:%M:%S')
        for item in items:
            item['collected_time'] = collected_time
            current_id += 1  # ID 순차 증가
            item['id'] = current_id

        # 데이터 저장 및 상태 업데이트
        all_data.extend(items)
        print(f"Path {path}, Page {page}: {len(items)}개 데이터 수집 완료. 누적 ID: {current_id}")

        if page % batch_size == 0:
            save_to_delta(all_data, page, path)
            all_data = []  # 저장 후 초기화

        page += 1
        time.sleep(0.5)

    # 남은 데이터 저장
    if all_data:
        save_to_delta(all_data, page, path)
        print(f"Path {path}: Delta 테이블에 최종 데이터 저장 완료.")


# COMMAND ----------

# MAGIC %md
# MAGIC 3. 데이터 확인

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM bronze.api_public.`인천e음_군구별_업종별_일별_결제금액_인천_20241031`
# MAGIC ORDER BY id DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*)
# MAGIC FROM(
# MAGIC SELECT DISTINCT(*) FROM bronze.api_public.`인천e음_군구별_업종별_일별_결제금액_인천_20241031`
# MAGIC ORDER BY id);

# COMMAND ----------

# MAGIC %md
# MAGIC 4. Comment - 생략
