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

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql.functions import lit
from datetime import datetime, timezone, timedelta
import requests
import time

# PySpark 세션 생성
spark = SparkSession.builder \
    .appName("API Data Collection with Delta Checkpoint") \
    .getOrCreate()

# API 정보
url = "http://apis.data.go.kr/B190001/paperStock/v2/stockV2"
service_key = dbutils.secrets.get(scope="asac_6", key="datago")
catalog_table = "bronze.api_public.`한국조폐공사_지역사랑상품권_지류_지점별_재고정보_전국`"

params = {
    'serviceKey': service_key,
    'type': 'json',
    'page': 1,
    'perPage': 1000
}

# 데이터 스키마 정의
schema = StructType([
    StructField("pvsn_inst_cd", StringType(), True),
    StructField("crtr_ymd", StringType(), True),
    StructField("emd_cd", StringType(), True),
    StructField("emd_nm", StringType(), True),
    StructField("brnch_id", StringType(), True),
    StructField("brnch_nm", StringType(), True),
    StructField("gt_type_dnmn", StringType(), True),
    StructField("gt_type_stc_qty", StringType(), True),
    StructField("usage_rgn_cd", StringType(), True),
    StructField("usage_rgn_nm", StringType(), True),
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

# 이미 처리된 페이지 확인
def get_processed_pages():
    try:
        existing_data = spark.table(catalog_table)
        processed_pages = (
            existing_data
            .select("page")
            .distinct()
            .toPandas()["page"]
            .tolist()
        )
        return set(processed_pages)
    except Exception as e:
        print(f"Delta 테이블에서 처리된 페이지 정보를 가져오는 데 실패: {e}")
        return set()

# 데이터 저장
def save_to_delta(data, page):
    if data:
        df = spark.createDataFrame(data, schema=schema)
        df = df.withColumn("page", lit(page))  # page 정보 추가
        df.write.format("delta").mode("append").saveAsTable(catalog_table)
        print(f"Delta 테이블에 저장 완료: {len(data)}개 데이터 저장됨. (Page: {page})")

# Delta 테이블에서 기존 정보 로드
processed_pages = get_processed_pages()
last_id = get_last_id()
page = max(processed_pages) + 1 if processed_pages else 1
current_id = last_id  # 기존 데이터 누적 ID

batch_size = 10  # 10페이지(10,000개) 단위로 저장
all_data = []

while True:
    params['page'] = page
    retries = 0

    # API 호출 및 데이터 수집
    while retries < 3:
        try:
            response = requests.get(url, params=params, timeout=10)
            response.raise_for_status()
            items = response.json().get("data", [])
            break
        except requests.exceptions.RequestException as e:
            retries += 1
            print(f"Page {page}: 오류 발생 - {e}. 재시도 중 ({retries}/3)...")
            time.sleep(2)

    if retries == 3 or not items:
        print(f"Page {page}: 데이터 없음 또는 최대 재시도 초과. 종료.")
        break

    # 수집시간 및 순차 ID 추가
    collected_time = datetime.now(timezone(timedelta(hours=9))).strftime('%Y-%m-%d %H:%M:%S')
    for item in items:
        item['collected_time'] = collected_time
        current_id += 1  # ID 순차 증가
        item['id'] = current_id

    # 데이터 저장 및 상태 업데이트
    all_data.extend(items)
    print(f"Page {page}: {len(items)}개 데이터 수집 완료. 누적 ID: {current_id}")

    if page % batch_size == 0:
        save_to_delta(all_data, page)
        all_data = []  # 저장 후 초기화

    page += 1
    time.sleep(0.5)

# 남은 데이터 저장
if all_data:
    save_to_delta(all_data, page)
    print("Delta 테이블에 최종 데이터 저장 완료.")


# COMMAND ----------

# MAGIC %md
# MAGIC 3. 데이터 확인

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM bronze.api_public.`한국조폐공사_지역사랑상품권_지류_지점별_재고정보_전국`
# MAGIC ORDER BY id DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*)
# MAGIC FROM(
# MAGIC SELECT DISTINCT(*) FROM bronze.api_public.`한국조폐공사_지역사랑상품권_지류_지점별_재고정보_전국`
# MAGIC ORDER BY id);

# COMMAND ----------

# MAGIC %md
# MAGIC 4. Comment

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE bronze.api_public.`한국조폐공사_지역사랑상품권_지류_지점별_재고정보_전국`
# MAGIC ALTER COLUMN pvsn_inst_cd COMMENT '제공기관코드(운영대행사 코드)';
# MAGIC
# MAGIC ALTER TABLE bronze.api_public.`한국조폐공사_지역사랑상품권_지류_지점별_재고정보_전국`
# MAGIC ALTER COLUMN crtr_ymd COMMENT '데이터 기준일자';
# MAGIC
# MAGIC ALTER TABLE bronze.api_public.`한국조폐공사_지역사랑상품권_지류_지점별_재고정보_전국`
# MAGIC ALTER COLUMN emd_cd COMMENT '읍면동코드(숫자8자리)';
# MAGIC
# MAGIC ALTER TABLE bronze.api_public.`한국조폐공사_지역사랑상품권_지류_지점별_재고정보_전국`
# MAGIC ALTER COLUMN emd_nm COMMENT '읍면동명';
# MAGIC
# MAGIC ALTER TABLE bronze.api_public.`한국조폐공사_지역사랑상품권_지류_지점별_재고정보_전국`
# MAGIC ALTER COLUMN brnch_id COMMENT '지점구분ID';
# MAGIC
# MAGIC ALTER TABLE bronze.api_public.`한국조폐공사_지역사랑상품권_지류_지점별_재고정보_전국`
# MAGIC ALTER COLUMN brnch_nm COMMENT '지점명';
# MAGIC
# MAGIC ALTER TABLE bronze.api_public.`한국조폐공사_지역사랑상품권_지류_지점별_재고정보_전국`
# MAGIC ALTER COLUMN gt_type_dnmn COMMENT '상품권종액면가';
# MAGIC
# MAGIC ALTER TABLE bronze.api_public.`한국조폐공사_지역사랑상품권_지류_지점별_재고정보_전국`
# MAGIC ALTER COLUMN gt_type_stc_qty COMMENT '상품권재고여부(0:없음, 1:있음)';
# MAGIC
# MAGIC ALTER TABLE bronze.api_public.`한국조폐공사_지역사랑상품권_지류_지점별_재고정보_전국`
# MAGIC ALTER COLUMN usage_rgn_cd COMMENT '사용처지역코드';
# MAGIC
# MAGIC ALTER TABLE bronze.api_public.`한국조폐공사_지역사랑상품권_지류_지점별_재고정보_전국`
# MAGIC ALTER COLUMN usage_rgn_nm COMMENT '사용처지역명';
# MAGIC
