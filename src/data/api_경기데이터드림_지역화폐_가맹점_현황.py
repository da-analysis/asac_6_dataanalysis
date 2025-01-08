# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType
from pyspark.sql.functions import lit
from datetime import datetime, timezone, timedelta
import requests
import time

# PySpark 세션 생성
spark = SparkSession.builder \
    .appName("API Data Collection for RegionMnyFacltStus") \
    .getOrCreate()

# API 정보
url = "https://openapi.gg.go.kr/RegionMnyFacltStus"
catalog_table = "bronze.api_gyeonggi.`지역화폐_가맹점_현황_경기`"

# 데이터 스키마 정의
schema = StructType([
    StructField("SIGUN_NM", StringType(), True),
    StructField("CMPNM_NM", StringType(), True),
    StructField("INDUTYPE_NM", StringType(), True),
    StructField("REFINE_ROADNM_ADDR", StringType(), True),
    StructField("REFINE_LOTNO_ADDR", StringType(), True),
    StructField("REFINE_ZIPNO", StringType(), True),
    StructField("REFINE_WGS84_LAT", FloatType(), True),
    StructField("REFINE_WGS84_LOGT", FloatType(), True),
    StructField("BIZREGNO", StringType(), True),
    StructField("INDUTYPE_CD", StringType(), True),
    StructField("FRCS_NO", StringType(), True),
    StructField("LEAD_TAX_MAN_STATE", StringType(), True),
    StructField("LEAD_TAX_MAN_STATE_CD", StringType(), True),
    StructField("CLSBIZ_DAY", StringType(), True),
    StructField("collected_time", StringType(), True),  # 수집시간 필드
    StructField("id", IntegerType(), True),            # 넘버링을 위한 ID 필드
    StructField("pageNo", IntegerType(), True)         # 페이지 정보 필드
])

# Delta 테이블 초기화 함수
def initialize_delta_table():
    try:
        spark.table(catalog_table)
        print(f"Delta 테이블 '{catalog_table}'이 이미 존재합니다.")
    except Exception:
        print(f"Delta 테이블 '{catalog_table}'이 존재하지 않습니다. 새로 생성합니다.")
        empty_df = spark.createDataFrame([], schema)
        empty_df.write.format("delta").mode("overwrite").saveAsTable(catalog_table)
        print(f"Delta 테이블 '{catalog_table}' 생성 완료.")

# 데이터 저장
def save_to_delta(data, page):
    if data:
        df = spark.createDataFrame(data, schema=schema)
        df = df.withColumn("pageNo", lit(page))
        df.write.format("delta").mode("append").saveAsTable(catalog_table)
        print(f"Delta 테이블에 저장 완료: {len(data)}개 데이터 저장됨. (Page: {page})")

# 중복 제거 함수
def remove_duplicates():
    try:
        # Delta 테이블 불러오기
        df = spark.table(catalog_table)
        
        # 중복 제거 기준 컬럼
        dedup_columns = [
            "SIGUN_NM", "CMPNM_NM", "INDUTYPE_NM",
            "REFINE_ROADNM_ADDR", "REFINE_LOTNO_ADDR", "REFINE_ZIPNO",
            "REFINE_WGS84_LAT", "REFINE_WGS84_LOGT", "BIZREGNO",
            "INDUTYPE_CD", "FRCS_NO", "LEAD_TAX_MAN_STATE",
            "LEAD_TAX_MAN_STATE_CD", "CLSBIZ_DAY"
        ]
        
        # 중복 제거
        deduplicated_df = df.dropDuplicates(dedup_columns)
        
        # Delta 테이블 업데이트
        deduplicated_df.write.format("delta").mode("overwrite").saveAsTable(catalog_table)
        print(f"[INFO] 중복 제거 완료: {deduplicated_df.count()}개 데이터가 남았습니다.")
    except Exception as e:
        print(f"[ERROR] 중복 제거 중 오류 발생: {e}")

# Delta 테이블 초기화
initialize_delta_table()

# API 키 설정
api_key = dbutils.secrets.get(scope="asac_6", key="datagggo")  # 인증키

# 요청 파라미터 기본값
params = {
    'KEY': api_key,
    'Type': 'json',
    'pIndex': 1,  # 항상 처음부터 시작
    'pSize': 100
}

# 수집 로직
batch_size = 10
all_data = []
current_id = 0
page = 1

while True:
    params['pIndex'] = page
    retries = 0

    while retries < 3:
        try:
            response = requests.get(url, params=params, timeout=10)
            response.raise_for_status()

            print(f"[DEBUG] Page {page}: URL={response.url}, Status={response.status_code}")
            
            try:
                result = response.json()
            except ValueError:
                print(f"[ERROR] Page {page}: JSON 파싱 실패.")
                items = []
                break

            try:
                code = result['RegionMnyFacltStus'][0]['head'][1]['RESULT']['CODE']
                if code == 'INFO-000':  # 정상 응답
                    items = result['RegionMnyFacltStus'][1]['row']
                    break
                else:
                    print(f"[WARNING] Page {page}: 응답 코드 {code}.")
                    items = []
                    break
            except KeyError:
                print(f"[ERROR] Page {page}: 예상된 키가 응답에 없습니다.")
                items = []
                break

        except requests.exceptions.RequestException as e:
            retries += 1
            print(f"[ERROR] Page {page}: 요청 실패 - {e}. 재시도 중 ({retries}/3)...")
            time.sleep(2)

    if retries == 3 or not items:
        print(f"[INFO] Page {page}: 데이터 없음 또는 최대 재시도 초과. 종료.")
        break

    collected_time = datetime.now(timezone(timedelta(hours=9))).strftime('%Y-%m-%d %H:%M:%S')
    for item in items:
        item['collected_time'] = collected_time
        current_id += 1
        item['id'] = current_id

        try:
            item['REFINE_WGS84_LAT'] = float(item['REFINE_WGS84_LAT']) if item['REFINE_WGS84_LAT'] else None
        except ValueError:
            item['REFINE_WGS84_LAT'] = None

        try:
            item['REFINE_WGS84_LOGT'] = float(item['REFINE_WGS84_LOGT']) if item['REFINE_WGS84_LOGT'] else None
        except ValueError:
            item['REFINE_WGS84_LOGT'] = None

    all_data.extend(items)
    print(f"[INFO] Page {page}: {len(items)}개 데이터 수집 완료.")

    if page % batch_size == 0:
        save_to_delta(all_data, page)
        all_data = []

    page += 1
    time.sleep(0.5)

if all_data:
    save_to_delta(all_data, page)
    print("[INFO] Delta 테이블에 최종 데이터 저장 완료.")

# 중복 제거 후 업데이트
remove_duplicates()


# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE bronze.api_gyeonggi.`지역화폐_가맹점_현황_경기`
# MAGIC ALTER COLUMN SIGUN_NM COMMENT '시군명';
# MAGIC
# MAGIC ALTER TABLE bronze.api_gyeonggi.`지역화폐_가맹점_현황_경기`
# MAGIC ALTER COLUMN CMPNM_NM COMMENT '상호명';
# MAGIC
# MAGIC ALTER TABLE bronze.api_gyeonggi.`지역화폐_가맹점_현황_경기`
# MAGIC ALTER COLUMN INDUTYPE_NM COMMENT '업종명';
# MAGIC
# MAGIC ALTER TABLE bronze.api_gyeonggi.`지역화폐_가맹점_현황_경기`
# MAGIC ALTER COLUMN REFINE_ROADNM_ADDR COMMENT '정제도로명주소';
# MAGIC
# MAGIC ALTER TABLE bronze.api_gyeonggi.`지역화폐_가맹점_현황_경기`
# MAGIC ALTER COLUMN REFINE_LOTNO_ADDR COMMENT '정제지번주소';
# MAGIC
# MAGIC ALTER TABLE bronze.api_gyeonggi.`지역화폐_가맹점_현황_경기`
# MAGIC ALTER COLUMN REFINE_ZIPNO COMMENT '정제우편번호';
# MAGIC
# MAGIC ALTER TABLE bronze.api_gyeonggi.`지역화폐_가맹점_현황_경기`
# MAGIC ALTER COLUMN REFINE_WGS84_LAT COMMENT '정제WGS84위도';
# MAGIC
# MAGIC ALTER TABLE bronze.api_gyeonggi.`지역화폐_가맹점_현황_경기`
# MAGIC ALTER COLUMN REFINE_WGS84_LOGT COMMENT '정제WGS84경도';
# MAGIC
# MAGIC ALTER TABLE bronze.api_gyeonggi.`지역화폐_가맹점_현황_경기`
# MAGIC ALTER COLUMN BIZREGNO COMMENT '사업자등록번호';
# MAGIC
# MAGIC ALTER TABLE bronze.api_gyeonggi.`지역화폐_가맹점_현황_경기`
# MAGIC ALTER COLUMN INDUTYPE_CD COMMENT '업종코드';
# MAGIC
# MAGIC ALTER TABLE bronze.api_gyeonggi.`지역화폐_가맹점_현황_경기`
# MAGIC ALTER COLUMN FRCS_NO COMMENT '가맹점번호';
# MAGIC
# MAGIC ALTER TABLE bronze.api_gyeonggi.`지역화폐_가맹점_현황_경기`
# MAGIC ALTER COLUMN LEAD_TAX_MAN_STATE COMMENT '휴폐업상태';
# MAGIC
# MAGIC ALTER TABLE bronze.api_gyeonggi.`지역화폐_가맹점_현황_경기`
# MAGIC ALTER COLUMN LEAD_TAX_MAN_STATE_CD COMMENT '휴폐업상태코드';
# MAGIC
# MAGIC ALTER TABLE bronze.api_gyeonggi.`지역화폐_가맹점_현황_경기`
# MAGIC ALTER COLUMN CLSBIZ_DAY COMMENT '폐업일자';
# MAGIC
