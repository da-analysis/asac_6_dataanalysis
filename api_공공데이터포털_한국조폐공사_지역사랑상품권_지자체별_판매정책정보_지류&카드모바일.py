# Databricks notebook source
# 지류

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType
from datetime import datetime, timezone, timedelta
import requests
import time

# PySpark 세션 생성
spark = SparkSession.builder \
    .appName("API Data Collection") \
    .getOrCreate()

# API 정보
url = "http://apis.data.go.kr/B190001/salesPolicy/paper"
service_key = dbutils.secrets.get(scope="asac_6", key="datago")

# 요청 파라미터 기본값 설정
params = {
    'serviceKey': service_key,
    'type': 'json',
    'page': 1,
    'perPage': 1000
}

all_data = []
page = 1

# 한국 시간대 설정 (KST)
KST = timezone(timedelta(hours=9))

while True:
    params['page'] = page
    try:
        # API 호출
        response = requests.get(url, params=params, timeout=10)
        response.raise_for_status()

        # 데이터 처리
        data = response.json()
        items = data.get("data", [])

        if not items:
            print(f"Page {page}: 데이터 없음. 종료.")
            break

        # 수집시간 추가 (KST)
        collected_time = datetime.now(KST).strftime('%Y-%m-%d %H:%M:%S')
        for item in items:
            item['collected_time'] = collected_time  # 수집시간 추가

        all_data.extend(items)
        print(f"Page {page}: {len(items)}개 데이터 수집 완료. 누적 데이터: {len(all_data)}")

        if len(items) < 1000:
            print("마지막 페이지 도달. 수집 종료.")
            break

        page += 1
        time.sleep(0.5)

    except requests.exceptions.RequestException as e:
        print(f"Page {page}: 오류 발생 - {e}. 재시도 중...")
        time.sleep(2)

# 데이터 스키마 정의 (수집시간 추가)
# 데이터 스키마 정의 (수집시간 추가)
schema = StructType([
    StructField("expt_plcy_yn", StringType(), True),
    StructField("pvsn_inst_cd", StringType(), True),
    StructField("crtr_ymd", StringType(), True),
    StructField("usage_rgn_cd", StringType(), True),
    StructField("dscnt_plcy_aplcn_bgng_ymd", StringType(), True),
    StructField("dscnt_plcy_aplcn_end_ymd", StringType(), True),
    StructField("dscnt_rt", StringType(), True),  
    StructField("dy_prchs_lmt_amt", StringType(), True),  
    StructField("mm_prchs_lmt_amt", StringType(), True),  
    StructField("yr_prchs_lmt_amt", StringType(), True),  
    StructField("max_excng_lmt_amt", StringType(), True),  
    StructField("max_dscnt_lmt_amt", StringType(), True),  
    StructField("corp_smpl_prchs_yn", StringType(), True),
    StructField("corp_dscnt_prchs_yn", StringType(), True),
    StructField("collected_time", StringType(), True)  
])

# PySpark DataFrame으로 변환
# if all_data:
#     df = spark.createDataFrame(all_data, schema=schema)
#     print("PySpark DataFrame 생성 완료!")
#     df.show(truncate=False)

#     # Delta Lake에 데이터 저장
#     delta_path = "/mnt/delta/affiliates_temp"
#     df.write.format("delta").mode("overwrite").save(delta_path)
#     print(f"데이터가 Delta Lake에 임시로 저장되었습니다: {delta_path}")
# else:
#     print("수집된 데이터가 없습니다.")


# COMMAND ----------

# 카드모바일

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType
from datetime import datetime, timezone, timedelta
import requests
import time

# PySpark 세션 생성
spark = SparkSession.builder \
    .appName("API Data Collection") \
    .getOrCreate()

# API 정보
url = "http://apis.data.go.kr/B190001/salesPolicy/cardMobile"
service_key = dbutils.secrets.get(scope="asac_6", key="datago")

# 요청 파라미터 기본값 설정
params = {
    'serviceKey': service_key,
    'type': 'json',
    'page': 1,
    'perPage': 1000
}

all_data = []
page = 1

# 한국 시간대 설정 (KST)
KST = timezone(timedelta(hours=9))

while True:
    params['page'] = page
    try:
        # API 호출
        response = requests.get(url, params=params, timeout=10)
        response.raise_for_status()

        # 데이터 처리
        data = response.json()
        items = data.get("data", [])

        if not items:
            print(f"Page {page}: 데이터 없음. 종료.")
            break

        # 수집시간 추가 (KST)
        collected_time = datetime.now(KST).strftime('%Y-%m-%d %H:%M:%S')
        for item in items:
            item['collected_time'] = collected_time  # 수집시간 추가

        all_data.extend(items)
        print(f"Page {page}: {len(items)}개 데이터 수집 완료. 누적 데이터: {len(all_data)}")

        if len(items) < 1000:
            print("마지막 페이지 도달. 수집 종료.")
            break

        page += 1
        time.sleep(0.5)

    except requests.exceptions.RequestException as e:
        print(f"Page {page}: 오류 발생 - {e}. 재시도 중...")
        time.sleep(2)

# 데이터 스키마 정의 (수집시간 추가)
# 데이터 스키마 정의 (수집시간 추가)
schema = StructType([
    StructField("mm_prchs_lmt_amt", StringType(), True),
    StructField("pvsn_inst_cd", StringType(), True),
    StructField("crtr_ymd", StringType(), True),
    StructField("usage_rgn_cd", StringType(), True),
    StructField("dscnt_rt", StringType(), True)
])

# PySpark DataFrame으로 변환
# if all_data:
#     df = spark.createDataFrame(all_data, schema=schema)
#     print("PySpark DataFrame 생성 완료!")
#     df.show(truncate=False)

#     # Delta Lake에 데이터 저장
#     delta_path = "/mnt/delta/affiliates_temp"
#     df.write.format("delta").mode("overwrite").save(delta_path)
#     print(f"데이터가 Delta Lake에 임시로 저장되었습니다: {delta_path}")
# else:
#     print("수집된 데이터가 없습니다.")
