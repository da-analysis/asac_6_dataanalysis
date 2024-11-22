# Databricks notebook source
# 수집시간 추가하기(KST)
# 임시로 델타레이크에 저장해보고 스키마 확정되면 거기로 옮기기

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
url = "http://apis.data.go.kr/B190001/localFranchisesV2/franchiseV2"
service_key = dbutils.secrets.get(scope="komsco", key="Affiliate_Basic_Information")

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
schema = StructType([
    StructField("bk_awa_perf_hd_yn", StringType(), True),
    StructField("onl_dlvy_ent_use_yn", StringType(), True),
    StructField("pos_use_yn", StringType(), True),
    StructField("ppr_frcs_aply_yn", StringType(), True),
    StructField("bzmn_stts", StringType(), True),
    StructField("bzmn_stts_nm", StringType(), True),
    StructField("ksic_cd", StringType(), True),
    StructField("ksic_cd_nm", StringType(), True),
    StructField("qr_reg_conm", StringType(), True),
    StructField("te_gds_hd_yn", StringType(), True),
    StructField("pvsn_inst_cd", StringType(), True),
    StructField("crtr_ymd", StringType(), True),
    StructField("alt_text", StringType(), True),
    StructField("brno", StringType(), True),
    StructField("frcs_reg_se", StringType(), True),
    StructField("frcs_reg_se_nm", StringType(), True),
    StructField("frcs_nm", StringType(), True),
    StructField("frcs_stlm_info_se", StringType(), True),
    StructField("frcs_stlm_info_se_nm", StringType(), True),
    StructField("frcs_rprs_telno", StringType(), True),
    StructField("usage_rgn_cd", StringType(), True),
    StructField("frcs_zip", StringType(), True),
    StructField("frcs_addr", StringType(), True),
    StructField("frcs_dtl_addr", StringType(), True),
    StructField("lat", StringType(), True),
    StructField("lot", StringType(), True),
    StructField("emd_cd", StringType(), True),
    StructField("emd_nm", StringType(), True),
    StructField("collected_time", StringType(), True)  # 수집시간 필드 추가
])

# PySpark DataFrame으로 변환
if all_data:
    df = spark.createDataFrame(all_data, schema=schema)
    print("PySpark DataFrame 생성 완료!")
    df.show(truncate=False)

    # Delta Lake에 데이터 저장
    delta_path = "/mnt/delta/affiliates_temp"
    df.write.format("delta").mode("overwrite").save(delta_path)
    print(f"데이터가 Delta Lake에 임시로 저장되었습니다: {delta_path}")
else:
    print("수집된 데이터가 없습니다.")

