# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql.functions import lit, col, row_number, expr, max as spark_max
from pyspark.sql.window import Window
import requests
from datetime import datetime, timezone, timedelta
import time

# PySpark 세션 생성
spark = SparkSession.builder \
    .appName("API Data Collection with Delta Checkpoint") \
    .getOrCreate()

# API 설정
api_settings = {
    "franchise": {
        "url": "http://apis.data.go.kr/B190001/localFranchisesV2/franchiseV2",
        "catalog_table": "bronze.api_public.`한국조폐공사_지역사랑상품권_가맹점기본정보_전국`",
        "schema": StructType([
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
            StructField("collected_time", StringType(), True),
            StructField("id", IntegerType(), True),
            StructField("page", IntegerType(), True)
        ]),
        "merge_conditions": """
            target.bk_awa_perf_hd_yn = source.bk_awa_perf_hd_yn AND
            target.onl_dlvy_ent_use_yn = source.onl_dlvy_ent_use_yn AND
            target.pos_use_yn = source.pos_use_yn AND
            target.ppr_frcs_aply_yn = source.ppr_frcs_aply_yn AND
            target.bzmn_stts = source.bzmn_stts AND
            target.ksic_cd = source.ksic_cd AND
            target.qr_reg_conm = source.qr_reg_conm AND
            target.te_gds_hd_yn = source.te_gds_hd_yn AND
            target.pvsn_inst_cd = source.pvsn_inst_cd AND
            target.crtr_ymd = source.crtr_ymd AND
            target.alt_text = source.alt_text AND
            target.brno = source.brno AND
            target.frcs_reg_se = source.frcs_reg_se AND
            target.frcs_nm = source.frcs_nm AND
            target.frcs_stlm_info_se = source.frcs_stlm_info_se AND
            target.frcs_rprs_telno = source.frcs_rprs_telno AND
            target.usage_rgn_cd = source.usage_rgn_cd AND
            target.frcs_zip = source.frcs_zip AND
            target.frcs_addr = source.frcs_addr AND
            target.frcs_dtl_addr = source.frcs_dtl_addr AND    
            target.lat = source.lat AND
            target.lot = source.lot AND
            target.emd_cd = source.emd_cd                         
        """
    }
}

# Delta 테이블 초기화
def initialize_delta_table(catalog_table, schema):
    try:
        spark.sql(f"DESCRIBE TABLE {catalog_table}")
        print(f"Delta 테이블 {catalog_table}이 이미 존재합니다.")
    except:
        print(f"Delta 테이블 {catalog_table}이 존재하지 않음. 생성 중...")
        spark.sql(f"""
            CREATE TABLE {catalog_table}
            ({", ".join([f"{field.name} {field.dataType.simpleString().upper()}" for field in schema.fields])})
            USING DELTA
        """)
        print(f"Delta 테이블 {catalog_table} 생성 완료.")

# Delta 테이블에서 마지막 페이지 가져오기
def get_last_page(catalog_table):
    try:
        existing_data = spark.table(catalog_table)
        if existing_data.count() == 0:
            return 1  # 테이블이 비어 있으면 page=1 반환
        last_page = existing_data.selectExpr("max(page) as max_page").collect()[0]["max_page"]
        return int(last_page)
    except Exception as e:
        print(f"Delta 테이블에서 마지막 페이지 가져오는 데 실패: {e}")
        return 1

# 데이터 저장 및 ID 계산
def save_to_delta(data, schema, catalog_table, merge_conditions, current_page, per_page=1000):
    if data:
        for idx, item in enumerate(data, start=1):
            item["id"] = (current_page - 1) * per_page + idx  # ID 계산
        df = spark.createDataFrame(data, schema=schema)
        df.createOrReplaceTempView("temp_new_data")
        spark.sql(f"""
            MERGE INTO {catalog_table} AS target
            USING temp_new_data AS source
            ON {merge_conditions}
            WHEN NOT MATCHED THEN INSERT *
        """)
        print(f"Delta 테이블에 저장 완료: {len(data)}개 데이터 저장됨. Page {current_page}, ID 할당 완료.")

# 마지막 ID 출력
def print_last_id(catalog_table):
    try:
        existing_data = spark.table(catalog_table)
        last_id = existing_data.select(spark_max("id").alias("max_id")).collect()[0]["max_id"]
        print(f"Delta 테이블의 총 개수: {last_id}")
    except Exception as e:
        print(f"Delta 테이블에서 총 개수 가져오는 데 실패: {e}")

# API 호출 및 데이터 수집
def run_api_collection(api_name):
    settings = api_settings[api_name]
    url = settings["url"]
    catalog_table = settings["catalog_table"]
    schema = settings["schema"]
    merge_conditions = settings["merge_conditions"]

    # Delta 테이블 초기화
    initialize_delta_table(catalog_table, schema)

    # 데이터 수집 및 저장
    start_page = get_last_page(catalog_table)
    params = {
        'serviceKey': dbutils.secrets.get(scope="asac_6", key="datago"),
        'type': 'json',
        'perPage': 1000,
    }
    page = start_page
    total_count = 0

    while True:
        params['page'] = page
        print(f"Page {page}: 호출 중...")

        retries = 0
        while retries < 3:
            try:
                response = requests.get(url, params=params, timeout=10)
                response.raise_for_status()
                items = response.json().get("data", [])
                current_page = response.json().get("page", page)

                # 첫 번째 호출 시 totalCount 출력
                if page == start_page and total_count == 0:
                    total_count = response.json().get("totalCount", 0)
                    print(f"API 전체 데이터 수 (totalCount): {total_count}")

                if not items:
                    print(f"Page {page}: 데이터 없음. 종료.")
                    print_last_id(catalog_table)  # 마지막 ID 출력
                    return
                collected_time = datetime.now(timezone(timedelta(hours=9))).strftime('%Y-%m-%d %H:%M:%S')
                for item in items:
                    item["collected_time"] = collected_time
                    item["page"] = current_page
                save_to_delta(items, schema, catalog_table, merge_conditions, current_page)
                page = current_page + 1
                break
            except requests.exceptions.RequestException as e:
                retries += 1
                print(f"Page {page}: 오류 발생 - {e}. 재시도 중 ({retries}/3)...")
                time.sleep(2)
        if retries == 3:
            print("최대 재시도 초과. 종료.")
            print_last_id(catalog_table)  # 마지막 ID 출력
            return

    print(f"API {api_name} 수집 및 저장 완료.")

# API 실행
run_api_collection("franchise")
