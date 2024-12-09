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
    # 지류 지자체별 판매정보
    "salesPolicy_paper": {
        "url": "http://apis.data.go.kr/B190001/salesPolicy/paper",
        "catalog_table": "bronze.api_public.`한국조폐공사_지역사랑상품권_지류_지자체별_판매정책정보_전국`",
        "schema": StructType([
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
            StructField("collected_time", StringType(), True),
            StructField("id", IntegerType(), True),
            StructField("page", IntegerType(), True)
        ]),
        "merge_conditions": """
            target.expt_plcy_yn = source.expt_plcy_yn AND
            target.pvsn_inst_cd = source.pvsn_inst_cd AND
            target.crtr_ymd = source.crtr_ymd AND
            target.usage_rgn_cd = source.usage_rgn_cd AND
            target.dscnt_plcy_aplcn_bgng_ymd = source.dscnt_plcy_aplcn_bgng_ymd AND
            target.dscnt_plcy_aplcn_end_ymd = source.dscnt_plcy_aplcn_end_ymd AND
            target.dscnt_rt = source.dscnt_rt
        """
    },
    # 카드모바일 지자체별 판매정보
    "salesPolicy_cardMobile": {
        "url": "http://apis.data.go.kr/B190001/salesPolicy/cardMobile",
        "catalog_table": "bronze.api_public.`한국조폐공사_지역사랑상품권_카드모바일_지자체별_판매정책정보_전국`",
        "schema": StructType([
            StructField("mm_prchs_lmt_amt", StringType(), True),
            StructField("pvsn_inst_cd", StringType(), True),
            StructField("crtr_ymd", StringType(), True),
            StructField("usage_rgn_cd", StringType(), True),
            StructField("dscnt_rt", StringType(), True),
            StructField("collected_time", StringType(), True),
            StructField("id", IntegerType(), True),
            StructField("page", IntegerType(), True)
        ]),
        "merge_conditions": """
            target.mm_prchs_lmt_amt = source.mm_prchs_lmt_amt AND
            target.pvsn_inst_cd = source.pvsn_inst_cd AND
            target.crtr_ymd = source.crtr_ymd AND
            target.usage_rgn_cd = source.usage_rgn_cd AND
            target.dscnt_rt = source.dscnt_rt
        """
    },
    # 지류 판매지점정보
    "paperSale": {
        "url": "http://apis.data.go.kr/B190001/paperSale/v2/saleV2",
        "catalog_table": "bronze.api_public.`한국조폐공사_지역사랑상품권_지류_판매지점정보_전국`",
        "schema": StructType([
            StructField("pvsn_inst_cd", StringType(), True),
            StructField("crtr_ymd", StringType(), True),
            StructField("emd_cd", StringType(), True),
            StructField("emd_nm", StringType(), True),
            StructField("brnch_id", StringType(), True),
            StructField("brnch_nm", StringType(), True),
            StructField("brnch_addr", StringType(), True),
            StructField("brnch_daddr", StringType(), True),
            StructField("brnch_rprs_telno", StringType(), True),
            StructField("gt_nm", StringType(), True),
            StructField("ntsl_yn", StringType(), True),
            StructField("lat", StringType(), True),
            StructField("lot", StringType(), True),
            StructField("collected_time", StringType(), True),
            StructField("id", IntegerType(), True),
            StructField("page", IntegerType(), True)
        ]),
        "merge_conditions": """
            target.pvsn_inst_cd = source.pvsn_inst_cd AND
            target.crtr_ymd = source.crtr_ymd AND
            target.emd_cd = source.emd_cd AND
            target.brnch_id = source.brnch_id AND
            target.brnch_nm = source.brnch_nm AND
            target.brnch_addr = source.brnch_addr AND
            target.brnch_daddr = source.brnch_daddr AND
            target.brnch_rprs_telno = source.brnch_rprs_telno AND
            target.gt_nm = source.gt_nm AND
            target.ntsl_yn = source.ntsl_yn AND
            target.lat = source.lat AND
            target.lot = source.lot
        """
    },
    # 지류 환전지점정보
    "paperExchange": {
        "url": "http://apis.data.go.kr/B190001/paperExchange/v2/exchangeV2",
        "catalog_table": "bronze.api_public.`한국조폐공사_지역사랑상품권_지류_환전지점정보_전국`",
        "schema": StructType([
            StructField("pvsn_inst_cd", StringType(), True),
            StructField("crtr_ymd", StringType(), True),
            StructField("emd_cd", StringType(), True),
            StructField("emd_nm", StringType(), True),
            StructField("brnch_id", StringType(), True),
            StructField("brnch_nm", StringType(), True),
            StructField("brnch_addr", StringType(), True),
            StructField("brnch_daddr", StringType(), True),
            StructField("brnch_rprs_telno", StringType(), True),
            StructField("gt_nm", StringType(), True),
            StructField("excng_yn", StringType(), True),
            StructField("lat", StringType(), True),
            StructField("lot", StringType(), True),
            StructField("collected_time", StringType(), True),
            StructField("id", IntegerType(), True),
            StructField("page", IntegerType(), True)
        ]),
        "merge_conditions": """
            target.pvsn_inst_cd = source.pvsn_inst_cd AND
            target.crtr_ymd = source.crtr_ymd AND
            target.emd_cd = source.emd_cd AND
            target.brnch_id = source.brnch_id AND
            target.brnch_nm = source.brnch_nm AND
            target.brnch_addr = source.brnch_addr AND
            target.brnch_daddr = source.brnch_daddr AND
            target.brnch_rprs_telno = source.brnch_rprs_telno AND
            target.gt_nm = source.gt_nm AND
            target.excng_yn = source.excng_yn AND
            target.lat = source.lat AND
            target.lot = source.lot
        """
    },
    # 운영정보
    "operations": {
        "url": "http://apis.data.go.kr/B190001/localGiftsOperateV2/operationsV2",
        "catalog_table": "bronze.api_public.`한국조폐공사_지역사랑상품권_운영정보_전국`",
        "schema": StructType([
            StructField("pvsn_inst_cd", StringType(), True),
            StructField("crtr_ym", StringType(), True),
            StructField("usage_rgn_cd", StringType(), True),
            StructField("card_pblcn_qty", StringType(), True),
            StructField("mbl_joiner_cnt", StringType(), True),
            StructField("mbl_chg_amt", StringType(), True),
            StructField("ppr_ntsl_amt", StringType(), True),
            StructField("ppr_rtrvl_amt", StringType(), True),
            StructField("collected_time", StringType(), True),
            StructField("id", IntegerType(), True),
            StructField("page", IntegerType(), True)
        ]),
        "merge_conditions": """
            target.pvsn_inst_cd = source.pvsn_inst_cd AND
            target.crtr_ym = source.crtr_ym AND
            target.usage_rgn_cd = source.usage_rgn_cd AND
            target.mbl_joiner_cnt = source.mbl_joiner_cnt AND
            target.mbl_chg_amt = source.mbl_chg_amt AND
            target.ppr_ntsl_amt = source.ppr_ntsl_amt AND
            target.ppr_rtrvl_amt = source.ppr_rtrvl_amt
        """
    },
    # 지류 지점별 재고정보
    "stock": {
        "url": "http://apis.data.go.kr/B190001/paperStock/v2/stockV2",
        "catalog_table": "bronze.api_public.`한국조폐공사_지역사랑상품권_지류_지점별_재고정보_전국`",
        "schema": StructType([
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
            StructField("collected_time", StringType(), True),
            StructField("id", IntegerType(), True),
            StructField("page", IntegerType(), True)
        ]),
        "merge_conditions": """
            target.pvsn_inst_cd = source.pvsn_inst_cd AND
            target.crtr_ymd = source.crtr_ymd AND
            target.emd_cd = source.emd_cd AND
            target.brnch_id = source.brnch_id AND
            target.gt_type_dnmn = source.gt_type_dnmn AND
            target.gt_type_stc_qty = source.gt_type_stc_qty
        """
    },
    # 결제정보
    "payment": {
        "url": "http://apis.data.go.kr/B190001/localGiftsPaymentV3/paymentsV3",
        "catalog_table": "bronze.api_public.`한국조폐공사_지역사랑상품권_결제정보_전국`",
        "schema": StructType([
            StructField("crtr_ym", StringType(), True),
            StructField("usage_rgn_cd", StringType(), True),
            StructField("par_gend", StringType(), True),
            StructField("par_ag", StringType(), True),
            StructField("stlm_nocs", StringType(), True),
            StructField("stlm_amt", StringType(), True),
            StructField("card_use_amt", StringType(), True),
            StructField("mbl_user_cnt", StringType(), True),
            StructField("mbl_use_amt", StringType(), True),
            StructField("emd_cd", StringType(), True),
            StructField("emd_nm", StringType(), True),
            StructField("collected_time", StringType(), True),
            StructField("id", IntegerType(), True),
            StructField("page", IntegerType(), True)
        ]),
        "merge_conditions": """
            target.crtr_ym = source.crtr_ym AND
            target.usage_rgn_cd = source.usage_rgn_cd AND
            target.par_gend = source.par_gend AND
            target.par_ag = source.par_ag AND
            target.stlm_nocs = source.stlm_nocs AND
            target.stlm_amt = source.stlm_amt AND
            target.card_use_amt = source.card_use_amt AND
            target.mbl_user_cnt = source.mbl_user_cnt AND
            target.mbl_use_amt = source.mbl_use_amt AND
            target.emd_cd = source.emd_cd AND
            target.emd_nm = source.emd_nm
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
def save_to_delta(data, schema, catalog_table, merge_conditions, current_page, per_page=10000):
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
        'perPage': 10000,
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

# 데이터브릭스 파라미터 가져오기
dbutils.widgets.text("api_calls", "")  # api_calls라는 파라미터 정의
api_calls = dbutils.widgets.get("api_calls")  # 파라미터 값 가져오기

# 쉼표로 구분된 API 리스트로 변환
api_call_list = api_calls.split(",")

# API를 하나씩 호출
for api in api_call_list:
    run_api_collection(api)

