import streamlit as st
from utils.vanna_model import VannaModelManager as NormalVannaModelManager
from utils.vanna_model_dash import VannaModelManager as DashboardVannaModelManager
from utils.text_model import RAGTextManager
import logging
import time
import asyncio
import pandas as pd
from sqlalchemy import create_engine, text  # SQL 실행을 위한 라이브러리
import json  # JSON 처리를 위한 라이브러리
import ssl  # SSL 인증 관련 라이브러리
import numpy as np  # 수치 계산을 위한 라이브러리
from openai import OpenAI
from langchain.prompts import ChatPromptTemplate
from langchain_community.chat_models import ChatOpenAI
from langchain_openai import ChatOpenAI
from langchain.schema import BaseOutputParser
from typing import List, Union
from datetime import datetime
import os
from utils.font_loader import load_custom_font
from utils.css_loader import load_css

# Load css
load_css("ui/styles.css")
load_css("ui/chatbot_styles.css")
load_custom_font()

# SSL 인증 설정
ssl._create_default_https_context = ssl._create_unverified_context

# 설정
config = {
    "api_key": st.secrets["api"]["api_key"],
    "model": "gpt-4o-mini"
}

server_hostname = st.secrets["databricks"]["host"]
http_path = st.secrets["databricks"]["http_path"]
access_token = st.secrets["databricks"]["personal_access_token"]
catalog = "gold"
schema = "normal_chatbot"


client = OpenAI(api_key=config["api_key"])
# Streamlit 캐싱을 활용한 모델 초기화
@st.cache_resource
def initialize_model_managers():
    """일반 챗봇, 대시보드 챗봇, 텍스트 챗봇 모델 매니저를 모두 초기화합니다."""
    normal_manager = NormalVannaModelManager(
        config=config,
        server_hostname=server_hostname,
        http_path=http_path,
        access_token=access_token,
        catalog=catalog,
        schema=schema
    )

    dashboard_manager = DashboardVannaModelManager(
        config=config,
        server_hostname=server_hostname,
        http_path=http_path,
        access_token=access_token,
        catalog=catalog,
        schema=schema
    )

    # 텍스트 챗봇 설정을 config 딕셔너리로 전달
    text_config = {
        "api_key": config["api_key"],
        "model": config["model"],
        "vector_store_path": "vector_store",
        "vector_store_name": "text_chatbot_chromadb"
    }

    text_manager = RAGTextManager(config=text_config)  # config 키워드 인자로 전달

    return normal_manager, dashboard_manager, text_manager

@st.cache_resource
def get_model_managers():
    return initialize_model_managers()

# 초기화된 모델 매니저들을 가져옴
normal_manager, dashboard_manager, text_manager = get_model_managers()

# 라우터 출력 파서 정의
class ModelRouterOutputParser(BaseOutputParser):
    def parse(self, text: str) -> str:
        """모델 타입을 파싱합니다."""
        text = text.strip().lower()
        if "dashboard" in text:
            return "dashboard"
        elif "text" in text:
            return "text"
        return "normal"

def determine_model_type(question: str) -> str:
    """LangChain을 사용하여 적절한 모델 타입을 결정합니다."""
    try:
        engine = create_engine(
            url=(f"databricks://token:{access_token}@{server_hostname}?"
                 f"http_path={http_path}&catalog=gold&schema=model_branch")
        )

        # run_sql 함수 정의
        def run_sql(sql: str) -> pd.DataFrame:
            with engine.connect() as connection:
                df = pd.read_sql_query(sql=text(sql), con=connection)
                return df

        # 분류를 위한 데이터 로드
        example_query = """
        SELECT `질문` AS question, `모델타입` AS model_type
        FROM gold.model_branch.branch_example
        """
        table_info_query = """
        SELECT `모델타입` AS model_type, `테이블명` AS table_name, `테이블설명` AS table_info
        FROM gold.model_branch.branch_table_info
        """

        # SQL 실행
        example_df = run_sql(example_query)
        table_info_df = run_sql(table_info_query)

        # 데이터프레임을 보기 좋게 포맷팅
        examples_str = example_df.to_string(index=False)  # index 제거
        table_info_str = table_info_df.to_string(index=False)  # index 제거

        print("🚀 [DEBUG] 테이블 정보:\n", table_info_str)
        print("🚀 [DEBUG] 모델 분류 예시:\n", examples_str)

        if example_df.empty:
            return "normal"

        # ✅ **프롬프트를 문자열로 직접 정의**
        template = """
        당신은 질문을 분석하여 적절한 모델을 선택하는 전문가입니다.

        1. 각 모델이 사용하는 테이블 정보:
        {table_info}

        2. 모델 분류 예시:
        {examples}

        3. 모델 타입별 특징:
        - normal: 일반적인 SQL 쿼리를 생성하여 데이터를 조회하는 질문
        - dashboard: 대시보드에 사용된 테이블의 SQL 쿼리를 생성하여 데이터를 조회하는 질문
        - text: SQL 쿼리 없이 일반 텍스트 응답이 필요한 질문

        사용자 질문: {question}

        위 정보를 바탕으로 이 질문에 가장 적합한 모델 타입을 선택하세요.
        응답은 반드시 "normal", "dashboard", "text" 중 하나여야 합니다.

        선택한 모델 타입:
        """

        # ✅ **format()을 직접 적용해서 최종 프롬프트 생성**
        raw_prompt = template.format(
            table_info=table_info_str,
            examples=examples_str,
            question=question
        )

        print("🚀 [DEBUG] LangChain에 전달될 최종 프롬프트:\n", raw_prompt)

        # ✅ **LangChain 체인을 사용하지 않고 직접 LLM 호출**
        llm = ChatOpenAI(
            model="gpt-4o-mini",
            temperature=0,
            api_key=config["api_key"]
        )
        response = llm.invoke(raw_prompt)

        print("🚀 [DEBUG] LangChain 응답 원본:", response)

        # 응답을 처리하여 모델 타입 결정
        response_text = response.content.strip().lower()  # ✅ 여기를 수정!
        if "dashboard" in response_text:
            return "dashboard"
        elif "text" in response_text:
            return "text"
        return "normal"

    except Exception as e:
        print(f"[ERROR] 모델 타입 결정 중 오류 발생: {e}")
        return "normal"

def save_feedback_to_databricks(question, response, model_type, feedback):
    """
    사용자 질의, 생성된 응답, 사용한 모델, 피드백을 Databricks에 저장
    Args:
        question (str): 사용자 질문
        response (str): 생성된 응답 (SQL 쿼리 또는 자연어 답변)
        model_type (str): 사용된 모델 타입 (sql_normal, sql_dashboard, text_normal)
        feedback (str): 사용자 피드백
    """
    try:
        current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

        # INSERT 쿼리 생성
        insert_query = f"""
        INSERT INTO gold.chatbot_feedback.user_feedback
        (question, response, model_type, user_feedback, created_at)
        VALUES ('{question}', '{response}', '{model_type}', '{feedback}', '{current_time}')
        """

        # Databricks에 저장
        engine = create_engine(f"databricks:///{catalog}.{schema}",
                             connect_args={
                                 "server_hostname": server_hostname,
                                 "http_path": http_path,
                                 "access_token": access_token
                             })
        with engine.connect() as connection:
            connection.execute(text(insert_query))
            connection.commit()

        return True
    except Exception as e:
        print(f"[ERROR] 피드백 저장 중 오류 발생: {e}")
        return False

# Streamlit UI
st.header("🤖 Chatbot")
st.write("질문에 맞는 SQL 쿼리 혹은 자연어 응답을 생성합니다.")

st.divider()

col1, col2 = st.columns([4,6])

with col1:
    # 질문 리스트 정의
    question_list = [
        "경기도에서 음식점 업종의 가맹점 수는?",
        "전국에서 가장 높은 지류 지역화폐 할인율을 제공하는 지역과 할인율을 알려줘",
        "서울에서 1회평균결제금액이 높은 지역은?",
        "경기도의 지역화폐 가맹점 중 계속사업 가맹점 수 알려줘",
        "영동사랑상품권 구매 방법은?",
        "춘천과 원주 지역화폐 혜택 비교해 줘"
    ]
    ### 📌 오른쪽 (질문 입력 및 실행) ###
    st.markdown('<div class="fixed-right">', unsafe_allow_html=True)
    # 질문 리스트 출력
    with st.expander("📋 질문 예시 보기"):
        clicked_question = st.radio("질문을 선택하세요:", question_list, index=None, label_visibility="collapsed")

    model_options = {
        "default": "LLM 자동 선택",
        "sql_normal": "페이봇-SQL",
        "sql_dashboard": "페이봇-SQL(대시보드)",
        "text_normal": "페이봇-QA"
    }
    selected_model = st.selectbox(
        "모델 선택:",
        options=list(model_options.keys()),
        format_func=lambda x: model_options[x],
        index=0
    )
    # 직접 입력 필드
    st.write("질문을 직접 입력하거나 추천 질문을 선택하세요:")
    user_question = st.text_input('질문', value=clicked_question or "", placeholder="예: 서울시에 등록된 지역화폐 가맹점 수는?", label_visibility="collapsed")
    input_button = st.button("질문 실행")

with col2:
    # 질문 처리 및 결과 출력 부분 수정
    if input_button:
        if user_question.strip():
            with st.spinner("답변을 생성중입니다..."):

                # LangChain을 사용한 모델 선택
                if selected_model == "default":
                    model_type = determine_model_type(user_question)
                    print(f"✅ [DEBUG] 결정된 모델 타입: {model_type}")
                    if model_type == "dashboard":
                        selected_manager = dashboard_manager
                        actual_model_type = "dashboard_chatbot"  # 실제 사용된 모델 타입 저장
                        is_text_model = False
                    elif model_type == "text":
                        selected_manager = text_manager
                        actual_model_type = "text_chatbot"    # 실제 사용된 모델 타입 저장
                        is_text_model = True
                    else:  # normal
                        selected_manager = normal_manager
                        actual_model_type = "normal_chatbot"     # 실제 사용된 모델 타입 저장
                        is_text_model = False

                else:
                    # 사용자가 직접 선택한 모델 사용
                    actual_model_type = selected_model       # 사용자가 선택한 모델 그대로 사용
                    if selected_model == "sql_normal":
                        selected_manager = normal_manager
                        is_text_model = False
                        actual_model_type = "normal_chatbot"
                    elif selected_model == "sql_dashboard":
                        selected_manager = dashboard_manager
                        is_text_model = False
                        actual_model_type = "dashboard_chatbot"
                    else:  # text_normal
                        selected_manager = text_manager
                        is_text_model = True
                        actual_model_type = "text_chatbot"

                max_retries = 3
                success = False

                for attempt in range(1, max_retries + 1):
                    try:
                        if is_text_model:
                            # 메타데이터 추출
                            sido_list, sigungu_list, currency_list = text_manager.model.extract_metadata()

                            # 필터 생성
                            filter_response = text_manager.generate_filter_prompt(
                                user_question, sido_list, sigungu_list, currency_list
                            )

                            # JSON 추출 및 ChromaDB 필터 변환
                            filters = text_manager.extract_json(filter_response)
                            chroma_filter = text_manager.transform_filters_to_chroma(filters)

                            # QA 실행
                            response = text_manager.model.perform_qa(user_question, chroma_filter)
                            success = True
                            break
                        else:
                            generated_sql, df_result = selected_manager.ask_question(user_question)
                            success = True
                            break
                    except Exception as e:
                        logging.error(f"시도 {attempt}번째에 오류가 발생했습니다: {e}")
                        time.sleep(0.3)

                if success:
                    if is_text_model:
                        # 텍스트 모델 응답 표시
                        st.markdown("**답변**")
                        col_response, col_feedback = st.columns([3, 1])
                        with col_response:
                            st.write(response)
                        generated_response = response
                    else:
                        # SQL 쿼리 표시
                        st.markdown("**생성된 SQL 쿼리**")
                        st.code(generated_sql)

                        # 쿼리 실행 결과와 피드백 버튼을 같은 줄에 배치
                        st.markdown("**쿼리 실행 결과**")
                        col_result, col_feedback = st.columns([3, 1])
                        with col_result:
                            st.dataframe(df_result)
                        generated_response = generated_sql

                    # 피드백 버튼 (공통 부분)
                    with col_feedback:
                        st.write("")  # 여백
                        st.write("응답이 도움되었나요?")
                        if st.button("👍 좋아요"):
                            if save_feedback_to_databricks(
                                question=user_question,
                                response=generated_response,  # 모델 타입에 따라 response 또는 generated_sql
                                model_type=actual_model_type,
                                feedback="good"
                            ):
                                st.success("감사합니다!")
                            else:
                                st.error("저장 실패")
                        if st.button("👎 싫어요"):
                            if save_feedback_to_databricks(
                                question=user_question,
                                response=generated_response,  # 모델 타입에 따라 response 또는 generated_sql
                                model_type=actual_model_type,
                                feedback="bad"
                            ):
                                st.success("감사합니다!")
                            else:
                                st.error("저장 실패")
                else:
                    st.error("응답 생성에 실패했습니다. 입력을 확인하거나 다시 시도해주세요.")
        else:
            st.warning("질문을 입력해주세요.")
    st.markdown('</div>', unsafe_allow_html=True)

st.divider()

# 📌 토글 버튼 (큰 카테고리)
st.subheader("📊 데이터 컬럼 정보")
tab1, tab2 = st.tabs(["📂 페이봇-SQL","📂 페이봇-SQL(대시보드)"])

with tab1:
    selected_button = st.selectbox("테이블:", ["한국조폐공사_지역사랑상품권_가맹점기본정보_전국", "한국조폐공사_지역사랑상품권_결제정보_전국", "한국조폐공사_지역사랑상품권_운영정보_전국", "한국조폐공사_지역사랑상품권_지류_지자체별_판매정책정보_전국", "한국조폐공사_지역사랑상품권_지류_지점별_재고정보_전국", "한국조폐공사_지역사랑상품권_지류_판매지점정보_전국", "한국조폐공사_지역사랑상품권_지류_환전지점정보_전국", "한국조폐공사_지역사랑상품권_카드모바일_지자체별_판매정책정보_전국"], index=None)

    # 선택된 버튼에 따라 다른 데이터 표시
    if selected_button == "한국조폐공사_지역사랑상품권_가맹점기본정보_전국":
        st.write("컬럼 정보")
        st.table({
            "컬럼명": ["bk_awa_perf_hd_yn", "onl_dlvy_ent_use_yn", "pos_use_yn", "ppr_frcs_aply_yn", "bzmn_stts", "bzmn_stts_nm", "ksic_cd","ksic_cd_nm", "qr_reg_conm", "te_gds_hd_yn","pvsn_inst_cd", "crtr_ymd", "alt_text", "brno", "frcs_reg_se", "frcs_reg_se_nm", "frcs_nm", "frcs_stlm_info_se", "frcs_stlm_info_se_nm", "frcs_rprs_telno", "usage_rgn_cd", "frcs_zip", "frcs_addr", "frcs_dtl_addr", "lat","lot", "emd_cd","emd_nm","sido","sigungu"],
            "설명": ["도서 및 공연 취급여부", "온라인 배달업체 사용여부", "POS 사용여부", "지류가맹점 신청여부", "사업자상태코드 (사업자 상태, 01: 계속사업자 02: 휴업자 03: 폐업자)", "사업자상태코드명", "표준산업분류코드","표준산업분류코드명","QR 등록 상호명","면세상품취급여부","제공기관코드(운영대행사 코드)","제공기관 가맹점 정보 생성, 변경, 해지 일자","개방용으로 생성되는 가맹점 구분값","사업자등록번호","01: 신규 02: 변경 03: 해지","코드에 해당하는 명칭: 신규, 변경, 해제","가맹점명","가맹점 결제유형 구분(01: 카드, 02: 모바일, 03: 지류)","가맹점 결제유형 구분에 해당하는 명칭: 카드, 모바일, 지류","가맹점대표전화번호(- 제외 숫자로만 구성)","사용처지역코드(법정동코드(지역코드 10자리) 중, 앞 5자리(시도코드 2자리 + 시군구코드 3자리))","가맹점우편번호(- 없는 연속된 숫자)","가맹점주소","가맹점상세주소","위도","경도","읍면동코드(법정동코드(지역코드 10자리) 중, 앞 8자리(시도코드 2자리 + 시군구코드 3자리 + 읍면동코드 3자리))","읍면동명","시도","시군구"]
        })

    elif selected_button == "한국조폐공사_지역사랑상품권_결제정보_전국":
        st.write("컬럼 정보")
        st.table({
            "컬럼명": ["crtr_ym", "usage_rgn_cd", "par_gend", "par_ag", "stlm_nocs", "stlm_amt", "card_use_amt","mbl_user_cnt", "mbl_use_amt", "emd_cd","emd_nm","sido","sigungu"],
            "설명": ["기준연월","사용처지역코드(법정동코드(지역코드 10자리) 중, 앞 5자리(시도코드 2자리 + 시군구코드 3자리))","결제자성별(M: 남성 F: 여성)","결제자연령대(01(~19세), 02(20~29세), 03(30~39세), 04(40~49세), 05(50~59세), 06(60세~), null)","결제건수(취소내역을 반영한 월간결제건수)","결제금액(취소 내역을 반영한 월간결제금액)","카드사용금액","모바일 이용자수(실제 결제를 한번이라도 진행한 사용자 수)","모바일 사용금액","읍면동코드(법정동코드(지역코드 10자리) 중, 앞 8자리(시도코드 2자리 + 시군구코드 3자리 + 읍면동코드 3자리))","읍면동명","시도","시군구"]
        })

    elif selected_button == "한국조폐공사_지역사랑상품권_운영정보_전국":
        st.write("컬럼 정보")
        st.table({
            "컬럼명": ["crtr_ym", "pvsn_inst_cd", "usage_rgn_cd", "card_pblcn_qty", "mbl_joiner_cnt", "mbl_chg_amt", "ppr_ntsl_amt","ppr_rtrvl_amt", "sido","sigungu"],
            "설명": ["기준연월","제공기관코드","사용처지역코드(법정동코드(지역코드 10자리) 중, 앞 5자리(시도코드 2자리 + 시군구코드 3자리))","카드발행수량","모바일 가입자수","모바일 충전금액","지류판매액(은행에서 고객에게 판매한 금액, 고객이 상품권을 구매한 기준)","지류회수액(가맹점에서 은행에 환전 요청이 들어온 금액, 가맹점 기준)","시도","시군구"]
        })

    elif selected_button == "한국조폐공사_지역사랑상품권_지류_지자체별_판매정책정보_전국":
        st.write("컬럼 정보")
        st.table({
            "컬럼명": ["expt_plcy_yn", "pvsn_inst_cd", "crtr_ymd", "usage_rgn_cd", "dscnt_plcy_aplcn_bgng_ymd", "dscnt_plcy_aplcn_end_ymd", "dscnt_rt","dy_prchs_lmt_amt", "mm_prchs_lmt_amt", "yr_prchs_lmt_amt", "max_excng_lmt_amt", "max_dscnt_lmt_amt", "corp_smpl_prchs_yn", "corp_dscnt_prchs_yn", "sido", "sigungu"],
            "설명": ["예외정책여부(Y/N)", "예외정책여부(Y/N)", "데이터 기준일자", "사용처지역코드(법정동코드(지역코드 10자리) 중, 앞 5자리(시도코드 2자리 + 시군구코드 3자리))", "할인정책적용시작일자", "할인정책적용종료일자","할인율","일간구매제한금액","월간구매제한금액","연간구매제한금액","최대환전제한금액","최대할인제한금액","법인단순구매가능여부(Y/N)","법인할인구매가능여부(Y/N)","시도","시군구"]
        })

    elif selected_button == "한국조폐공사_지역사랑상품권_지류_지점별_재고정보_전국":
        st.write("컬럼 정보")
        st.table({
            "컬럼명": ["pvsn_inst_cd", "crtr_ymd", "emd_cd", "emd_nm", "brnch_id", "brnch_nm", "gt_type_dnmn","gt_type_stc_qty", "usage_rgn_cd", "usage_rgn_nm"],
            "설명": ["제공기관코드(운영대행사 코드)", "데이터 기준일자", "읍면동코드(법정동코드(지역코드 10자리) 중, 앞 8자리(시도코드 2자리 + 시군구코드 3자리 + 읍면동코드 3자리))", "읍면동명", "지점구분ID", "지점명","상품권종액면가","상품권재고여부(0:없음, 1:있음)","사용처지역코드(법정동코드(지역코드 10자리) 중, 앞 5자리(시도코드 2자리 + 시군구코드 3자리))","사용처지역명"]
        })

    elif selected_button == "한국조폐공사_지역사랑상품권_지류_판매지점정보_전국":
        st.write("컬럼 정보")
        st.table({
            "컬럼명": ["pvsn_inst_cd", "crtr_ymd", "emd_cd", "emd_nm", "brnch_id", "brnch_nm", "brnch_addr","brnch_daddr", "brnch_rprs_telno", "gt_nm", "excng_yn", "lat", "lot", "sido", "sigungu"],
            "설명": ["제공기관코드(운영대행사 코드)", "데이터 기준일자", "읍면동코드(법정동코드(지역코드 10자리) 중, 앞 8자리(시도코드 2자리 + 시군구코드 3자리 + 읍면동코드 3자리))", "읍면동명", "지점구분ID", "지점명","지점주소","지점상세주소","지점대표전화번호('-' 제외 숫자로만 구성)","상품권명","판매여부(Y/N)","지점주소에 해당하는 위도값","지점주소에 해당하는 경도값","시도", "시군구"]
        })

    elif selected_button == "한국조폐공사_지역사랑상품권_지류_환전지점정보_전국":
        st.write("컬럼 정보")
        st.table({
            "컬럼명": ["pvsn_inst_cd", "crtr_ymd", "emd_cd", "emd_nm", "brnch_id", "brnch_nm", "brnch_addr","brnch_daddr", "brnch_rprs_telno", "gt_nm", "excng_yn", "lat", "lot", "sido", "sigungu"],
            "설명": ["제공기관코드(운영대행사 코드)", "데이터 기준일자", "읍면동코드(법정동코드(지역코드 10자리) 중, 앞 8자리(시도코드 2자리 + 시군구코드 3자리 + 읍면동코드 3자리))", "읍면동명", "지점구분ID", "지점명","지점주소","지점상세주소","지점대표전화번호(- 제외 숫자로만 구성)","상품권명","환전여부(Y/N)","지점주소에 해당하는 위도값","지점주소에 해당하는 경도값","시도", "시군구"]
        })

    elif selected_button == "한국조폐공사_지역사랑상품권_카드모바일_지자체별_판매정책정보_전국":
        st.write("컬럼 정보")
        st.table({
            "컬럼명": ["mm_prchs_lmt_amt", "pvsn_inst_cd", "crtr_ymd", "usage_rgn_cd", "dscnt_rt", "sido", "sigungu"],
            "설명": ["월간구매제한금액", "제공기관코드(운영대행사 코드)", "데이터 기준일자", "사용처지역코드(법정동코드(지역코드 10자리) 중, 앞 5자리(시도코드 2자리 + 시군구코드 3자리))", "할인율", "시도", "시군구"]
        })

with tab2:
    selected_button = st.selectbox("테이블:", ["가맹점", "결제현황", "이용현황"], index=None, key="col2_radio")

    # 선택된 버튼에 따라 다른 데이터 표시
    if selected_button == "가맹점":
        st.write("컬럼 정보")
        st.table({
            "컬럼명": ["시도명", "시군구명", "상호명", "업종명", "도로명주소", "지번주소", "우편번호", "사업자등록번호", "휴폐업상태", "폐업일자"],
            "설명": ["시도명", "시군구명", "상호명", "업종명", "도로명주소", "지번주소", "우편번호", "사업자등록번호", "휴폐업상태", "폐업일자"]
        })

    elif selected_button == "결제현황":
        st.write("컬럼 정보")
        st.table({
            "컬럼명": ["1회평균결제금액", "결제건수", "결제금액", "출생연도", "시도명", "시군구명", "행정동명", "업종명", "성별", "일", "연도", "월", "연령대", "요일", "결제취소건수", "결제취소금액"],
            "설명": ["결제금액/결제건수 (객단가)", "결제건수", "결제금액", "출생연도", "시도명", "시군구명", "행정동명", "업종명", "성별", "일", "연도", "월", "연령대", "요일", "결제취소건수", "결제취소금액"]
        })

    elif selected_button == "이용현황":
        st.write("컬럼 정보")
        st.table({
            "컬럼명": ["시도명", "시군구명", "연도", "월", "월별신규가입자수_명", "월별충전액_백만원", "월별사용액_백만원"],
            "설명": ["시도명", "시군구명", "연도", "월", "월별신규가입자수_명", "월별충전액_백만원", "월별사용액_백만원"]
        })
st.markdown('</div>', unsafe_allow_html=True)
