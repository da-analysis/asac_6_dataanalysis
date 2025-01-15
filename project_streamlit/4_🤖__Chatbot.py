import streamlit as st
from utils.vanna_model import VannaModelManager
import logging
import time
import asyncio
from utils.font_loader import load_custom_font
from utils.css_loader import load_css

# Load css
load_css("ui/styles.css")
load_css("ui/chatbot_styles.css")
load_custom_font()

# 설정
config = {
    "api_key": st.secrets["api"]["api_key"],
    "model": "gpt-4o-mini"
}

server_hostname = st.secrets["databricks"]["host"]
http_path = st.secrets["databricks"]["http_path"]
access_token = st.secrets["databricks"]["personal_access_token"]
catalog = "silver"
schema = "nationwide"

# Streamlit 캐싱을 활용한 모델 초기화
def initialize_model_manager():
    """모델 매니저를 초기화."""
    if "model_manager" not in st.session_state:
        with st.spinner("🤖 모델을 초기화하는 중입니다. 잠시만 기다려주세요..."):
            st.session_state.model_manager = VannaModelManager(
                config=config,
                server_hostname=server_hostname,
                http_path=http_path,
                access_token=access_token,
                catalog=catalog,
                schema=schema,
            )
    return st.session_state.model_manager


# Streamlit 인터페이스
st.header("🤖 Text-to-SQL Chatbot")
st.write("질문을 입력하면, 데이터베이스에서 관련 데이터를 가져옵니다.")

st.divider()
st.write("질문을 입력하세요:")
user_question = st.text_input('', placeholder="예: 서울시에 등록된 지역화폐 가맹점 수는?",label_visibility="collapsed")

# 모델 매니저 초기화
model_manager = initialize_model_manager()

# 질문 처리 및 결과 출력
if st.button("질문 실행") or user_question:
    if user_question.strip():
        with st.spinner("답변을 생성중입니다..."):
            max_retries = 3
            success = False
            generated_sql = None
            df_result = None

            for attempt in range(1, max_retries + 1):
                try:
                    # 학습된 질문 확인 및 프롬프트 구성
                    if attempt == 1:
                        prompt = f"{user_question}"
                    else:
                        prompt = f"{user_question} 잘못된 쿼리문이야. 제공된 테이블과 컬럼 정보를 활용해 쿼리문을 새롭게 정확히 만들어줘."

                    # 질문 실행
                    generated_sql, df_result = model_manager.ask_question(user_question=user_question)
                    success = True
                    break  # 성공 시 루프 종료
                except Exception as e:
                    logging.error(f"시도 {attempt}번째에 오류가 발생했습니다: {e}")
                    time.sleep(0.3)  # 재시도 간 대기

            if success:
                st.write("생성된 SQL 쿼리:")
                st.code(generated_sql)

                st.write("쿼리 실행 결과:")
                st.dataframe(df_result)
            else:
                st.error("SQL 쿼리 생성에 실패했습니다. 입력을 확인하거나 다시 시도해주세요.")
    else:
        st.warning("질문을 입력해주세요.")