import streamlit as st
from utils.db_connector import connect_to_databricks
from utils.font_loader import load_custom_font
from utils.css_loader import load_css

# 페이지 설정
st.set_page_config(page_title="전국 지역화폐 데이터 현황판", page_icon="🪙", layout="wide")

# load css
load_custom_font()
load_css("ui/styles.css")

# 네비게이션 바
pg = st.navigation([
    st.Page("1_🏠__Main.py"),
    st.Page("2_🗺️__지역화폐_발행_현황.py"),
    st.Page("3_📊__대시보드.py"),
    st.Page("4_🤖__Chatbot.py")
])
pg.run()