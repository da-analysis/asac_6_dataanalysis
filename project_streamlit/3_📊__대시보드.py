import streamlit as st
from utils.db_connector import embed_dashboard
from utils.font_loader import load_custom_font
from utils.css_loader import load_css

# Load css
load_css("ui/styles.css")
load_css("ui/dashboard_styles.css")
load_custom_font()

# 사이드바(css 각별 주의)
dashboard_options = ['전국', '설문조사', '경기', '경기도 지역화폐 가맹점 현황 종합분석']
selected_sidebar = st.sidebar.radio('', dashboard_options)

# 메인 대시보드 구역
if selected_sidebar == "전국":
    st.subheader('KPI 대시보드')

# 대시보드
if selected_sidebar == "설문조사":
    st.subheader('설문조사')
    embed_dashboard("https://tacademykr-dataanalysis.cloud.databricks.com/embed/dashboardsv3/01efbd13c0361cb1ad26794454fb92c2?o=2766527531974171")
    
if selected_sidebar == "경기도 지역화폐 가맹점 현황 종합분석":
    st.subheader('경기도 지역화폐 가맹점 현황 종합분석')
    embed_dashboard("https://tacademykr-dataanalysis.cloud.databricks.com/embed/dashboardsv3/01efc42a6be31b5fb3d5a64cd2242c0e?o=2766527531974171")


