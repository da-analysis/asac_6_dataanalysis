import streamlit as st
from streamlit_scroll_to_top import scroll_to_here
from utils.db_connector import embed_dashboard
from utils.font_loader import load_custom_font
from utils.css_loader import load_css

@st.cache_resource
def load_styles():
    load_css("ui/styles.css")
    load_css("ui/dashboard_styles.css")
    load_custom_font()

# ✅ CSS 및 폰트 로드
load_styles()

# Scroll to top 버튼 추가
if 'scroll_to_top' not in st.session_state:
    st.session_state.scroll_to_top = False
def scroll():
    st.session_state.scroll_to_top = True
if st.session_state.scroll_to_top:
    scroll_to_here(0, key='top')
    st.session_state.scroll_to_top = False  # ✅ 스크롤 후 상태 초기화

# 사이드바(css 각별 주의) --- 분류 index: 0, 4, 9, 11, 13
@st.cache_data
def get_dashboard_options():
    return [
        "전국", "KPI 대시보드", "전국 지역화폐 가맹점 및 결제 현황", "설문통계자료",
        "경기", "경기도 지역화폐 가맹점 현황", "경기도 지역화폐 발행 및 이용 현황",
        "경기도 지역화폐 결제 현황", "경기도 지역화폐 지역별 정책 적용 효과 및 기타 현황",
        "서울", "서울시 지역화폐 결제 현황", "부산", "부산시 지역화폐 결제 현황",
        "그 외", "전라북도 무주군 지역화폐 결제 현황", "전라북도 익산시 지역화폐 결제 현황",
        "충청북도 옥천군 지역화폐 결제 현황", "경상남도 김해시 지역화폐 결제 현황"
    ]

dashboard_options = get_dashboard_options()
selected_sidebar = st.sidebar.radio('대시보드 선택', dashboard_options, label_visibility="collapsed")

## 메인 대시보드 구역
if selected_sidebar == "전국":
    st.subheader('KPI 대시보드')
    embed_dashboard("https://tacademykr-dataanalysis.cloud.databricks.com/embed/dashboardsv3/01efd191d73119bf8d7a26911be42f03?o=2766527531974171",
                   max_height=1900)
    st.button("🔝", on_click=scroll)

## 대시보드
# 1) 전국
if selected_sidebar == "KPI 대시보드":
    st.subheader('KPI 대시보드')
    embed_dashboard("https://tacademykr-dataanalysis.cloud.databricks.com/embed/dashboardsv3/01efd191d73119bf8d7a26911be42f03?o=2766527531974171",
                   max_height=1900)
    st.button("🔝", on_click=scroll)

if selected_sidebar == "전국 지역화폐 가맹점 및 결제 현황":
    st.subheader('전국 지역화폐 가맹점 및 결제 현황')
    embed_dashboard("https://tacademykr-dataanalysis.cloud.databricks.com/embed/dashboardsv3/01efc361fea51c2282bb6cd9fa5b3cc4?o=2766527531974171",
                   max_height=3700)
    st.button("🔝", on_click=scroll)

if selected_sidebar == "설문통계자료":
    st.subheader('설문통계자료')
    embed_dashboard("https://tacademykr-dataanalysis.cloud.databricks.com/embed/dashboardsv3/01efbd13c0361cb1ad26794454fb92c2?o=2766527531974171",
                   max_height=1900)
    st.button("🔝", on_click=scroll)


# 2) 경기
if selected_sidebar == "경기도 지역화폐 가맹점 현황":
    st.subheader('경기도 지역화폐 가맹점 현황')
    embed_dashboard("https://tacademykr-dataanalysis.cloud.databricks.com/embed/dashboardsv3/01efc42a6be31b5fb3d5a64cd2242c0e?o=2766527531974171",
                   max_height=1800)
    st.button("🔝", on_click=scroll)

if selected_sidebar == "경기도 지역화폐 발행 및 이용 현황":
    st.subheader('경기도 지역화폐 발행 및 이용 현황')
    embed_dashboard("https://tacademykr-dataanalysis.cloud.databricks.com/embed/dashboardsv3/01efc41d169e157490cab658a3cc2b36?o=2766527531974171",
                   max_height=1400)
    st.button("🔝", on_click=scroll)

if selected_sidebar == "경기도 지역화폐 결제 현황":
    st.subheader('경기도 지역화폐 결제 현황')
    embed_dashboard("https://tacademykr-dataanalysis.cloud.databricks.com/embed/dashboardsv3/01efc322d4031d9c95ba480fa4b036ad?o=2766527531974171",
                   max_height=1050)
    st.button("🔝", on_click=scroll)

if selected_sidebar == "경기도 지역화폐 지역별 정책 적용 효과 및 기타 현황":
    st.subheader('경기도 지역화폐 지역별 정책 적용 효과 및 기타 현황')
    embed_dashboard("https://tacademykr-dataanalysis.cloud.databricks.com/embed/dashboardsv3/01efccc98fa41113af8e1e8315f3c4a4?o=2766527531974171",
                   max_height=1100)
    st.button("🔝", on_click=scroll)


# 3) 서울
if selected_sidebar == "서울시 지역화폐 결제 현황":
    st.subheader('서울시 지역화폐 결제 현황')
    embed_dashboard("https://tacademykr-dataanalysis.cloud.databricks.com/embed/dashboardsv3/01efcbfff68e119fad2fdbdaded819a7?o=2766527531974171",
                    max_height=2650)
    st.button("🔝", on_click=scroll)

# 4) 부산
if selected_sidebar == "부산시 지역화폐 결제 현황":
    st.subheader('부산시 지역화폐 결제 현황')
    embed_dashboard("https://tacademykr-dataanalysis.cloud.databricks.com/embed/dashboardsv3/01efc674035f18aeb0629f6c11884563?o=2766527531974171",
                   max_height=2800)
    st.button("🔝", on_click=scroll)


# 5) 그 외
if selected_sidebar == "전라북도 무주군 지역화폐 결제 현황":
    st.subheader('전라북도 무주군 지역화폐 결제 현황')
    embed_dashboard("https://tacademykr-dataanalysis.cloud.databricks.com/embed/dashboardsv3/01efc42541b31ad0a3f0293b27b86f53?o=2766527531974171",
                   max_height=3300)
    st.button("🔝", on_click=scroll)

if selected_sidebar == "전라북도 익산시 지역화폐 결제 현황":
    st.subheader('전라북도 익산시 지역화폐 결제 현황')
    embed_dashboard("https://tacademykr-dataanalysis.cloud.databricks.com/embed/dashboardsv3/01efcd7c62601b51a8cf887323acf7c2?o=2766527531974171",
                    max_height=2500)
    st.button("🔝", on_click=scroll)

if selected_sidebar == "충청북도 옥천군 지역화폐 결제 현황":
    st.subheader('충청북도 옥천군 지역화폐 결제 현황')
    embed_dashboard("https://tacademykr-dataanalysis.cloud.databricks.com/embed/dashboardsv3/01efcd7c6fd81967a4637705711893eb?o=2766527531974171",
                   max_height=2400)
    st.button("🔝", on_click=scroll)

if selected_sidebar == "경상남도 김해시 지역화폐 결제 현황":
    st.subheader('경상남도 김해시 지역화폐 결제 현황')
    embed_dashboard("https://tacademykr-dataanalysis.cloud.databricks.com/embed/dashboardsv3/01efcd7c68c3133781ceddb831403e73?o=2766527531974171",
                   max_height=2200)
    st.button("🔝", on_click=scroll)
