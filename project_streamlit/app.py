import streamlit as st
from utils.font_loader import load_custom_font
from utils.css_loader import load_css
from PIL import Image

# 페이지 설정
st.set_page_config(page_title="전국 지역화폐 데이터 현황판", page_icon="🪙", layout="wide")

# CSS 및 폰트 로드
@st.cache_resource
def load_styles():
    load_custom_font()
    load_css("ui/styles.css")

load_styles()

# 로고
@st.cache_resource
def load_logo_images():
    return Image.open('images/logo.png'), Image.open('images/logo2.png')

logo, logo2 = load_logo_images()

# 네비게이션 바
pg = st.navigation([
    st.Page("1_🏠__Main.py"),
    st.Page("2_대시보드_조회_신청.py"),
    st.Page("3_🗺️__지역화폐_발행_현황.py"),
    st.Page("4_📊__대시보드.py"),
    st.Page("5_🤖__Chatbot.py")
])
pg.run()

st.logo(
    logo,
    size="large",
    link="https://asacurrency.co.kr",
    icon_image=logo2
)