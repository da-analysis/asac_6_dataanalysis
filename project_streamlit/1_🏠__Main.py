import streamlit as st
from utils.font_loader import load_custom_font
from utils.css_loader import load_css
from PIL import Image

# Load css
load_css("ui/styles.css")
load_css("ui/main_styles.css")
load_custom_font()

img = Image.open('images/팀원소개.png')

# Title
st.header('🪙 전국 지역화폐 데이터 현황판 구축 프로젝트')

# Description
st.write('\n')
with st.container():
    st.markdown('''
                본 프로젝트는 지역 경제의 핵심인 지역화폐의 거래 내역 및 이용 현황을 종합적으로 분석하고 시각화하여 제공하는 프로젝트입니다. \n
                **왜 중요할까요?** \n
                지역화폐는 지역 내 소상공인을 지원하고, 지역 경제를 활성화하는 중요한 도구입니다. 그러나 관련 데이터는 여러 곳에 흩어져 있어 한눈에 파악하기 어려웠습니다. 본 프로젝트는 이러한 문제를 해결하고, 지역화폐 데이터를 이용자가 한눈에 이해할 수 있도록 기획되었습니다. \n
                이곳에서는 **공공데이터포털, 경기데이터드림, 국가통계포털**에서 취합한 **300개**의 테이블에서 **148,749,654개** 이상의 데이터에 대한 분석을 종합적으로 확인하실 수 있습니다.\n
                지금 바로 **대시보드**와 **챗봇**을 통해 지역화폐 데이터를 한눈에 확인하고 원하는 데이터를 찾아보세요!
                ''')

# 기능 소개
st.divider()
st.subheader('기능 소개')

tab1, tab2 = st.tabs(['📊대시보드', '🤖챗봇'])

with tab1:
    st.markdown('사용자와 정책 담당자가 데이터를 직관적으로 활용하고 분석할 수 있도록 지역화폐 대시보드를 제공합니다.')

with tab2:
    st.markdown('Text to SQL 기능으로 사용자가 챗봇을 통해 요청한 데이터를 제공합니다. 원하는 데이터를 마음껏 요청해 보세요!')

# 팀원 소개
st.divider()
st.subheader('팀원 소개')
st.image(img)