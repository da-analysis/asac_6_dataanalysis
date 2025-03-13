import streamlit as st
from utils.font_loader import load_custom_font
from utils.css_loader import load_css
from PIL import Image

load_css("ui/styles.css")
load_css("ui/main_styles.css")
load_custom_font()

# ✅ 이미지 로드를 캐시하여 성능 최적화
@st.cache_data
def load_team_image():
    return Image.open('images/팀원소개.png')

img = load_team_image()

# ✅ Streamlit UI
st.header('🪙 전국 지역화폐 데이터 현황판 구축 프로젝트')

# ✅ 프로젝트 설명
st.write('\n')
with st.container():
    st.markdown('''
        본 프로젝트는 지역 경제의 핵심인 지역화폐 관련 데이터를 종합적으로 분석하고 시각화하여 제공하는 프로젝트입니다.
        **왜 중요할까요?**
        지역화폐는 지역 내 소상공인을 지원하고 지역 경제를 활성화하는 중요한 도구입니다. 하지만 현재 지역화폐는 정보 접근성의 부족, 거래 데이터 관리의 어려움, 그리고 발행 방식의 비일관성과 같은 다양한 문제에 직면하고 있습니다.

        이를 해결하기 위해 본 프로젝트는 지역화폐 이용의 투명성을 강화하고 효율성을 제고하는 것을 목표로 합니다. 이를 위해, 분석을 통해 얻은 인사이트를 **대시보드**를 통해 사용자와 공유하며, 사용자가 원하는 데이터를 직접 찾아볼 수 있는 **챗봇**을 제공합니다.

        이곳에서는 **공공데이터포털, 경기데이터드림, 국가통계포털**에서 취합한 **300개**의 테이블에서 **148,749,654개** 이상의 데이터에 대한 분석을 종합적으로 확인하실 수 있습니다.

        지금 바로 **대시보드**와 **챗봇**을 통해 지역화폐 데이터를 한눈에 확인하고 원하는 데이터를 찾아보세요!
    ''')

# ✅ 팀원 소개
st.divider()
st.subheader('팀원 소개')
