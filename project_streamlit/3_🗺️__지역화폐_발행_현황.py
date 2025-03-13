import streamlit as st
import folium
from streamlit_folium import st_folium
from folium import Popup
import geopandas as gpd
from utils.db_connector import embed_dashboard
from utils.font_loader import load_custom_font
from utils.css_loader import load_css

# Load css
load_css("ui/styles.css")
load_css("ui/map_styles.css")
load_custom_font()

st.header('🗺️ 전국 지역화폐 발행 현황')

# ✅ GeoJSON 로드 최적화
@st.cache_data
def load_geojson(file_path):
    return gpd.read_file(file_path)

geojson_path = 'data/Sigungu_ver20241001.json'
gdf = load_geojson(geojson_path)  # 캐싱된 데이터 활용

# 타겟 지역명칭
target_names = [
    "서울특별시 종로구", "서울특별시 중구", "서울특별시 용산구", "서울특별시 성동구", "서울특별시 광진구",
    "서울특별시 동대문구", "서울특별시 중랑구", "서울특별시 성북구", "서울특별시 강북구", "서울특별시 노원구",
    "서울특별시 도봉구", "서울특별시 은평구", "서울특별시 서대문구", "서울특별시 마포구", "서울특별시 양천구",
    "서울특별시 강서구", "서울특별시 구로구", "서울특별시 금천구", "서울특별시 영등포구", "서울특별시 동작구",
    "서울특별시 관악구", "서울특별시 강동구", "서울특별시 서초구", "서울특별시 강남구", "서울특별시 송파구",
    "경기도 수원시", "경기도 용인시", "경기도 고양시", "경기도 화성시", "경기도 성남시", "경기도 부천시",
    "경기도 남양주시", "경기도 안산시", "경기도 평택시", "경기도 안양시", "경기도 시흥시", "경기도 파주시",
    "경기도 김포시", "경기도 의정부시", "경기도 광주시", "경기도 하남시", "경기도 광명시", "경기도 군포시",
    "경기도 양주시", "경기도 오산시", "경기도 이천시", "경기도 안성시", "경기도 구리시", "경기도 의왕시",
    "경기도 포천시", "경기도 양평군", "경기도 여주시", "경기도 동두천시", "경기도 과천시", "경기도 가평군",
    "경기도 연천군", "인천광역시 동구", "인천광역시 미추홀구", "인천광역시 연수구", "인천광역시 부평구",
    "인천광역시 계양구", "인천광역시 서구", "부산광역시 동구", "부산광역시 남구", "대구광역시 군위군",
    "강원특별자치도 춘천시", "강원특별자치도 원주시", "강원특별자치도 강릉시", "강원특별자치도 동해시",
    "강원특별자치도 태백시", "강원특별자치도 속초시", "강원특별자치도 삼척시", "강원특별자치도 홍천군",
    "강원특별자치도 횡성군", "강원특별자치도 영월군", "강원특별자치도 정선군", "강원특별자치도 철원군",
    "강원특별자치도 화천군", "강원특별자치도 양구군", "강원특별자치도 인제군", "강원특별자치도 고성군",
    "충청북도 청주시", "충청북도 충주시", "충청북도 제천시", "충청북도 보은군", "충청북도 옥천군",
    "충청북도 영동군", "충청북도 증평군", "충청북도 진천군", "충청북도 괴산군", "충청북도 음성군",
    "충청북도 단양군", "충청남도 천안시", "충청남도 공주시", "충청남도 보령시", "충청남도 아산시",
    "충청남도 서산시", "충청남도 논산시", "충청남도 계룡시", "충청남도 당진시", "충청남도 금산군",
    "충청남도 부여군", "충청남도 서천군", "충청남도 청양군", "충청남도 홍성군", "충청남도 예산군",
    "충청남도 태안군", "전북특별자치도 전주시", "전북특별자치도 군산시", "전북특별자치도 익산시", "전북특별자치도 정읍시",
    "전북특별자치도 남원시", "전북특별자치도 김제시", "전북특별자치도 완주군", "전북특별자치도 진안군", "전북특별자치도 무주군",
    "전북특별자치도 장수군", "전북특별자치도 임실군", "전북특별자치도 순창군", "전북특별자치도 고창군", "전북특별자치도 부안군",
    "전라남도 목포시", "전라남도 여수시", "전라남도 순천시", "전라남도 나주시", "전라남도 광양시",
    "전라남도 담양군", "전라남도 곡성군", "전라남도 구례군", "전라남도 고흥군", "전라남도 보성군",
    "전라남도 화순군", "전라남도 장흥군", "전라남도 강진군", "전라남도 해남군", "전라남도 영암군",
    "전라남도 무안군", "전라남도 함평군", "전라남도 영광군", "전라남도 장성군", "전라남도 완도군",
    "전라남도 진도군", "전라남도 신안군", "경상북도 포항시", "경상북도 경주시", "경상북도 김천시",
    "경상북도 안동시", "경상북도 구미시", "경상북도 영주시", "경상북도 영천시", "경상북도 상주시",
    "경상북도 문경시", "경상북도 경산시", "경상북도 의성군", "경상북도 청송군", "경상북도 영양군",
    "경상북도 영덕군", "경상북도 청도군", "경상북도 고령군", "경상북도 성주군", "경상북도 칠곡군",
    "경상북도 예천군", "경상북도 봉화군", "경상북도 울진군", "경상북도 울릉군", "경상남도 창원시",
    "경상남도 진주시", "경상남도 통영시", "경상남도 사천시", "경상남도 김해시", "경상남도 밀양시",
    "경상남도 거제시", "경상남도 양산시", "경상남도 의령군", "경상남도 함안군", "경상남도 창녕군",
    "경상남도 고성군", "경상남도 남해군", "경상남도 하동군", "경상남도 산청군", "경상남도 함양군",
    "경상남도 거창군", "경상남도 합천군"
]

# 타겟 시도 이름
target_prefixes = [
    "대전광역시", "세종특별자치시", "경기도", "강원특별자치도", "울산광역시", "서울특별시",
    "경상남도", "대구광역시", "부산광역시", "제주특별자치도", "광주광역시", "인천광역시"
]

# ✅ Folium 지도 객체 생성
m = folium.Map(location=[36.5000, 128.5000], zoom_start=7, tiles='cartodbpositron')

for _, row in gdf.iterrows():
    if row['sgg_nm'] in target_names:
        color = 'blue'
    elif any(row['sgg_nm'].startswith(prefix) for prefix in target_prefixes):
        color = 'green'
    else:
        color = 'gray'

    popup = Popup(row['sgg_nm'], parse_html=True)

    folium.GeoJson(
        row['geometry'],
        style_function=lambda x, color=color: {
            'fillColor': color,
            'color': 'black',
            'weight': 2,
            'fillOpacity': 0.6,
        },
        tooltip=folium.Tooltip(row['sgg_nm']),
    ).add_to(m)

# ✅ Streamlit UI 최적화
col1, col2 = st.columns(2, vertical_alignment="top")

with col1:
    st.markdown('🟩시도 지역화폐&nbsp;&nbsp;🟦시군구 지역화폐')
    st_folium(m, width=600, height=600)

with col2:
    embed_dashboard("https://tacademykr-dataanalysis.cloud.databricks.com/embed/dashboardsv3/01efcbf6fb8312e09dcfa6a57687aa13?o=2766527531974171",
                   height='650')

embed_dashboard("https://tacademykr-dataanalysis.cloud.databricks.com/embed/dashboardsv3/01efcc06ae7f168ea06c616214e9e790?o=2766527531974171",
               height='500')