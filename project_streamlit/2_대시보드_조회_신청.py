import streamlit as st
from utils.sheets_connector import add_user_to_sheet
from utils.font_loader import load_custom_font
from utils.css_loader import load_css
from PIL import Image

@st.cache_resource
def load_styles():
    load_css("ui/styles.css")
    load_css("ui/regis_styles.css")
    load_custom_font()

# ✅ CSS 및 폰트 로드
load_styles()

if "registered" not in st.session_state:
    st.session_state.registered = False

if "checkbox_state" not in st.session_state:
    st.session_state.checkbox_state = False

# images
@st.cache_data
def load_images():
    imgs = {}
    for i in range(1, 8):
        imgs[i] = Image.open(f'images/register/{i}.png')
    return imgs

imgs = load_images()

# Streamlit UI
st.header("Databricks 사용자 등록 및 조회 권한 부여")

# Google Sheets 정보 입력
sheet_url = "https://docs.google.com/spreadsheets/d/1j0-AJF3og1k5L-lWnPUKHHTac8ocBnnrKexsivDl4UE"
sheet_name = "시트1"

st.write('''
        크롬, 엣지, 사파리 환경에서 진행하는 것을 추천드립니다. \n
        대시보드 조회 권한 부여를 위해 Databricks에 사용자 이메일, 이름 및 소속을 등록하는 과정입니다. \n
        사용자 편의를 위해 거치는 과정으로, 입력된 정보는 대시보드 조회/실행 이외의 목적으로는 사용되지 않습니다. \n
        문의사항: admin@asacurrency.co.kr
        ''')

st.divider()

# 사용자 이메일 입력
user_email = st.text_input("이메일")
user_name = st.text_input("이름")
user_afiliation = st.text_input("소속")

# 체크박스 상태를 업데이트하는 콜백 함수
def update_checkbox():
    st.session_state.checkbox_state = not st.session_state.checkbox_state

# ✅ 체크박스: 변경 감지 후 UI 갱신
st.checkbox("동의합니다.", value=st.session_state.checkbox_state, key="checkbox", on_change=update_checkbox)

# 빈 공간 유지
placeholder = st.empty()

# ✅ 체크박스가 체크된 경우에만 등록 버튼 활성화
if st.session_state.checkbox_state:
    with placeholder:
        if st.button("등록"):
            if user_email and user_name and user_afiliation:
                sheet_result = add_user_to_sheet(
                    sheet_url,
                    sheet_name,
                    user_email,
                    user_name,
                    user_afiliation
                )
                st.success(sheet_result)
                st.session_state.registered = True
            else:
                st.error("이메일, 이름 및 소속을 모두 입력하세요.")
else:
    placeholder.empty()  # 체크 해제 시 등록 버튼 자리 차지 안 하도록 설정

st.divider()

# ✅ `st.expander` 위치를 고정하여 깜빡임 방지
expander_placeholder = st.empty()
with expander_placeholder.container():
    with st.expander("이용가이드"):
        col1, col2, col3 = st.columns([0.05, 0.9, 0.05])

        with col1:
            st.write('')
        with col2:
            st.write('1. 본 페이지에서 이메일 및 이름을 입력하고 등록 버튼을 누릅니다.')
            st.image(imgs[1])
            st.write('\n')

            st.write('2. 입력한 이메일 주소에서 Databricks에서 온 메일을 확인하고 Join now 버튼을 누릅니다. 메일 수신에는 시간이 소요될 수 있습니다.')
            st.image(imgs[2])
            st.write('\n')

            st.write('3. 리다이렉팅 된 Databricks에서 등록한 이메일 주소를 통해 로그인합니다.')
            st.image(imgs[3])
            st.write('\n')

            st.write('4. 인증 코드 입력 창이 나오면 이메일에서 수신된 인증 코드를 확인하고 입력합니다.')
            st.image(imgs[4])
            st.image(imgs[5])
            st.write('\n')

            st.write('5. asacurrency 웹서비스로 돌아와 대시보드를 조회/실행합니다.')
            st.image(imgs[6], caption="Databricks에서 본 페이지가 나타나는 것은 정상입니다. 이후에는 웹서비스로 다시 돌아오면 됩니다.")
            st.write('\n')
            st.image(imgs[7])
