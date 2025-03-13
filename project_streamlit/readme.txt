# Streamlit 작동방법
명령 프롬프트
cd "project_streamlit 경로(수정)"
streamlit run app.py

# css 구성
메인 테마는 .streamlit/config에서 정의
그 외 css는 ui 폴더에서 불러옴

# git push
깃 푸시 안 할 놈들은 .gitignore에 정의
중요) .streamlit 폴더에 있는 파일은 깃허브에 업로드 안되니 서버에서 적용 필요

# 중요) 챗봇 vanna 버전 관리
vanna 버전 호환을 위해서는 VS Build Tools C++ 설치 반드시 필요

# app.py 정보가 Streamlit 웹페이지에 실시간 반영
# 각 페이지별 파일들은 독립적으로 구성, app.py 아래서 한꺼번에 실행됨

written by gom-jung
