# Streamlit 작동방법
명령 프롬프트
cd "project_streamlit 경로(수정)"
streamlit run app.py

# css 구성
메인 테마는 .streamlit/config에서 정의
그 외 css는 ui 폴더에서 불러옴

# git push
깃 푸시 안 할 놈들은 .gitignore에 정의
.streamlit/secrets.toml은 정의 필수!

# app.py 정보가 Streamlit 웹페이지에 실시간 반영
# 각 페이지별 파일들은 독립적으로 구성, app.py 아래서 한꺼번에 실행됨