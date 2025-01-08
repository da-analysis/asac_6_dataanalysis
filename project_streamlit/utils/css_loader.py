import streamlit as st

def load_css(file_path):
    try:
        with open(file_path, "r") as f:
            css_content = f"<style>{f.read()}</style>"
            st.markdown(css_content, unsafe_allow_html=True)
    except FileNotFoundError:
        st.error("CSS 파일이 존재하지 않습니다. 경로를 확인하세요.")