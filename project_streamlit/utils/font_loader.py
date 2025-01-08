import streamlit as st

def load_custom_font(font_family='Pretendard'):
    st.markdown(
        f"""
        <style>
        @import url("https://cdn.jsdelivr.net/gh/orioncactus/pretendard@v1.3.8/dist/web/static/pretendard.css");
        
        /* Use the custom font for specific tags */
        html, body, h1, h2, h3, h4, h5, h6, p, [class*="css"] {{
            font-family: '{font_family}';
        }}
        </style>
        """,
        unsafe_allow_html=True
    )
