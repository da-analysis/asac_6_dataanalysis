import databricks.sql as sql
import streamlit as st

def connect_to_databricks():
    try:
        host = st.secrets["databricks"]["host"]
        http_path = st.secrets["databricks"]["http_path"]
        access_token = st.secrets["databricks"]["personal_access_token"]

        connection = sql.connect(
            server_hostname=host,
            http_path=http_path,
            access_token=access_token
        )

        st.success("✅ Databricks에 성공적으로 연결되었습니다.")
        return connection

    except Exception as e:
        st.error(f"❌ Databricks 연결에 실패했습니다.: {e}")
        return None

def embed_dashboard(dashboard_url, width='100%', height='100%', max_height=1000):
    st.components.v1.html(
        f"""
        <style>
            @import url("https://cdn.jsdelivr.net/gh/orioncactus/pretendard@v1.3.8/dist/web/static/pretendard.css");
            html, body, [class*="stFrame"]  {{
                height: 100%;
                margin: 0;
                font-family: 'Pretendard';
            }}
        </style>
        <iframe 
            src="{dashboard_url}" 
            width={width} 
            height={height} 
            frameborder="0" 
            style="position: absolute; top: 0; left: 0; bottom: 0; right: 0;">
        </iframe>
        """,
        height=max_height,
    )