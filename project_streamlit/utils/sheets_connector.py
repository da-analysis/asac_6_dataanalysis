import streamlit
import gspread
from oauth2client.service_account import ServiceAccountCredentials

# Google Sheets API 인증
def authenticate_gspread():
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_name("service_account.json", scope)
    client = gspread.authorize(creds)
    return client

# Google Sheets에 사용자 이메일 및 이름 추가
def add_user_to_sheet(sheet_url, sheet_name, user_email, user_name, user_affiliation):
    """Google Sheets에 사용자 이메일과 이름 추가"""
    try:
        client = authenticate_gspread()
        sheet = client.open_by_url(sheet_url).worksheet(sheet_name)
        sheet.append_row([user_email, user_name, user_affiliation])  # 이메일(A열), 이름(B열), 소속(C열) 추가
        return f"✅ {user_email} ({user_name}) 등록이 완료되었습니다."
    except Exception as e:
        return f"❌ Google Sheets 에러: {str(e)}"




