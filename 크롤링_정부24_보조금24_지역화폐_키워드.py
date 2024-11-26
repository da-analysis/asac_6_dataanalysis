# Databricks notebook source
import requests
import pandas as pd
from bs4 import BeautifulSoup
import re


def extract_data_from_urls(search_url):
    # 첫 번째 단계: 검색 URL에서 데이터 추출
    res = requests.get(search_url)
    soup = BeautifulSoup(res.text, 'html.parser')

    # 검색 결과를 담을 리스트
    result = []
    temp = soup.find_all('li', class_='result_li_box')

    for i in range(len(temp)):
        title = temp[i].find('a', class_='list_font17').text
        site = 'https://www.gov.kr/' + temp[i].find('a', class_='list_font17').get('href')
        content = temp[i].find('p', class_='list_info_txt').text.strip()
        region = temp[i].find('div', class_='badge_box').find_all('span')[1].text.strip()
        result.append([title, site, content, region])

    # 검색 결과를 DataFrame으로 변환
    base = pd.DataFrame(result, columns=['title', 'site', 'content', 'region'])

    # 두 번째 단계: 각 사이트의 데이터를 추출하고 데이터프레임에 추가
    all_columns = set(["소관기관", "최종수정일"])
    all_data = []

    for url in base['site']:
        res = requests.get(url)
        soup = BeautifulSoup(res.text, 'html.parser')
        data_dict = {col: "" for col in all_columns}

        # 소관기관 및 최종수정일 추출
        for item in soup.find_all('div', class_=['info-ins', 'info-date']):
            strong_text = item.find('strong').get_text(strip=True)
            span_text = item.find('span').get_text(strip=True)
            data_dict[strong_text] = span_text

        # 각 패널의 strong 태그와 해당 내용을 열로 추가
        for panel in soup.find_all('div', class_='tab-content'):
            h2_text = panel.find('h2', class_='blind').get_text(strip=True)
            for strong_tag in panel.find_all('strong'):
                column_name = f"{h2_text}: {strong_tag.get_text(strip=True)}"
                if column_name not in data_dict:
                    all_columns.add(column_name)  # 새 열 추가
                next_element = strong_tag.find_next_sibling()
                content = next_element.get_text(strip=True) if next_element else "내용 없음"
                data_dict[column_name] = content

        all_data.append(data_dict)

    # 모든 데이터프레임 열 업데이트
    df = pd.DataFrame(all_data).reindex(columns=sorted(all_columns))

    # 데이터 클린업: 정규식 적용
    df = df.applymap(lambda x: re.sub(r'\r|\n|\t|\s+', ' ', x) if isinstance(x, str) else x)

    # Base 데이터프레임과 결합
    combined_df = pd.concat([base, df], axis=1)
    combined_df = combined_df.fillna('')
    combined_df = combined_df.dropna(axis=1, how='all')

    return combined_df


# COMMAND ----------

# 실행할 URL 설정
search_url = '''https://www.gov.kr/search?srhQuery=%EC%A7%80%EC%97%AD%ED%99%94%ED%8F%90&collectionCd=rcv&textAnalyQuery=&
policyType=&webappType=&realQuery=%EC%A7%80%EC%97%AD%ED%99%94%ED%8F%90&pageSize=10000&publishOrg=&sfield=&
pageIndex=1&recommendpageIndex=1&sort=RANK&condSelTxt=&reSrchQuery=&sortSel=RANK'''

# 데이터 추출 실행
result_df = extract_data_from_urls(search_url)

# COMMAND ----------

display(result_df)

# COMMAND ----------

# # 데이터 저장 경로 설정 및 저장
# output_path = "/dbfs/FileStore/govern24.csv"
# result_df.to_csv(output_path, index=False, encoding='utf-8-sig')
# print(f"Data saved to {output_path}")
