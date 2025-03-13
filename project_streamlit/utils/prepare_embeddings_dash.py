from vanna_model_dash import VannaModelManager
import pandas as pd
import os
import json

# 설정
config = {
    "api_key": "",
    "model": "gpt-4o-mini"
}

server_hostname = ""
http_path = ""
access_token = ""
catalog = "gold"
schema = "normal_chatbot"

current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(current_dir)

def prepare_embeddings():
    """질문-쿼리 매핑 데이터셋을 로드하고 임베딩을 생성하여 ChromaDB에 저장"""
    try:
        print("[INFO] 임베딩 준비 시작")

        config["vector_store_path"] = os.path.join(project_root, "vector_store")  # ✅ 경로는 고정
        config["vector_store_name"] = "dashboard_chatbot_chromadb"  # 🔹 폴더 이름만 변경

        # VannaModelManager 초기화
        model_manager = VannaModelManager(
            config=config,
            server_hostname=server_hostname,
            http_path=http_path,
            access_token=access_token,
            catalog=catalog,
            schema=schema
        )

        # ChromaDB 컬렉션 초기화
        model_manager.reset_chromadb_collections()

        # 질문-쿼리 매핑 데이터 로드
        mapping_query = """
        SELECT
            `질문` AS question,
            `쿼리` AS query
        FROM gold.dashboard_chatbot_qq_mapping.qq_mapping_table
        """
        mapping_df = model_manager.vn.run_sql(mapping_query)

        if mapping_df.empty:
            print("[WARN] 질문-쿼리 매핑 테이블이 비어 있습니다.")
            return

        print(f"[INFO] 임베딩 생성 시작: 총 {len(mapping_df)}개 질문-쿼리 쌍")

        # 각 질문-쿼리 쌍에 대해 임베딩 생성 및 저장
        for idx, row in mapping_df.iterrows():
            print(f"[INFO] 임베딩 생성 중: {idx+1}/{len(mapping_df)}")
            model_manager.vn.train(
                question=row['question'],
                sql=row['query'],
            )
            model_manager.vn.store_question_with_embedding(
                question=row['question'],
                sql=row['query']
            )

        print("[INFO] 임베딩 준비 완료")

    except Exception as e:
        print(f"[ERROR] 임베딩 준비 중 오류 발생: {e}")

if __name__ == "__main__":
    prepare_embeddings()
