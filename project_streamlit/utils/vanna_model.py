import pandas as pd
from vanna.base import VannaBase
from vanna.chromadb.chromadb_vector import ChromaDB_VectorStore
from openai import OpenAI
from sqlalchemy import create_engine, text
import ssl
import json
from sklearn.metrics.pairwise import cosine_similarity
import numpy as np
import uuid

ssl._create_default_https_context = ssl._create_unverified_context

class DbrxLLM(VannaBase):
    def __init__(self, config=None):
        self.client = OpenAI(api_key=config["api_key"])
        self.model = config["model"]

    def system_message(self, message: str) -> any:
        return {"role": "system", "content": message}

    def user_message(self, message: str) -> any:
        return {"role": "user", "content": message}

    def assistant_message(self, message: str) -> any:
        return {"role": "assistant", "content": message}

    def generate_sql(self, question: str, **kwargs) -> str:
        sql = super().generate_sql(question, **kwargs)
        sql = sql.replace("\\_", "_")  # 백슬래시 제거
        return sql

    def submit_prompt(self, user_question: str, **kwargs) -> str:
        base_prompt = """
        추가 응답 지침
        1. 가맹점과 관련된 질문은 "한국조폐공사_지역사랑상품권_가맹점기본정보_전국" 테이블을 참고하되, 
        기본적으로 한 사업체에 대해서 상태 변화에 따른 중복값이 존재하니 brno 컬럼을 기준으로 distinct 하여 결과를 출력하라.
        1-1. 만약 가맹점과 관련된 질문이지만 특정 기간의 사업체 상태(휴업,폐업,계속사업자)에 대해서 물어볼땐 필요에따라 distinct 하지않고 쿼리문을 작성해도된다.
        2. 지역, 사업체 상태, 지역화폐명 등 WHERE 문법을 사용해서 조건을 검색할땐 "="을 사용하지 않고 사용자가 질문한 단어를 해석해 LIKE 문법을 사용해 쿼리를 작성한다.
        2-1. 전북, 전라북도는 전북, 전라북도, 전북특별자치도 와 같은 다양한 이름으로 되어있으니 해당 지역에 대한 질문에는 OR을 통해 여러 경우를 고려해서 쿼리를 작성한다. (전라도도 마찬가지)
        3. 카드발행수량, 모바일 가입자수, 모바일 충전금액, 지류판매액, 지류회수액 데이터는 "한국조폐공사_지역사랑상품권_운영정보_전국" 테이블에 있다.
        3-1. 결제건수, 결제금액, 카드사용금액, 모바일 이용자수, 모바일 사용금액 데이터는 "한국조폐공사_지역사랑상품권_결제정보_전국" 테이블에 있다.
        3-2. "판매정책정보"라는 단어가 들어간 테이블은 모두 구매제한금액이나 할인율에 대한 데이터가 담겨있다.
        4. 사용자 질문에서 지역을 명시적으로 "시군구" 라고 하지 않으고 지역에 대한 질문을 한다면 기본적으론 "시도" 정보를 제공한다.(특정 지역끼리의 비교는 제외)
        """
        # user_question이 리스트로 전달된 경우 처리
        if isinstance(user_question, list):
            # 리스트를 문자열로 병합 (각 요소를 공백으로 구분)
            user_question = " ".join(str(item) for item in user_question)

        # user_question이 문자열인지 확인
        if not isinstance(user_question, str):
            raise ValueError(f"user_question은 문자열이어야 합니다. 전달된 값: {user_question}")
        
        # 'messages'를 올바른 배열 형태로 구성
        messages = [
            {"role": "system", "content": base_prompt.strip()},
            {"role": "user", "content": user_question.strip()}
        ]
        print("\n[DEBUG] [submit_prompt] 최종적으로 OpenAI API에 보낼 messages:\n", messages, "\n")
        chat_response = self.client.chat.completions.create(
            model=self.model,
            messages=messages,
            max_tokens=500,
            temperature=0.8
        )
        print(chat_response.choices[0].message.content)
        return chat_response.choices[0].message.content

class MyVanna(DbrxLLM, ChromaDB_VectorStore):
    def __init__(self, config):
        ChromaDB_VectorStore.__init__(self, config=config)  # ChromaDB_VectorStore 초기화
        DbrxLLM.__init__(self, config=config)  # DbrxLLM 초기화
        self.allow_llm_to_see_data = True

    def store_question_with_embedding(self, question: str, sql: str):
        try:
            # 임베딩 생성 디버깅
            print("[DEBUG] Generating embedding for question:", question)
            question_embedding = self.generate_embedding(question)
            print("[DEBUG] Generated embedding:", question_embedding)

            # 이미 리스트인지 확인
            if isinstance(question_embedding, np.ndarray):
                question_embedding = question_embedding.tolist()  # NumPy 배열인 경우만 변환

            # 데이터 저장
            document = {
                "question": question,
                "sql": sql,
                "embedding": question_embedding,  # 리스트 그대로 저장
            }
            self.sql_collection.add(
                documents=[json.dumps(document)],
                ids=[str(uuid.uuid4())]
            )
            print("[INFO] 질문과 임베딩 저장 완료:", document)
        except Exception as e:
            print(f"[ERROR] 질문 저장 중 오류 발생: {e}")


    def get_similar_question_sql(self, question: str, top_k: int = 5) -> list:
        try:
            print("[DEBUG] Fetching stored data from ChromaDB.")
            stored_data = self.sql_collection.get()
            documents = stored_data.get("documents", [])

            if not documents:
                print("[WARN] 저장된 질문이 없습니다.")
                return []

            # 입력 질문 임베딩 생성 디버깅
            print("[DEBUG] Generating embedding for input question:", question)
            question_embedding = self.generate_embedding(question)

            if isinstance(question_embedding, np.ndarray):
                question_embedding = question_embedding.tolist()
            
            print("[DEBUG] Input question embedding:", question_embedding)

            embeddings, stored_questions, sqls = [], [], []

            # 저장된 데이터 디버깅
            for idx, doc in enumerate(documents):
                try:
                    doc_data = json.loads(doc)
                    if "embedding" not in doc_data or not isinstance(doc_data["embedding"], list):
                        print(f"[WARN] 문서 {idx + 1}에 'embedding'이 없거나 잘못된 형식입니다. 건너뜁니다.")
                        continue

                    print(f"[DEBUG] Stored document {idx + 1}:", doc_data)

                    stored_questions.append(doc_data["question"])
                    sqls.append(doc_data["sql"])
                    embeddings.append(np.array(doc_data["embedding"]))
                except Exception as e:
                    print(f"[ERROR] 문서 {idx + 1} 처리 중 오류 발생: {e}")

            # 임베딩 리스트로 변환
            embeddings = np.array(embeddings)
            print("[DEBUG] All stored embeddings shape:", embeddings.shape)

            # 유사도 계산 디버깅
            print("[DEBUG] Calculating cosine similarity.")
            similarity_scores = cosine_similarity(
                np.array([question_embedding]), embeddings
            )[0]
            print("[DEBUG] Similarity scores:", similarity_scores)

            # 유사도 기준 상위 top_k 선택
            top_indices = np.argsort(similarity_scores)[::-1][:top_k]
            similar_questions = [
                {
                    "question": stored_questions[i],
                    "sql": sqls[i],
                    "score": similarity_scores[i],
                }
                for i in top_indices
            ]

            print("[INFO] 유사한 질문과 SQL 반환:", similar_questions)
            return similar_questions

        except Exception as e:
            print(f"[ERROR] get_similar_question_sql 실행 중 오류: {e}")
            return []


class VannaModelManager:
    def __init__(self, config, server_hostname, http_path, access_token, catalog, schema):
        self.config = config  # 수정: config를 클래스 속성으로 저장
        self.vn = MyVanna(config=config)
        self.vn.allow_llm_to_see_data = True

        # 데이터베이스 연결 설정
        self.engine = create_engine(
            url=(
                f"databricks://token:{access_token}@{server_hostname}?"
                f"http_path={http_path}&catalog={catalog}&schema={schema}"
            )
        )

        # 모델 설정 및 학습
        self._setup_model()
        self.reset_chromadb_collections()  # ChromaDB 컬렉션 초기화
        self.train_model()  # 초기화 후 학습 실행

    def _setup_model(self):
        def run_sql(sql: str) -> pd.DataFrame:
            df = pd.read_sql_query(sql=text(sql), con=self.engine.connect())
            return df

        self.vn.run_sql = run_sql
        self.vn.run_sql_is_set = True

    def reset_chromadb_collections(self):
        """ChromaDB에 등록된 모든 컬렉션을 초기화."""
        try:
            print("[INFO] 모든 ChromaDB 컬렉션 초기화를 시작합니다.")

            # SQL 컬렉션 초기화
            if self.vn.sql_collection:
                try:
                    self.vn.remove_collection("sql")
                    print("[INFO] SQL 컬렉션 초기화 완료.")
                except Exception as e:
                    print(f"[WARN] SQL 컬렉션 초기화 중 문제가 발생했지만 계속 진행합니다: {e}")
            else:
                print("[INFO] SQL 컬렉션이 존재하지 않아 초기화를 건너뜁니다.")

            # DDL 컬렉션 초기화
            if self.vn.ddl_collection:
                try:
                    self.vn.remove_collection("ddl")
                    print("[INFO] DDL 컬렉션 초기화 완료.")
                except Exception as e:
                    print(f"[WARN] DDL 컬렉션 초기화 중 문제가 발생했지만 계속 진행합니다: {e}")
            else:
                print("[INFO] DDL 컬렉션이 존재하지 않아 초기화를 건너뜁니다.")

            # Documentation 컬렉션 초기화
            if self.vn.documentation_collection:
                try:
                    self.vn.remove_collection("documentation")
                    print("[INFO] Documentation 컬렉션 초기화 완료.")
                except Exception as e:
                    print(f"[WARN] Documentation 컬렉션 초기화 중 문제가 발생했지만 계속 진행합니다: {e}")
            else:
                print("[INFO] Documentation 컬렉션이 존재하지 않아 초기화를 건너뜁니다.")

            print("[INFO] 모든 ChromaDB 컬렉션 초기화 완료.")

        except Exception as e:
            print(f"[ERROR] ChromaDB 초기화 중 치명적인 오류 발생: {e}")

    def train_model(self):
        try:
            print("[INFO] 모델 학습을 시작합니다.")
            # 1) silver 카탈로그, nationwide 스키마의 메타데이터
            query = """
            SELECT
                *
            FROM
                silver.information_schema.columns c
            WHERE
                c.table_catalog = 'silver'
                AND c.table_schema = 'nationwide';
            """
            print(query)
            df_metadata = self.vn.run_sql(query)
            print(df_metadata)
            
            print(self.vn.get_training_data())

            print("[DEBUG] df_metadata shape:", df_metadata.shape)

            if df_metadata.shape[0] == 0:
                print("[WARN] 메타데이터가 비어 있습니다. get_training_plan_generic()을 스킵합니다.")
            else:
                print("[DEBUG] Calling get_training_plan_generic() with metadata...")
                self.vn.get_training_plan_generic(df_metadata)
                print("[DEBUG] Done: get_training_plan_generic()")

            # 2) 질문-쿼리 매핑 테이블
            mapping_query = """
            SELECT
                `질문` AS question,
                `쿼리` AS query
            FROM silver.question_query_mapping_table.qq_mapping_table
            """
            print(mapping_query)
            mapping_df = self.vn.run_sql(mapping_query)
            print("[DEBUG] mapping_df shape:", mapping_df.shape)

            if mapping_df.shape[0] == 0:
                print("[WARN] 질문-쿼리 매핑 테이블도 비어 있습니다. 학습할 내용이 없습니다.")
                return
            
            print(f"[INFO] 학습할 질문-쿼리 쌍의 총 개수: {len(mapping_df)}")
            print(mapping_df.head())

            # 질문-쿼리 학습
            for idx, row in mapping_df.iterrows():
                q_text = row['question']
                sql_text = row['query']
                print(f"\n[TRAIN] {idx+1}/{len(mapping_df)}")
                print("[QUESTION]", repr(q_text))
                print("[SQL]", repr(sql_text))

                self.vn.train(
                    question=q_text,
                    sql=sql_text,
                )
                self.vn.store_question_with_embedding(question=q_text, sql=sql_text)

            print("[INFO] 모델 학습이 완료되었습니다.")

        except Exception as e:
            print(f"[ERROR] 모델 학습 중 오류 발생: {e}")


    def ask_question(self, user_question: str):
        """
        사용자의 질문을 처리하여 적합한 SQL을 생성하고 실행한 결과를 반환합니다.

        Args:
            user_question (str): 사용자가 입력한 질문.
            base_prompt (str): SQL 생성 시 사용할 기본 프롬프트.

        Returns:
            tuple: 생성된 SQL 쿼리와 실행 결과 DataFrame.
        """
        print("[DEBUG] ask_question called with user_question:", repr(user_question))

        def normalize_string(s):
            """중복된 공백 제거 및 전체 소문자 변환"""
            return " ".join(s.split()).lower().strip()

        try:
            print("[INFO] ChromaDB 상태를 디버깅합니다.")
            self.debug_chromadb()

            # Step 1: 저장된 데이터에서 동일한 질문 찾기
            all_data = self.vn.sql_collection.get()
            print("[DEBUG] All stored data from ChromaDB:", json.dumps(all_data, indent=4, ensure_ascii=False))

            if "documents" in all_data:
                for idx, doc in enumerate(all_data["documents"]):
                    try:
                        doc_data = json.loads(doc)
                        stored_question = doc_data.get("question", "")
                        stored_sql = doc_data.get("sql", "")
                        print("[DEBUG] Original question:", user_question)
                        print("[DEBUG] Normalized question:", normalize_string(user_question))
                        print("[DEBUG] Stored question:", stored_question)
                        print("[DEBUG] Normalized stored question:", normalize_string(stored_question))

                        if normalize_string(stored_question) == normalize_string(user_question):
                            print("[INFO] 학습된 질문과 동일한 질문이 감지되었습니다. 저장된 SQL을 반환하고 실행합니다.")
                            
                            # 저장된 SQL 실행
                            df_result = self.vn.run_sql(stored_sql)
                            return stored_sql, df_result
                    except json.JSONDecodeError as e:
                        print(f"[WARN] Document {idx} is not valid JSON: {e}")

            # Step 2: 동일한 질문을 찾지 못한 경우 새로운 SQL 생성
            print("[INFO] 동일한 질문을 찾을 수 없습니다. 새로운 SQL을 생성합니다.")
            generated_sql = self.vn.generate_sql(question=user_question)

            if not generated_sql:
                raise Exception("SQL 생성 실패")

            # Step 3: SQL 실행
            df_result = self.vn.run_sql(generated_sql)

            print("[DEBUG] [LLM Generated SQL] =======")
            print(generated_sql)
            print(df_result)
            print("===================================")

            return generated_sql, df_result

        except Exception as e:
            print(f"[ERROR] 질문 처리 중 오류 발생: {e}")
            return None, None

        
    def debug_chromadb(self):
        try:
            print("[INFO] ChromaDB 디버깅 시작")

            # SQL 컬렉션 상태 확인
            sql_data = self.vn.sql_collection.get()
            print("[DEBUG] SQL Collection 상태:")
            print(json.dumps(sql_data, indent=4, ensure_ascii=False))

            if "documents" in sql_data and sql_data["documents"]:
                for idx, doc in enumerate(sql_data["documents"]):
                    try:
                        parsed_doc = json.loads(doc)
                        print(f"[DEBUG] Document {idx + 1}: {parsed_doc}")
                    except json.JSONDecodeError as e:
                        print(f"[WARN] Document {idx + 1} JSON 디코딩 실패: {e}")

            else:
                print("[WARN] SQL 컬렉션에 저장된 문서가 없습니다.")

            print("[INFO] ChromaDB 디버깅 완료")
        except Exception as e:
            print(f"[ERROR] ChromaDB 디버깅 중 오류 발생: {e}")

