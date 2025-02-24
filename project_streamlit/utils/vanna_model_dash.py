import pandas as pd
from vanna.base import VannaBase
from vanna.chromadb.chromadb_vector import ChromaDB_VectorStore
from openai import OpenAI
from sqlalchemy import create_engine, text
import ssl
import json
import numpy as np
import uuid
import os

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

    def submit_prompt(self, user_question: str, base_prompt: str = "", **kwargs) -> str:
        prompt = f"{base_prompt}\n{user_question}" if base_prompt else user_question
        print("\n[DEBUG] [submit_prompt] 최종적으로 OpenAI API에 보낼 messages:\n", prompt, "\n")
        chat_response = self.client.chat.completions.create(
            model=self.model,
            messages=prompt,
            max_tokens=500,
            temperature=0.8
        )
        print(chat_response.choices[0].message.content)
        return chat_response.choices[0].message.content

class MyVanna(DbrxLLM, ChromaDB_VectorStore):
    def __init__(self, config):
        vector_store_path = os.path.join(project_root, "vector_store")  # ✅ 경로 변경 없음
        vector_store_name = "dashboard_chatbot_chromadb"  # 기본 폴더명
        full_store_path = os.path.join(vector_store_path, vector_store_name)  # 기존 경로 + 폴더 이름
        ChromaDB_VectorStore.__init__(self, config={**config, "path": full_store_path})
        DbrxLLM.__init__(self, config=config)  # DbrxLLM 초기화
        self.allow_llm_to_see_data = True
        self.custom_initial_prompt = None  # 사용자 지정 프롬프트 변수 추가

    def set_initial_prompt(self, prompt: str):
        """사용자가 원하는 SQL 생성 프롬프트를 설정하는 메서드"""
        self.custom_initial_prompt = prompt

    def get_sql_prompt(
        self,
        initial_prompt: str,
        question: str,
        question_sql_list: list,
        ddl_list: list,
        doc_list: list,
        **kwargs,
    ):
        """
        부모 클래스의 get_sql_prompt를 오버라이딩하여 Response Guidelines 부분을 제거한다.
        """

        # ✅ 사용자가 설정한 `initial_prompt` 적용
        if self.custom_initial_prompt:
            initial_prompt = self.custom_initial_prompt
        elif initial_prompt is None:
            initial_prompt = "당신은 SQL 전문가입니다. 질문에 대한 SQL 쿼리를 생성하는 데 도움을 주세요."

        # ✅ DDL 및 문서 추가 (부모 메서드 활용)
        initial_prompt = self.add_ddl_to_prompt(initial_prompt, ddl_list, max_tokens=self.max_tokens)

        if self.static_documentation != "":
            doc_list.append(self.static_documentation)

        initial_prompt = self.add_documentation_to_prompt(initial_prompt, doc_list, max_tokens=self.max_tokens)

        # ✅ 🚀 여기서 **Response Guidelines를 추가하지 않음!**
        message_log = [self.system_message(initial_prompt)]

        # ✅ 저장된 질문 및 SQL 추가
        for example in question_sql_list:
            if example and "question" in example and "sql" in example:
                message_log.append(self.user_message(example["question"]))
                message_log.append(self.assistant_message(example["sql"]))

        # ✅ 현재 사용자 질문 추가
        message_log.append(self.user_message(question))

        return message_log

    def store_question_with_embedding(self, question: str, sql: str):
        try:
            # 임베딩 생성 디버깅
            question_embedding = self.generate_embedding(question)

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
        except Exception as e:
            print(f"[ERROR] 질문 저장 중 오류 발생: {e}")


    def get_similar_question_sql(self, question: str, top_k: int = 5) -> list:
        try:
            print("[DEBUG] Fetching similar questions using ChromaDB_VectorStore.")
            similar_questions = super().get_similar_question_sql(question)

            if not similar_questions:
                print("[WARN] 유사한 질문이 없습니다.")
                return []

            # 상위 5개 필터링
            filtered_questions = similar_questions[:top_k]
            return filtered_questions

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

            # 📌 기존 Query 유지
            query = """
            SELECT
                *
            FROM
                gold.information_schema.columns c
            WHERE
                c.table_catalog = 'gold'
                AND c.table_schema = 'dashboard_chatbot';
            """
            df_metadata = self.vn.run_sql(query)

            # 📌 테이블 정보 정리 (table_catalog, table_schema, table_name, column_name, data_type, comment)
            table_info_dict = {}
            for _, row in df_metadata.iterrows():
                table_key = f"{row['table_schema']}.`{row['table_name']}`"
                if table_key not in table_info_dict:
                    table_info_dict[table_key] = []

                # 컬럼 정보 추가
                table_info_dict[table_key].append(
                    f"  - 컬럼: {row['column_name']} ({row['data_type']}) → {row['comment'] or '설명 없음'}"
                )

            # 📌 최종적으로 GPT에 전달할 테이블 정보 문자열 생성
            formatted_table_info = "\n".join(
                [f"- 테이블: gold.{table_name}\n" + "\n".join(columns) for table_name, columns in table_info_dict.items()]
            )

            # 📌 기존 set_initial_prompt 코드 유지 + 테이블 정보만 추가
            base_prompt = """
            당신은 SQL 전문가입니다. 질문에 대한 SQL 쿼리를 생성하는 데 도움을 주세요.
            응답은 제공된 컨텍스트에만 기반해야 하며, 응답 지침 및 형식 지침을 따라야 합니다.

            === 응답 지침 ===
            1. 제공된 컨텍스트가 충분한 경우, 질문에 대한 유효한 SQL 쿼리를 설명 없이 생성하세요.
            2. 제공된 컨텍스트가 부족한 경우, 왜 쿼리를 생성할 수 없는지 설명하세요.
            3. 가장 관련성이 높은 테이블을 사용하세요.
            4. 해당 질문이 이전에 이미 답변된 적이 있다면, 동일한 답변을 그대로 반복하세요.
            5. 출력되는 SQL이 SQL 문법을 준수하고 실행 가능하며, 문법 오류가 없도록 하세요.
            6. 지역, 휴폐업상태 등 WHERE 문법을 사용해서 조건을 검색할땐 '='을 사용하지 않고 사용자가 질문한 단어를 해석해 'LIKE' 와 '%' 문법을 사용해 쿼리를 작성하세요.
            7. 사용자 질문에서 지역을 명시적으로 '시군구' 라고 하지 않으고 지역에 대한 질문을 한다면 기본적으론 '시도' 정보를 제공하세요.(특정 지역끼리의 비교는 제외)
            8. 쿼리문에서 한글을 사용하는 경우 '``'(백틱)을 사용해서 쿼리문을 작성하세요.
            9. 지역은 경기, 서울, 강원, 부산 등..과 같이 뒤에 시도 및 시군구 단어를 제외해서 쿼리문을 작성하세요.
            10. 작성하는 쿼리문의 FROM에는 반드시 {catalog}.{schema}.`테이블명` 형식으로 작성하세요.
            """

            # 📌 테이블 정보 추가
            full_prompt = f"{base_prompt.strip()}\n\n=== 테이블 및 컬럼 정보 ===\n{formatted_table_info}"

            self.vn.set_initial_prompt(full_prompt)

            print("[INFO] 메타데이터 학습이 완료되었습니다.")

        except Exception as e:
            print(f"[ERROR] 메타데이터 학습 중 오류 발생: {e}")



    def ask_question(self, user_question: str, base_prompt: str = ""):
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

            if "documents" in all_data:
                for idx, doc in enumerate(all_data["documents"]):
                    try:
                        doc_data = json.loads(doc)
                        stored_question = doc_data.get("question", "")
                        stored_sql = doc_data.get("sql", "")

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

            return generated_sql, df_result

        except Exception as e:
            print(f"[ERROR] 질문 처리 중 오류 발생: {e}")
            return None, None


    def debug_chromadb(self):
        try:
            print("[INFO] ChromaDB 디버깅 시작")

            # SQL 컬렉션 상태 확인
            sql_data = self.vn.sql_collection.get()

            if "documents" in sql_data and sql_data["documents"]:
                for idx, doc in enumerate(sql_data["documents"]):
                    try:
                        parsed_doc = json.loads(doc)
                    except json.JSONDecodeError as e:
                        print(f"[WARN] Document {idx + 1} JSON 디코딩 실패: {e}")

            else:
                print("[WARN] SQL 컬렉션에 저장된 문서가 없습니다.")

            print("[INFO] ChromaDB 디버깅 완료")
        except Exception as e:
            print(f"[ERROR] ChromaDB 디버깅 중 오류 발생: {e}")
