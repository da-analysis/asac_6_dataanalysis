import streamlit as st
from langchain.vectorstores import Chroma
from langchain.embeddings.openai import OpenAIEmbeddings
from langchain_openai import ChatOpenAI
from langchain.chains import RetrievalQA
from langchain.prompts import PromptTemplate
import json
import re
import os
import pickle

config = {
    "api_key": st.secrets["api"]["api_key"],
    "model": "gpt-4o-mini"
}

class RAGTextBase:
    def __init__(self, config):
        """RAG 텍스트 모델의 기본 클래스"""
        self.config = config
        self.embeddings = OpenAIEmbeddings(api_key=config["api_key"])
        self.llm = ChatOpenAI(model=config["model"], api_key=config["api_key"])

        # ChromaDB 경로 설정
        vector_store_path = "vector_store"
        vector_store_name = "text_chatbot_chromadb"
        self.chroma_db_path = os.path.join(vector_store_path, vector_store_name)

        # ChromaDB 로드
        self.vectorstore = Chroma(
            persist_directory=self.chroma_db_path,
            embedding_function=self.embeddings,
            collection_name="text_db_0"
        )

        # 메타데이터 캐시 설정
        self.cache_file = os.path.join(vector_store_path, "metadata_cache.pkl")

    def save_metadata_cache(self, metadata):
        """메타데이터 캐시 저장"""
        try:
            os.makedirs(os.path.dirname(self.cache_file), exist_ok=True)
            with open(self.cache_file, "wb") as f:
                pickle.dump(metadata, f)
        except Exception as e:
            print(f"[ERROR] 메타데이터 캐시 저장 중 오류 발생: {e}")

    def load_metadata_cache(self):
        """메타데이터 캐시 로드"""
        try:
            if os.path.exists(self.cache_file):
                with open(self.cache_file, "rb") as f:
                    return pickle.load(f)
        except Exception as e:
            print(f"[ERROR] 캐시 파일 로드 중 오류 발생: {e}")
        return None

    def extract_metadata(self):
        """메타데이터 추출"""
        try:
            cached_metadata = self.load_metadata_cache()
            if cached_metadata is None:
                metadata_response = self.vectorstore.get()
                all_metadata = metadata_response.get("metadatas", []) if metadata_response else []
                self.save_metadata_cache(all_metadata)
            else:
                all_metadata = cached_metadata

            unique_sido = list(set([doc["시도"] for doc in all_metadata if "시도" in doc]))
            unique_sigungu = list(set([doc["시군구"] for doc in all_metadata if "시군구" in doc]))
            unique_local_currency = list(set([doc["지역화폐명"] for doc in all_metadata if "지역화폐명" in doc]))

            return unique_sido, unique_sigungu, unique_local_currency
        except Exception as e:
            print(f"[ERROR] 메타데이터 추출 중 오류 발생: {e}")
            return [], [], []

    def perform_qa(self, query, chroma_filter):
        """ChromaDB 기반 검색 후, LLM을 이용한 QA 실행"""
        try:
            search_kwargs = {"k": 10}
            if chroma_filter:
                search_kwargs["filter"] = chroma_filter

            qa_chain = RetrievalQA.from_chain_type(
                llm=self.llm,
                retriever=self.vectorstore.as_retriever(search_kwargs=search_kwargs),
            )

            response = qa_chain.invoke(query)
            return response.get("result")
        except Exception as e:
            print(f"[ERROR] LLM 질의 응답 중 오류 발생: {e}")
            return {"result": "죄송합니다. 현재 답변을 생성할 수 없습니다."}

class RAGTextManager:
    def __init__(self, config):
        """RAG 텍스트 모델 매니저"""
        self.config = config
        self.model = RAGTextBase(config)

    def generate_filter_prompt(self, query: str, sido_list: list, sigungu_list: list, currency_list: list) -> str:
        """필터 프롬프트 생성"""
        try:
            region_empty = not sido_list and not sigungu_list
            additional_instruction = "\n- 특정 지역(광역시/도, 시군구)에 국한된 설명을 피하세요." if region_empty else ""

            query_expansion_prompt = PromptTemplate(
                input_variables=["query", "sido_list", "sigungu_list", "currency_list", "additional_instruction"],
                template="""사용자가 입력한 검색 질문 '{query}'을(를) 분석하여 가능한 필터를 JSON 형식으로 반환하세요.

                ✅ 현재 데이터에 존재하는 값:
                - 가능한 시도 값: {sido_list}
                - 가능한 시군구 값: {sigungu_list}
                - 가능한 지역화폐명 값: {currency_list}

                💡 필터링 규칙:
                - 사용자가 특정 **시도(광역시/도)**를 여러 개 언급했다면 모두 포함하세요.
                - 사용자가 특정 **시군구**를 여러 개 언급했다면 모두 포함하세요.
                - 사용자가 특정 **지역화폐명**을 여러 개 언급했다면 모두 포함하세요.
                - JSON 형식으로만 출력하고, 설명은 포함하지 마세요.
                {additional_instruction}

                🔥 예제 출력:
                ```json
                {{"시도": ["부산", "서울"], "시군구": ["부산진구", "강남구"], "지역화폐명": ["동백전", "서울페이"]}}
                ```
                또는, 일부만 존재할 경우:
                ```json
                {{"시도": ["부산"], "지역화폐명": ["동백전"]}}
                ```
                하나의 요소만 감지됐다면:
                ```json
                {{"시도": "부산"}}
                ```
                """
            )

            return self.model.llm.invoke(query_expansion_prompt.format(
                query=query,
                sido_list=", ".join(sido_list),
                sigungu_list=", ".join(sigungu_list),
                currency_list=", ".join(currency_list),
                additional_instruction=additional_instruction
            )).content
        except Exception as e:
            print(f"[ERROR] 필터 프롬프트 생성 중 오류 발생: {e}")
            return "{}"

    @staticmethod
    def extract_json(llm_response: str) -> dict:
        """LLM 응답에서 JSON 부분만 추출"""
        try:
            json_match = re.search(r'```json\n(.*?)\n```', llm_response, re.DOTALL)
            json_text = json_match.group(1) if json_match else llm_response

            return json.loads(json_text)
        except json.JSONDecodeError as e:
            print(f"[ERROR] JSON 파싱 오류: {e}")
            return {}

    @staticmethod
    def transform_filters_to_chroma(filters):
        """LLM이 생성한 필터를 ChromaDB의 $in, $and 형식으로 변환"""
        try:
            if not filters:
                return None

            chroma_filters = [{key: {"$in": value} if isinstance(value, list) else {"$eq": value}} for key, value in filters.items()]
            return {"$and": chroma_filters} if len(chroma_filters) > 1 else chroma_filters[0]
        except Exception as e:
            print(f"[ERROR] 필터 변환 중 오류 발생: {e}")
            return None
