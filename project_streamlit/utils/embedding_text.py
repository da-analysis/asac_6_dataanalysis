import os
import pandas as pd
from langchain.document_loaders import PyMuPDFLoader, CSVLoader
from langchain.schema import Document
from langchain.text_splitter import RecursiveCharacterTextSplitter
from langchain.vectorstores import Chroma
from langchain_openai import OpenAIEmbeddings

# ✅ OpenAI API 키 설정 (환경 변수로 등록)
os.environ["OPENAI_API_KEY"] = ""

# ✅ 경로 설정
current_dir = os.path.dirname(os.path.abspath(__file__))  # `utils` 폴더의 절대 경로
base_dir = os.path.dirname(current_dir)  # `utils`의 상위 디렉토리

# ✅ 변경된 경로
pdf_directory = os.path.join(base_dir, "이용가이드")
csv_file_path = os.path.join(base_dir, "보조금24_혜택", "정부24_보조금24_지역화폐_크롤링_전처리.csv")
vector_store_path = os.path.join(base_dir, "vector_store", "text_chatbot_chromadb")

# ✅ OpenAI 임베딩 모델
embeddings = OpenAIEmbeddings()

# ✅ 1️⃣ PDF 로드 및 메타데이터 추가
def load_pdf_documents(pdf_directory):
    documents = []

    if not os.path.exists(pdf_directory):
        print(f"🚨 PDF 디렉토리가 존재하지 않습니다: {pdf_directory}")
        return documents

    for filename in os.listdir(pdf_directory):
        if filename.endswith(".pdf"):
            file_path = os.path.join(pdf_directory, filename)
            loader = PyMuPDFLoader(file_path)
            doc_list = loader.load()

            # 📌 파일명에서 지역명, 지역화폐명 추출
            parts = filename.replace(".pdf", "").split("_")
            sido = parts[0] if len(parts) > 0 else "정보 없음"
            sigungu = parts[1] if len(parts) > 1 else "정보 없음"
            currency_name = parts[2] if len(parts) > 2 else "정보 없음"

            # 📌 PDF 파일별 자동 메타데이터 추가
            for doc in doc_list:
                doc.metadata.update({
                    "시도": sido,
                    "시군구": sigungu,
                    "지역화폐명": currency_name,
                    "파일명": filename,
                    "문서유형": "database"
                })
            documents.extend(doc_list)

    print(f"✅ PDF 문서 로드 완료: {len(documents)} 개")
    return documents

# ✅ 2️⃣ CSV 파일 로드 및 메타데이터 변환
def load_csv_documents(csv_file_path):
    if not os.path.exists(csv_file_path):
        print(f"🚨 CSV 파일이 존재하지 않습니다: {csv_file_path}")
        return []

    df = pd.read_csv(csv_file_path, encoding="cp949")

    # ✅ 첫 3개 컬럼은 메타데이터, 나머지는 본문
    meta_columns = df.iloc[:, :3].columns.tolist()
    content_columns = df.iloc[:, 3:].columns.tolist()

    csv_documents = []
    for _, row in df.iterrows():
        metadata = {
            "시도": row[meta_columns[0]],
            "시군구": row[meta_columns[1]],
            "지역화폐명": row[meta_columns[2]],
            "문서유형": "database"
        }

        # ✅ 본문 내용 결합
        text_content = "\n".join([f"{col}: {row[col]}" for col in content_columns])

        # ✅ CSV 데이터를 `Document` 객체로 변환
        csv_documents.append(Document(page_content=text_content, metadata=metadata))

    print(f"✅ CSV 문서 로드 완료: {len(csv_documents)} 개")
    return csv_documents

# ✅ 3️⃣ 문서 분할
def split_documents(documents):
    text_splitter = RecursiveCharacterTextSplitter(chunk_size=700, chunk_overlap=150)
    split_docs = text_splitter.split_documents(documents)

    print(f"✅ 문서 조각 생성 완료: {len(split_docs)} 개")

    # ✅ 메타데이터 필드 제한
    allowed_metadata_keys = {"시도", "시군구", "지역화폐명", "파일명", "문서유형"}

    cleaned_docs = []
    for doc in split_docs:
        new_metadata = {key: doc.metadata.get(key, "정보 없음") for key in allowed_metadata_keys}
        cleaned_docs.append(Document(page_content=doc.page_content, metadata=new_metadata))

    print(f"✅ 메타데이터 정리 완료: {len(cleaned_docs)} 개")
    return cleaned_docs

# ✅ 4️⃣ ChromaDB에 저장
def save_to_chroma(split_docs, persist_directory):
    os.makedirs(persist_directory, exist_ok=True)

    vectorstore = Chroma.from_documents(
        documents=split_docs,
        embedding=embeddings,
        persist_directory=persist_directory,
        collection_name='text_db_0'
    )

    vectorstore.persist()
    print(f"✅ ChromaDB 저장 완료: {persist_directory}")

# ✅ 실행 파이프라인 (PDF + CSV 통합 저장)
print("📌 데이터 로딩 시작...")

pdf_documents = load_pdf_documents(pdf_directory)
csv_documents = load_csv_documents(csv_file_path)

# ✅ 문서 통합
all_documents = pdf_documents + csv_documents

if len(all_documents) == 0:
    print("🚨 로드된 문서가 없습니다. 프로세스를 종료합니다.")
else:
    # ✅ 문서 분할
    split_docs = split_documents(all_documents)

    # ✅ ChromaDB 저장
    save_to_chroma(split_docs, persist_directory=vector_store_path)

print("🎉 모든 작업이 완료되었습니다.")
