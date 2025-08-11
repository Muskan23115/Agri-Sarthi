import os
import sqlite3
import uuid
from typing import List, Dict, Any

import chromadb
from chromadb.config import Settings  # type: ignore
from sentence_transformers import SentenceTransformer  # type: ignore

DB_PATH = os.getenv("DB_PATH", "knowledge.db")
CHROMA_PATH = os.getenv("CHROMA_PATH", "chroma_db")
COLLECTION_NAME = os.getenv("COLLECTION_NAME", "agri_sarthi_knowledge")
EMBEDDING_MODEL_NAME = os.getenv("EMBEDDING_MODEL", "sentence-transformers/all-MiniLM-L6-v2")


def get_all_table_names(connection: sqlite3.Connection) -> List[str]:
    cursor = connection.cursor()
    cursor.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name"
    )
    return [row[0] for row in cursor.fetchall()]


def fetch_table_rows(connection: sqlite3.Connection, table_name: str) -> List[Dict[str, Any]]:
    cursor = connection.cursor()
    cursor.execute(f"SELECT * FROM {table_name}")
    columns = [desc[0] for desc in cursor.description]
    rows = cursor.fetchall()
    return [dict(zip(columns, row)) for row in rows]


def format_document(table: str, row: Dict[str, Any]) -> str:
    # Create a descriptive, self-contained text document per row
    kv_pairs = "; ".join(f"{k}: {v}" for k, v in row.items() if v is not None and v != "")
    return f"Table: {table} | {kv_pairs}"


def main() -> None:
    if not os.path.exists(DB_PATH):
        raise FileNotFoundError(f"SQLite database not found at {DB_PATH}. Run etl.py first.")

    connection = sqlite3.connect(DB_PATH)
    try:
        tables = get_all_table_names(connection)
        documents: List[str] = []
        metadatas: List[Dict[str, Any]] = []
        ids: List[str] = []

        for table in tables:
            rows = fetch_table_rows(connection, table)
            for row in rows:
                doc = format_document(table, row)
                documents.append(doc)
                metadatas.append({"table": table})
                ids.append(str(uuid.uuid4()))

    finally:
        connection.close()

    if not documents:
        raise RuntimeError("No data found to index. Ensure ETL has populated knowledge.db.")

    model = SentenceTransformer(EMBEDDING_MODEL_NAME)
    embeddings = model.encode(documents, convert_to_numpy=True)

    client = chromadb.PersistentClient(path=CHROMA_PATH)
    collection = client.get_or_create_collection(COLLECTION_NAME)

    # Clear existing vectors for idempotency
    existing_count = collection.count()
    if existing_count:
        # Workaround: Chroma lacks clear-all in older versions; delete by where clause matches all
        collection.delete(where={})

    collection.upsert(ids=ids, documents=documents, metadatas=metadatas, embeddings=embeddings.tolist())

    print(
        f"Indexed {len(documents)} documents into Chroma collection '{COLLECTION_NAME}' at '{CHROMA_PATH}'."
    )


if __name__ == "__main__":
    main()
