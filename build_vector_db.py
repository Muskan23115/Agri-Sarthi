import os
import sqlite3
from typing import List, Dict, Any

import pandas as pd

# Load .env if present
try:
    from dotenv import load_dotenv  # type: ignore
    load_dotenv()
except Exception:
    pass

# RAG stack
try:
    import chromadb  # type: ignore
    from sentence_transformers import SentenceTransformer  # type: ignore
except Exception:
    chromadb = None  # type: ignore
    SentenceTransformer = None  # type: ignore

DB_PATH = os.getenv("DB_PATH", "knowledge.db")
CHROMA_PATH = os.getenv("CHROMA_PATH", "chroma_db")
COLLECTION_NAME = os.getenv("COLLECTION_NAME", "agri_sarthi_knowledge")
EMBEDDING_MODEL_NAME = os.getenv("EMBEDDING_MODEL", "sentence-transformers/all-MiniLM-L6-v2")

def load_data_from_sqlite() -> List[Dict[str, Any]]:
    """Loads all data from all tables in the SQLite DB."""
    conn = sqlite3.connect(DB_PATH)
    tables = ["crop_info", "pest_info", "govt_schemes"]
    all_docs = []
    for table in tables:
        try:
            df = pd.read_sql_query(f"SELECT * from {table}", conn)
            # Format each row into a descriptive document
            for _, row in df.iterrows():
                doc_text = f"Table: {table} | " + " | ".join(
                    [f"{col}: {val}" for col, val in row.items()]
                )
                all_docs.append(
                    {
                        "document": doc_text,
                        "metadata": {"source": table, "id": row.iloc[0]},
                    }
                )
        except Exception as e:
            print(f"Could not read table {table}: {e}")
    conn.close()
    return all_docs

def run_vector_db_build():
    """Builds or rebuilds the ChromaDB vector store."""
    if chromadb is None or SentenceTransformer is None:
        print("Required libraries (chromadb, sentence-transformers) not found.")
        return

    docs_with_metadata = load_data_from_sqlite()
    if not docs_with_metadata:
        print("No data loaded from SQLite. Aborting vector DB build.")
        return

    print(f"Loaded {len(docs_with_metadata)} documents from SQLite.")

    # Initialize ChromaDB client and Sentence Transformer model
    client = chromadb.PersistentClient(path=CHROMA_PATH)
    embedder = SentenceTransformer(EMBEDDING_MODEL_NAME)

    # --- CORRECTED LOGIC ---
    # Delete the collection if it exists, then create it again to ensure a clean slate.
    # This is the new, recommended way to clear a collection.
    try:
        print(f"Deleting existing collection: {COLLECTION_NAME}...")
        client.delete_collection(name=COLLECTION_NAME)
        print("Collection deleted.")
    except Exception as e:
        print(f"Collection doesn't exist or could not be deleted, creating new one. Details: {e}")

    print(f"Creating new collection: {COLLECTION_NAME}...")
    collection = client.create_collection(name=COLLECTION_NAME)
    print("Collection created.")

    # Prepare data for ChromaDB
    documents = [d["document"] for d in docs_with_metadata]
    metadatas = [d["metadata"] for d in docs_with_metadata]
    ids = [f"{d['metadata']['source']}_{i}" for i, d in enumerate(docs_with_metadata)]

    print("Generating embeddings (this may take a moment)...")
    embeddings = embedder.encode(documents, show_progress_bar=True).tolist()
    print("Embeddings generated.")

    # Add data to the collection in batches
    batch_size = 100
    for i in range(0, len(documents), batch_size):
        print(f"Adding batch {i//batch_size + 1}...")
        collection.add(
            embeddings=embeddings[i : i + batch_size],
            documents=documents[i : i + batch_size],
            metadatas=metadatas[i : i + batch_size],
            ids=ids[i : i + batch_size],
        )

    print(f"✅ Vector database build complete. {len(documents)} documents added.")

if __name__ == "__main__":
    run_vector_db_build()
