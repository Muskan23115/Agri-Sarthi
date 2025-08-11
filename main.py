import io
import os
from typing import Dict, Optional, List

import httpx
import orjson
from fastapi import FastAPI, UploadFile
from fastapi import Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel

# Load .env if present
try:
    from dotenv import load_dotenv  # type: ignore
    load_dotenv()
except Exception:
    pass

# Optional AI imports (load lazily)
try:
    import whisper  # openai-whisper
except Exception:  # pragma: no cover
    whisper = None

try:
    from ctransformers import AutoModelForCausalLM  # type: ignore
except Exception:  # pragma: no cover
    AutoModelForCausalLM = None  # type: ignore

# RAG stack
try:
    import chromadb  # type: ignore
    from sentence_transformers import SentenceTransformer  # type: ignore
except Exception:
    chromadb = None  # type: ignore
    SentenceTransformer = None  # type: ignore

WHATSAPP_API_URL = os.getenv("WHATSAPP_API_URL")
WHATSAPP_API_TOKEN = os.getenv("WHATSAPP_API_TOKEN")
WHATSAPP_SENDER_ID = os.getenv("WHATSAPP_SENDER_ID")

# --- Using the faster TinyLlama model ---
MODEL_FILENAME = "tinyllama-1.1b-chat-v1.0.Q4_K_M.gguf"
LLM_MODEL_PATH = os.path.abspath(os.path.join("models", MODEL_FILENAME))


CHROMA_PATH = os.getenv("CHROMA_PATH", "chroma_db")
COLLECTION_NAME = os.getenv("COLLECTION_NAME", "agri_sarthi_knowledge")
EMBEDDING_MODEL_NAME = os.getenv("EMBEDDING_MODEL", "sentence-transformers/all-MiniLM-L6-v2")

app = FastAPI(title="Agri-Sarthi MVP (RAG)")


class WebhookText(BaseModel):
    from_number: str
    message: str
    location: Optional[str] = "Jaipur, Rajasthan"


def load_llm():
    """Loads the LLM with detailed error logging."""
    if AutoModelForCausalLM is None:
        print("Warning: ctransformers library not found.")
        return None
    
    if not os.path.exists(LLM_MODEL_PATH):
        print(f"CRITICAL ERROR: Model file not found at path: {LLM_MODEL_PATH}")
        return None

    try:
        print(f"Attempting to load LLM from absolute path: {LLM_MODEL_PATH}")
        llm = AutoModelForCausalLM.from_pretrained(
            LLM_MODEL_PATH,
            model_type="llama",
            gpu_layers=0,
            context_length=2048,
            threads=int(os.getenv("LLM_THREADS", "4")),
        )
        print("LLM loaded successfully.")
        return llm
    except Exception as e:
        print(f"CRITICAL ERROR: LLM failed to load. Details: {e}")
        return None


def load_rag_components():
    if chromadb is None or SentenceTransformer is None:
        return None, None, None
    try:
        client = chromadb.PersistentClient(path=CHROMA_PATH)
        collection = client.get_collection(COLLECTION_NAME)
    except Exception:
        client = chromadb.PersistentClient(path=CHROMA_PATH)
        collection = None
    try:
        embedder = SentenceTransformer(EMBEDDING_MODEL_NAME)
    except Exception:
        embedder = None
    return client, collection, embedder


LLM = load_llm()
CHROMA_CLIENT, CHROMA_COLLECTION, EMBEDDER = load_rag_components()


async def transcribe_audio(file_bytes: bytes) -> str:
    if whisper is None:
        return ""
    try:
        model = whisper.load_model("small")
        tmp_path = "_tmp_audio.ogg"
        with open(tmp_path, "wb") as wf:
            wf.write(file_bytes)
        result = model.transcribe(tmp_path, language="hi")
        try:
            os.remove(tmp_path)
        except Exception:
            pass
        return result.get("text", "").strip()
    except Exception:
        return ""


def generate_response(user_query: str) -> str:
    if CHROMA_COLLECTION is None or EMBEDDER is None:
        return (
            "Vector database not initialized. Please run: `python build_vector_db.py` "
            "to build the Chroma collection before asking questions."
        )

    try:
        query_embedding = EMBEDDER.encode([user_query], convert_to_numpy=True)[0].tolist()
        results = CHROMA_COLLECTION.query(query_embeddings=[query_embedding], n_results=3)
        docs = results.get("documents", [[]])[0]
    except Exception as e:
        docs = []

    retrieved_context = "\n\n".join(docs) if docs else "(No relevant context found.)"

    # --- FINAL ATTEMPT: Simplest possible prompt format ---
    prompt = f"""Instruction: You are a helpful farm advisor. Answer the user's query in simple Hindi based only on the provided context.

Context:
{retrieved_context}

User Query:
{user_query}

Answer (in Hindi):
"""

    if LLM is None:
        return (
            "LLM not available. Please check server logs for errors."
        )

    try:
        print("\n--- PROMPT SENT TO LLM ---")
        print(prompt)
        print("--- END OF PROMPT ---\n")
        
        output = LLM(prompt, max_new_tokens=256, temperature=0.7, top_p=0.9)

        print("\n--- RAW OUTPUT FROM LLM ---")
        print(repr(output))
        print("--- END OF RAW OUTPUT ---\n")

        return str(output).strip()
    except Exception as e:
        print(f"Error during LLM generation: {e}")
        return "Unable to generate response at the moment. Please try again later."


async def send_whatsapp_text(to_number: str, text: str) -> None:
    if not WHATSAPP_API_URL or not WHATSAPP_SENDER_ID:
        return
    headers = {"Authorization": f"Bearer {WHATSAPP_API_TOKEN}"} if WHATSAPP_API_TOKEN else {}
    payload = {
        "from": WHATSAPP_SENDER_ID,
        "to": to_number,
        "message": text,
    }
    try:
        async with httpx.AsyncClient(timeout=10) as client:
            await client.post(WHATSAPP_API_URL, json=payload, headers=headers)
    except Exception:
        pass


@app.post("/webhook")
async def webhook(request: Request):
    content_type = request.headers.get("content-type", "")

    if content_type.startswith("application/json"):
        body = await request.body()
        data = orjson.loads(body)
        payload = WebhookText(**data)
        user_query = payload.message.strip()
        from_number = payload.from_number

        answer = generate_response(user_query)
        return JSONResponse({"ok": True, "answer": answer})

    if content_type.startswith("multipart/form-data"):
        form = await request.form()
        from_number = str(form.get("from_number", ""))
        file: UploadFile = form.get("audio")  # type: ignore
        transcript = ""
        if file is not None:
            audio_bytes = await file.read()
            transcript = await transcribe_audio(audio_bytes)
        user_query = transcript or ""

        answer = generate_response(user_query)
        return JSONResponse({"ok": True, "answer": answer, "transcript": transcript})

    return JSONResponse({"ok": False, "error": "Unsupported content-type"}, status_code=400)
