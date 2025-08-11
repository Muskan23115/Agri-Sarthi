import base64
import io
import os
from typing import Dict, Optional

import httpx
import orjson
from fastapi import FastAPI, File, UploadFile, Form
from fastapi import Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel

from agents import get_crop_advice, get_weather, get_market_price, get_pest_advice

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

WHATSAPP_API_URL = os.getenv("WHATSAPP_API_URL")  # e.g., local gateway or self-hosted stack
WHATSAPP_API_TOKEN = os.getenv("WHATSAPP_API_TOKEN")
WHATSAPP_SENDER_ID = os.getenv("WHATSAPP_SENDER_ID")
LLM_MODEL_PATH = os.getenv("LLM_MODEL_PATH", "models/mistral-7b-instruct.Q4_K_M.gguf")

app = FastAPI(title="Agri-Sarthi MVP")


class WebhookText(BaseModel):
    from_number: str
    message: str
    location: Optional[str] = "Jaipur, Rajasthan"


async def transcribe_audio(file_bytes: bytes) -> str:
    if whisper is None:
        return ""
    try:
        # Use small model for MVP speed; will auto-download on first use
        model = whisper.load_model("small")
        with io.BytesIO(file_bytes) as f:
            audio = f.read()
        # whisper expects file path or numpy array; using temporary buffer is okay via load_audio
        # Simpler approach: write temp file
        tmp_path = "_tmp_audio.ogg"
        with open(tmp_path, "wb") as wf:
            wf.write(audio)
        result = model.transcribe(tmp_path, language="hi")
        try:
            os.remove(tmp_path)
        except Exception:
            pass
        return result.get("text", "").strip()
    except Exception:
        return ""


def run_llm_hindi(context: str, user_query: str) -> str:
    if AutoModelForCausalLM is None:
        # Fallback template response
        return (
            "कृपया ध्यान दें: ऑफ़लाइन LLM उपलब्ध नहीं है। आपके प्रश्न का संक्षिप्त उत्तर: "
            + user_query
            + " | संदर्भ: "
            + context[:400]
        )

    try:
        llm = AutoModelForCausalLM.from_pretrained(
            LLM_MODEL_PATH,
            model_type="mistral",
            gpu_layers=0,
            context_length=2048,
            threads=int(os.getenv("LLM_THREADS", "4")),
        )
        prompt = (
            "You are Agri-Sarthi, a helpful agricultural advisor for Jaipur farmers. "
            "Respond in simple, natural Hindi. Keep it concise and factual.\n\n"
            f"Context:\n{context}\n\n"
            f"User question:\n{user_query}\n\n"
            "उत्तर हिंदी में दें।"
        )
        output = llm(prompt, max_new_tokens=256, temperature=0.4, top_p=0.9)
        return str(output).strip()
    except Exception:
        return (
            "कृपया ध्यान दें: LLM से उत्तर जनरेट नहीं हो सका। संक्षेप: "
            + user_query
        )


def build_context(crop: Optional[str], location: str, include_pest_advice: bool = False) -> Dict:
    data: Dict[str, Optional[Dict]] = {
        "crop_info": None,
        "weather": None,
        "market_price": None,
    }
    if crop:
        data["crop_info"] = get_crop_advice(crop, location)
        data["market_price"] = get_market_price(crop)
        if include_pest_advice:
            # store as list of dicts
            data["pest_advice"] = get_pest_advice(crop)
    else:
        if include_pest_advice:
            data["pest_advice"] = []
    data["weather"] = get_weather(location)
    return data


def format_context_string(data: Dict, user_query: str, crop: Optional[str], location: str) -> str:
    parts = [
        f"User: {user_query}",
        f"Location: {location}",
    ]
    if crop:
        parts.append(f"Crop: {crop}")

    ci = data.get("crop_info") or {}
    if ci:
        parts.append(
            "Crop Info: "
            + ", ".join(
                f"{k}={v}" for k, v in ci.items() if v is not None and v != ""
            )
        )

    wx = data.get("weather") or {}
    if wx:
        parts.append(
            "Weather: "
            + ", ".join(
                f"{k}={v}" for k, v in wx.items() if v is not None and v != ""
            )
        )

    mp = data.get("market_price") or {}
    if mp:
        parts.append(
            "Market Price: "
            + ", ".join(
                f"{k}={v}" for k, v in mp.items() if v is not None and v != ""
            )
        )

    pest_list = data.get("pest_advice") or []
    if pest_list:
        # Compact representation of pests
        pest_strs = []
        for p in pest_list:
            n = p.get("pest_name")
            s = p.get("symptoms")
            m = p.get("management_advice")
            pest_strs.append(f"{n}: symptoms={s}; management={m}")
        parts.append("Pest Advice: " + " || ".join(pest_strs))

    return " | ".join(parts)


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


PEST_KEYWORDS = ["pest", "disease", "कीड़ा", "रोग", "बीमारी"]


@app.post("/webhook")
async def webhook(request: Request):
    """
    WhatsApp webhook. Accepts JSON for text messages and multipart for audio.

    - For text JSON:
      {
        "from_number": "+91...",
        "message": "सरसों के लिए सिंचाई?",
        "location": "Jaipur, Rajasthan" (optional)
      }

    - For audio multipart:
      fields: from_number, location (optional), and file under key 'audio'
    """
    content_type = request.headers.get("content-type", "")

    if content_type.startswith("application/json"):
        body = await request.body()
        data = orjson.loads(body)
        payload = WebhookText(**data)
        user_query = payload.message.strip()
        location = payload.location or "Jaipur, Rajasthan"
        from_number = payload.from_number
        # Keyword-based orchestrator
        crop = None
        uq_lower = user_query.lower()
        if any(k in uq_lower for k in ["wheat", "गेहूं", "gehun", "gehu"]):
            crop = "Wheat"
        elif any(k in uq_lower for k in ["mustard", "सरसों", "sarson"]):
            crop = "Mustard"

        include_pests = any(k in uq_lower for k in PEST_KEYWORDS)

        data = build_context(crop, location, include_pest_advice=include_pests)
        ctx = format_context_string(data, user_query, crop, location)
        answer = run_llm_hindi(ctx, user_query)
        await send_whatsapp_text(from_number, answer)
        return JSONResponse({"ok": True, "answer": answer})

    if content_type.startswith("multipart/form-data"):
        form = await request.form()
        from_number = str(form.get("from_number", ""))
        location = str(form.get("location", "Jaipur, Rajasthan"))
        file: UploadFile = form.get("audio")  # type: ignore
        transcript = ""
        if file is not None:
            audio_bytes = await file.read()
            transcript = await transcribe_audio(audio_bytes)
        user_query = transcript or ""

        uq_lower = user_query.lower()
        crop = None
        if any(k in uq_lower for k in ["wheat", "गेहूं", "gehun", "gehu"]):
            crop = "Wheat"
        elif any(k in uq_lower for k in ["mustard", "सरसों", "sarson"]):
            crop = "Mustard"

        include_pests = any(k in uq_lower for k in PEST_KEYWORDS)

        data = build_context(crop, location, include_pest_advice=include_pests)
        ctx = format_context_string(data, user_query, crop, location)
        answer = run_llm_hindi(ctx, user_query)
        await send_whatsapp_text(from_number, answer)
        return JSONResponse({"ok": True, "answer": answer, "transcript": transcript})

    return JSONResponse({"ok": False, "error": "Unsupported content-type"}, status_code=400)
