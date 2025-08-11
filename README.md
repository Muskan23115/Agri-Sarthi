# Agri-Sarthi (MVP)

WhatsApp-based agricultural advisory bot for Jaipur farmers (Wheat, Mustard). Fully FOSS. Backend: FastAPI + SQLite + Whisper + ctransformers (Mistral GGUF).

## Features
- Text and voice queries (Hindi)
- Rule-based orchestrator for Wheat/Mustard
- Static knowledge in SQLite (`knowledge.db`) via ETL
- Weather from Open-Meteo (free)
- Market price: Agmarknet scrape with fallback
- Local LLM (Mistral 7B Instruct GGUF) via `ctransformers`

## Requirements
- Python 3.10+
- Windows PowerShell (tested on Windows 10+)

## Setup (Windows PowerShell)
```powershell
# 1) Create venv
python -m venv .venv
.\.venv\Scripts\Activate.ps1

# 2) Install deps
pip install --upgrade pip
pip install -r requirements.txt

# 3) Build knowledge DB
python etl.py

# 4) Run API
uvicorn main:app --reload
```

## Environment variables (.env)
Copy `.env.example` to `.env` and adjust if needed.

- `DB_PATH` (default `knowledge.db`)
- `LLM_MODEL_PATH` (default `models/mistral-7b-instruct.Q4_K_M.gguf`)
- `LLM_THREADS` (default `4`)
- `WHATSAPP_API_URL` (optional, for sending replies)
- `WHATSAPP_API_TOKEN` (optional)
- `WHATSAPP_SENDER_ID` (optional)
- `WEBHOOK_URL` (CLI default webhook)

## Download a GGUF model
Use an openly licensed Mistral 7B Instruct GGUF, e.g. `TheBloke/Mistral-7B-Instruct-GGUF` Q4_K_M. Place it at `models/mistral-7b-instruct.Q4_K_M.gguf` or set `LLM_MODEL_PATH`.

## CLI usage
```powershell
# Ensure API is running: uvicorn main:app --reload

# Run ETL
python cli.py etl

# Send text
python cli.py text --message "सरसों के लिए सिंचाई?" --from-number "+911234567890" --location "Jaipur, Rajasthan" --url http://127.0.0.1:8000/webhook

# Send audio (OGG/MP3 file)
python cli.py audio --file sample.ogg --from-number "+911234567890" --location "Jaipur, Rajasthan" --url http://127.0.0.1:8000/webhook
```

## PowerShell one-liners (optional)
- Text JSON:
```powershell
$body = @{ from_number = "+911234567890"; message = "गेहूं के लिए सलाह?"; location = "Jaipur, Rajasthan" } | ConvertTo-Json -Depth 5
Invoke-RestMethod -Uri "http://127.0.0.1:8000/webhook" -Method Post -ContentType "application/json" -Body $body
```

- Multipart audio:
```powershell
Invoke-RestMethod -Uri "http://127.0.0.1:8000/webhook" -Method Post -Form @{ 
  from_number = "+911234567890"; 
  location = "Jaipur, Rajasthan"; 
  audio = Get-Item ".\sample.ogg"
}
```

## Notes
- Whisper auto-downloads models on first use. For faster transcribe, consider `small` or `base`.
- If `ctransformers` model is not available, a Hindi fallback reply is returned.
- This MVP focuses on Jaipur and Wheat/Mustard only.
