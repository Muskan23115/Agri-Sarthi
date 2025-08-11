import io
import os
from typing import Optional

import httpx
import orjson
import typer
from rich import print_json

# Load .env if present
try:
    from dotenv import load_dotenv  # type: ignore
    load_dotenv()
except Exception:
    pass

# Default webhook URL from environment or fallback
DEFAULT_WEBHOOK_URL = os.getenv("WEBHOOK_URL", "http://127.0.0.1:8000/webhook")

app = typer.Typer(
    help="Agri-Sarthi CLI for testing the API, running ETL, etc.",
    pretty_exceptions_show_locals=False,
)


@app.command(help="Run the ETL process to build the main SQLite DB.")
def etl():
    """Runs the main ETL to build or rebuild the SQLite database."""
    try:
        from etl import run_etl  # type: ignore

        print("Running main ETL process...")
        run_etl()
        print("✅ ETL process completed successfully.")
    except Exception as e:
        print(f"❌ ETL process failed: {e}")


@app.command(help="Run the Vector DB build process.")
def build_db():
    """Builds the ChromaDB vector store from the SQLite DB."""
    try:
        from build_vector_db import run_vector_db_build  # type: ignore

        print("Building vector database...")
        run_vector_db_build()
        print("✅ Vector database build completed successfully.")
    except Exception as e:
        print(f"❌ Vector DB build failed: {e}")


@app.command(help="Send a test text message to the webhook.")
def text(
    message: str = typer.Option(..., "--message", "-m", help="Message text to send"),
    from_number: str = typer.Option(
        "+919999999999", "--from-number", "-f", help="Simulated sender phone number"
    ),
    location: str = typer.Option(
        "Jaipur, Rajasthan", "--location", "-l", help="Simulated location"
    ),
    url: str = typer.Option(
        DEFAULT_WEBHOOK_URL, "--url", "-u", help="Webhook URL to send the request to"
    ),
):
    """Sends a text query to the webhook."""
    payload = {
        "from_number": from_number,
        "message": message,
        "location": location,
    }
    try:
        # --- INCREASED TIMEOUT TO 5 MINUTES ---
        # This gives the local LLM plenty of time to process the first request.
        with httpx.Client(timeout=300.0) as client:
            response = client.post(url, json=payload)
        print(f"Status: {response.status_code}")
        print_json(data=response.json())
    except httpx.RequestError as e:
        print(f"Request failed: {e}")


@app.command(help="Send a test audio file to the webhook.")
def audio(
    file: str = typer.Option(..., "--file", "-F", help="Path to the audio file"),
    from_number: str = typer.Option(
        "+919999999999", "--from-number", "-f", help="Simulated sender phone number"
    ),
    location: Optional[str] = typer.Option(
        "Jaipur, Rajasthan", "--location", "-l", help="Simulated location"
    ),
    url: str = typer.Option(
        DEFAULT_WEBHOOK_URL, "--url", "-u", help="Webhook URL to send the request to"
    ),
):
    """Sends an audio file query to the webhook."""
    if not os.path.exists(file):
        print(f"File not found: {file}")
        raise typer.Exit(1)

    files = {"audio": (os.path.basename(file), open(file, "rb"))}
    data = {"from_number": from_number, "location": location}

    try:
        # --- INCREASED TIMEOUT TO 5 MINUTES ---
        with httpx.Client(timeout=300.0) as client:
            response = client.post(url, files=files, data=data)
        print(f"Status: {response.status_code}")
        print_json(data=response.json())
    except httpx.RequestError as e:
        print(f"Request failed: {e}")


if __name__ == "__main__":
    app()
