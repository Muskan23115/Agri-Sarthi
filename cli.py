import argparse
import json
import os
import sys
from typing import Optional

import requests

WEBHOOK_URL_DEFAULT = os.getenv("WEBHOOK_URL", "http://127.0.0.1:8000/webhook")


def cmd_etl(_: argparse.Namespace) -> int:
    # Run ETL by invoking etl.run_etl
    try:
        from etl import run_etl, DB_PATH  # type: ignore
    except Exception as exc:  # pragma: no cover
        print(f"Failed to import ETL: {exc}")
        return 1
    try:
        n_crops, n_soils = run_etl()
        print(f"Loaded {n_crops} crop rows and {n_soils} soil rows into {DB_PATH}")
        return 0
    except Exception as exc:
        print(f"ETL failed: {exc}")
        return 1


def cmd_text(args: argparse.Namespace) -> int:
    payload = {
        "from_number": args.from_number,
        "message": args.message,
        "location": args.location or "Jaipur, Rajasthan",
    }
    try:
        r = requests.post(args.url, json=payload, timeout=20)
        print(f"Status: {r.status_code}")
        try:
            print(json.dumps(r.json(), ensure_ascii=False, indent=2))
        except Exception:
            print(r.text)
        return 0 if r.ok else 1
    except Exception as exc:
        print(f"Request failed: {exc}")
        return 1


def cmd_audio(args: argparse.Namespace) -> int:
    if not os.path.isfile(args.file):
        print(f"File not found: {args.file}")
        return 1
    files = {
        "audio": (os.path.basename(args.file), open(args.file, "rb"), "application/octet-stream"),
    }
    data = {
        "from_number": args.from_number,
        "location": args.location or "Jaipur, Rajasthan",
    }
    try:
        r = requests.post(args.url, files=files, data=data, timeout=60)
        print(f"Status: {r.status_code}")
        try:
            print(json.dumps(r.json(), ensure_ascii=False, indent=2))
        except Exception:
            print(r.text)
        return 0 if r.ok else 1
    except Exception as exc:
        print(f"Request failed: {exc}")
        return 1


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Agri-Sarthi CLI")
    sub = p.add_subparsers(dest="cmd", required=True)

    p_etl = sub.add_parser("etl", help="Run ETL to build the SQLite knowledge DB")
    p_etl.set_defaults(func=cmd_etl)

    p_text = sub.add_parser("text", help="Send a text message to the webhook")
    p_text.add_argument("--message", required=True, help="User message in Hindi or English")
    p_text.add_argument("--from-number", default="+910000000000", help="WhatsApp sender number")
    p_text.add_argument("--location", default="Jaipur, Rajasthan", help="User location")
    p_text.add_argument("--url", default=WEBHOOK_URL_DEFAULT, help="Webhook URL")
    p_text.set_defaults(func=cmd_text)

    p_audio = sub.add_parser("audio", help="Send an audio file to the webhook")
    p_audio.add_argument("--file", required=True, help="Path to audio file (e.g., OGG/MP3)")
    p_audio.add_argument("--from-number", default="+910000000000", help="WhatsApp sender number")
    p_audio.add_argument("--location", default="Jaipur, Rajasthan", help="User location")
    p_audio.add_argument("--url", default=WEBHOOK_URL_DEFAULT, help="Webhook URL")
    p_audio.set_defaults(func=cmd_audio)

    return p


def main(argv: Optional[list] = None) -> int:
    argv = argv if argv is not None else sys.argv[1:]
    parser = build_parser()
    args = parser.parse_args(argv)
    return int(args.func(args))


if __name__ == "__main__":
    raise SystemExit(main())
