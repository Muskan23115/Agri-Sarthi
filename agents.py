import os
import sqlite3
from typing import Dict, Optional, List

import requests
from bs4 import BeautifulSoup

DB_PATH = os.getenv("DB_PATH", "knowledge.db")


def _connect_db() -> sqlite3.Connection:
    return sqlite3.connect(DB_PATH)


def get_crop_advice(crop: str, location: str) -> Dict:
    """Query SQLite for crop info.

    Args:
        crop: Crop name (e.g., "Wheat", "Mustard"). Case-insensitive.
        location: Location string (e.g., "Jaipur, Rajasthan"). Case-insensitive contains match.
    Returns:
        Dict with crop info or empty dict.
    """
    connection = _connect_db()
    try:
        cursor = connection.cursor()
        cursor.execute(
            """
            SELECT crop, location, season, sowing_period, harvesting_period,
                   irrigation_schedule, fertilizer, pests
            FROM crop_info
            WHERE LOWER(crop) = LOWER(?) AND LOWER(location) LIKE LOWER(?)
            LIMIT 1
            """,
            (crop, f"%{location}%"),
        )
        row = cursor.fetchone()
        if not row:
            return {}
        keys = [
            "crop",
            "location",
            "season",
            "sowing_period",
            "harvesting_period",
            "irrigation_schedule",
            "fertilizer",
            "pests",
        ]
        return dict(zip(keys, row))
    finally:
        connection.close()


def get_weather(location: str) -> Dict:
    """Fetch current weather for Jaipur using Open-Meteo.

    For MVP, we map 'Jaipur' to its lat/lon. No API key needed.
    """
    # Jaipur coordinates
    lat, lon = 26.9124, 75.7873

    try:
        resp = requests.get(
            "https://api.open-meteo.com/v1/forecast",
            params={
                "latitude": lat,
                "longitude": lon,
                "current_weather": True,
                "hourly": "temperature_2m,relative_humidity_2m,precipitation",
                "timezone": "Asia/Kolkata",
            },
            timeout=10,
        )
        resp.raise_for_status()
        data = resp.json()
        current = data.get("current_weather", {})
        hourly = data.get("hourly", {})
        return {
            "location": location,
            "temperature_c": current.get("temperature"),
            "windspeed_kmh": current.get("windspeed"),
            "weathercode": current.get("weathercode"),
            "humidity": (hourly.get("relative_humidity_2m") or [None])[-1],
            "precipitation_mm": (hourly.get("precipitation") or [None])[-1],
        }
    except Exception:
        return {
            "location": location,
            "temperature_c": None,
            "windspeed_kmh": None,
            "weathercode": None,
            "humidity": None,
            "precipitation_mm": None,
        }


def _parse_agmarknet_price(html: str, crop: str) -> Optional[Dict]:
    soup = BeautifulSoup(html, "lxml")
    # MVP heuristic parsing; real portal often uses forms and JS. We'll parse any table present.
    tables = soup.find_all("table")
    for table in tables:
        headers = [th.get_text(strip=True).lower() for th in table.find_all("th")]
        if not headers:
            continue
        if any("variety" in h or "commodity" in h for h in headers):
            # Try to find Jaipur rows
            for tr in table.find_all("tr"):
                tds = [td.get_text(strip=True) for td in tr.find_all("td")]
                if len(tds) < 3:
                    continue
                row_text = " ".join(tds).lower()
                if "jaipur" in row_text and crop.lower() in row_text:
                    # Heuristic: find any price-like numeric
                    price = None
                    for cell in tds:
                        digits = "".join(ch for ch in cell if ch.isdigit())
                        if digits and len(digits) >= 2:
                            price = int(digits)
                            break
                    return {"market": "Jaipur", "crop": crop, "price_inr_per_quintal": price}
    return None


def get_market_price(crop: str) -> Dict:
    """Scrape Agmarknet portal for Jaipur prices. Fallback to a seeded estimate.
    """
    urls = [
        "https://agmarknet.gov.in/",
    ]
    for url in urls:
        try:
            resp = requests.get(url, timeout=10)
            if resp.status_code != 200:
                continue
            parsed = _parse_agmarknet_price(resp.text, crop)
            if parsed:
                return parsed
        except Exception:
            continue

    # Fallback seed values (illustrative ranges)
    fallback = {
        "wheat": {"market": "Jaipur", "crop": "Wheat", "price_inr_per_quintal": 2200},
        "mustard": {"market": "Jaipur", "crop": "Mustard", "price_inr_per_quintal": 5400},
    }
    return fallback.get(crop.lower(), {"market": "Jaipur", "crop": crop, "price_inr_per_quintal": None})


def get_pest_advice(crop: str) -> List[Dict]:
    """Return all pest advisory rows for the given crop from pest_info table.

    Args:
        crop: Crop name (e.g., "Wheat", "Mustard"). Case-insensitive exact match on affected_crop.

    Returns:
        List of dictionaries with keys: pest_name, affected_crop, symptoms, management_advice.
    """
    connection = _connect_db()
    try:
        cursor = connection.cursor()
        cursor.execute(
            """
            SELECT pest_name, affected_crop, symptoms, management_advice
            FROM pest_info
            WHERE LOWER(affected_crop) = LOWER(?)
            ORDER BY pest_name ASC
            """,
            (crop,),
        )
        rows = cursor.fetchall()
        keys = ["pest_name", "affected_crop", "symptoms", "management_advice"]
        return [dict(zip(keys, row)) for row in rows]
    except sqlite3.OperationalError:
        # Table may not exist if ETL hasn't been run yet
        return []
    finally:
        connection.close()
