import os
import sqlite3
from typing import Tuple

import pandas as pd
import requests
from bs4 import BeautifulSoup

# Load .env if present
try:
    from dotenv import load_dotenv  # type: ignore
    load_dotenv()
except Exception:
    pass

DB_PATH = os.getenv("DB_PATH", "knowledge.db")


def ensure_database_schema(connection: sqlite3.Connection) -> None:
    """Create required tables if they do not exist."""
    cursor = connection.cursor()
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS crop_info (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            crop TEXT NOT NULL,
            location TEXT NOT NULL,
            season TEXT,
            sowing_period TEXT,
            harvesting_period TEXT,
            irrigation_schedule TEXT,
            fertilizer TEXT,
            pests TEXT
        )
        """
    )
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS soil_data (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            location TEXT NOT NULL,
            soil_type TEXT,
            ph_min REAL,
            ph_max REAL,
            n_status TEXT,
            p_status TEXT,
            k_status TEXT
        )
        """
    )
    # Define explicit schema for pest_info as requested
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS pest_info (
            pest_name TEXT,
            affected_crop TEXT,
            symptoms TEXT,
            management_advice TEXT
        )
        """
    )
    connection.commit()


def create_and_populate_pest_info(connection: sqlite3.Connection) -> int:
    """Create the pest_info table (if needed) and populate with initial seed data.

    Returns the number of rows inserted.
    """
    ensure_database_schema(connection)
    cursor = connection.cursor()
    # Clear existing rows for idempotent ETL
    cursor.execute("DELETE FROM pest_info")

    seed_rows = [
        {
            "pest_name": "White Grub (सफ़ेद लट)",
            "affected_crop": "Mustard",
            "symptoms": "Roots eaten, plant wilting, sudden drying of plants. जड़ें खाई हुई, पौधा मुरझा रहा है, अचानक सूखना।",
            "management_advice": "Apply Phorate 10G granules at 10 kg/ha before sowing. बुवाई से पहले फोरेट 10जी दाने 10 किग्रा/हेक्टेयर की दर से प्रयोग करें।",
        },
        {
            "pest_name": "Aphids (माहू or चैंपा)",
            "affected_crop": "Wheat",
            "symptoms": "Yellowing of leaves, sticky honeydew secretion, black sooty mold. पत्तियों का पीला पड़ना, चिपचिपा स्राव, काला कवक।",
            "management_advice": "Spray Imidacloprid 17.8% SL at 1 ml/litre of water. इमिडाक्लोप्रिड 17.8% एसएल का 1 मिली/लीटर पानी में घोलकर छिड़काव करें।",
        },
    ]

    cursor.executemany(
        """
        INSERT INTO pest_info (pest_name, affected_crop, symptoms, management_advice)
        VALUES (:pest_name, :affected_crop, :symptoms, :management_advice)
        """,
        seed_rows,
    )
    connection.commit()
    return cursor.rowcount or 0


def try_scrape_wheat_mustard_info() -> pd.DataFrame:
    """
    Attempt to scrape public sources for Wheat and Mustard basic info in Jaipur.
    Since public sites can change, this function includes robust fallbacks.
    Returns a DataFrame with standardized columns.
    """
    rows = []

    # Attempt 1: Example ICAR/KVK-like pages (structure may vary). We'll try to parse
    # for content blocks and keywords. If requests fail or structure unknown, we'll fallback.
    sources = [
        # These URLs are examples and may change or be unavailable; we handle failures gracefully.
        "https://icar.org.in/",
        "https://kvk.icar.gov.in/",
        "https://www.agriculture.rajasthan.gov.in/",
    ]

    for url in sources:
        try:
            resp = requests.get(url, timeout=10)
            if resp.status_code != 200:
                continue
            soup = BeautifulSoup(resp.text, "html.parser")
            text = soup.get_text(separator=" ", strip=True)
            # Very rough heuristic: not reliable, so we don't rely on it to populate structured data.
            if any(k in text.lower() for k in ["wheat", "mustard", "गेहूं", "सरसों"]):
                # We purposely do not parse due to instability; scraping here is placeholder.
                pass
        except Exception:
            continue

    # Fallback: curated minimal facts suitable for Jaipur, Rajasthan demo
    rows.extend([
        {
            "crop": "Wheat",
            "location": "Jaipur, Rajasthan",
            "season": "Rabi",
            "sowing_period": "November - December",
            "harvesting_period": "March - April",
            "irrigation_schedule": "First irrigation 20-25 days after sowing, then every 20-25 days depending on rainfall",
            "fertilizer": "Apply 120 kg N, 60 kg P2O5, 40 kg K2O per hectare in splits as per soil test",
            "pests": "Aphids, Rust; consider timely monitoring and IPM practices",
        },
        {
            "crop": "Mustard",
            "location": "Jaipur, Rajasthan",
            "season": "Rabi",
            "sowing_period": "October - November",
            "harvesting_period": "February - March",
            "irrigation_schedule": "First irrigation 25-30 days after sowing, critical stages at flowering and pod formation",
            "fertilizer": "Apply 60 kg N, 40 kg P2O5, 20 kg K2O per hectare in splits as per soil test",
            "pests": "Aphids, Alternaria blight; adopt IPM and timely sprays if required",
        },
    ])

    return pd.DataFrame(rows)


def try_scrape_soil_data_jaipur() -> pd.DataFrame:
    """
    Attempt to scrape soil characteristics for Jaipur. If not available, return fallback ranges.
    """
    # Attempt example source (structure may vary); we are resilient and fallback to curated values.
    try:
        # Placeholder attempt: often soil maps are PDFs/geoportals; skip hard scraping here
        pass
    except Exception:
        pass

    fallback = [
        {
            "location": "Jaipur, Rajasthan",
            "soil_type": "Sandy loam to loam",
            "ph_min": 6.5,
            "ph_max": 8.0,
            "n_status": "Low to Medium",
            "p_status": "Low to Medium",
            "k_status": "Medium",
        }
    ]
    return pd.DataFrame(fallback)


def load_to_sqlite(crop_df: pd.DataFrame, soil_df: pd.DataFrame, db_path: str = DB_PATH) -> None:
    connection = sqlite3.connect(db_path)
    try:
        ensure_database_schema(connection)
        crop_df.to_sql("crop_info", connection, if_exists="replace", index=False)
        soil_df.to_sql("soil_data", connection, if_exists="replace", index=False)
        connection.commit()
    finally:
        connection.close()


def run_etl() -> Tuple[int, int]:
    crop_df = try_scrape_wheat_mustard_info()
    soil_df = try_scrape_soil_data_jaipur()

    if crop_df.empty:
        raise RuntimeError("No crop data collected")
    if soil_df.empty:
        raise RuntimeError("No soil data collected")

    load_to_sqlite(crop_df, soil_df, DB_PATH)

    # Create and populate pest_info as part of ETL
    conn = sqlite3.connect(DB_PATH)
    try:
        inserted = create_and_populate_pest_info(conn)
    finally:
        conn.close()

    return len(crop_df), len(soil_df)


if __name__ == "__main__":
    num_crops, num_soils = run_etl()
    print(f"Loaded {num_crops} crop rows and {num_soils} soil rows into {DB_PATH}")
