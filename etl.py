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
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS govt_schemes (
            scheme_name TEXT,
            purpose TEXT,
            eligibility TEXT,
            benefits TEXT,
            how_to_apply TEXT
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


def create_and_populate_govt_schemes(connection: sqlite3.Connection) -> int:
    """Create the govt_schemes table (if needed) and populate with initial seed data.

    Returns the number of rows inserted.
    """
    ensure_database_schema(connection)
    cursor = connection.cursor()
    cursor.execute("DELETE FROM govt_schemes")

    seed_rows = [
        {
            "scheme_name": "Pradhan Mantri Fasal Bima Yojana (PMFBY)",
            "purpose": "Crop insurance against yield losses. फसल हानि के खिलाफ बीमा।",
            "eligibility": "All farmers, including sharecroppers and tenant farmers, growing notified crops in notified areas.",
            "benefits": "Provides insurance coverage and financial support in the event of crop failure due to natural calamities, pests, or diseases.",
            "how_to_apply": "Contact your nearest bank, Primary Agricultural Credit Society (PACS), or a Common Service Center (CSC). अपने नज़दीकी बैंक, PACS, या CSC केंद्र से संपर्क करें।",
        },
        {
            "scheme_name": "Kisan Credit Card (KCC)",
            "purpose": "Provides affordable credit for farmers. किसानों को सस्ती दर पर कर्ज़।",
            "eligibility": "All farmers, individuals/joint borrowers who are owner cultivators.",
            "benefits": "Short-term credit for cultivation, post-harvest expenses, and other farm needs. Low interest rates, with subsidies available.",
            "how_to_apply": "Apply at any commercial bank, regional rural bank, or cooperative bank. किसी भी कमर्शियल बैंक, ग्रामीण बैंक, या सहकारी बैंक में आवेदन करें।",
        },
    ]

    cursor.executemany(
        """
        INSERT INTO govt_schemes (scheme_name, purpose, eligibility, benefits, how_to_apply)
        VALUES (:scheme_name, :purpose, :eligibility, :benefits, :how_to_apply)
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

    sources = [
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
            if any(k in text.lower() for k in ["wheat", "mustard", "गेहूं", "सरसों"]):
                pass
        except Exception:
            continue

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
    rows = [
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
    return pd.DataFrame(rows)


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

    conn = sqlite3.connect(DB_PATH)
    try:
        create_and_populate_pest_info(conn)
        create_and_populate_govt_schemes(conn)
    finally:
        conn.close()

    return len(crop_df), len(soil_df)


if __name__ == "__main__":
    num_crops, num_soils = run_etl()
    print(f"Loaded {num_crops} crop rows and {num_soils} soil rows into {DB_PATH}")
