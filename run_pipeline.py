# ============================================================
# ETL PIPELINE - CLIENTES & TARJETAS
# ============================================================

# ---------------- IMPORTS ---------------- #

import os
import re
import hashlib
import logging
import unicodedata
from datetime import datetime

import pandas as pd
from sqlalchemy import create_engine, text


# ---------------- CONFIGURACIÓN ---------------- #

INPUT_DIR = "input"
OUTPUT_DIR = "output"
ERROR_DIR = "errors"
LOG_DIR = "logs"

DB_URL = "mysql://grupo_plata_user:Plata123!@83.32.73.143:3306/etl_db"
SALT = "MI_SALT_SECRETA"

os.makedirs(OUTPUT_DIR, exist_ok=True)
os.makedirs(ERROR_DIR, exist_ok=True)
os.makedirs(LOG_DIR, exist_ok=True)


# ---------------- LOGGING ---------------- #

logging.basicConfig(
    filename=f"{LOG_DIR}/etl.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)


# ---------------- DATABASE ---------------- #

engine = create_engine(DB_URL)

def get_connection():
    return engine.begin()


# ---------------- CACHE ---------------- #

CLIENT_CACHE = set()


# ---------------- UTILIDADES TEXTO ---------------- #

def remove_accents(text: str) -> str | None:
    if pd.isna(text):
        return None
    return ''.join(
        c for c in unicodedata.normalize('NFD', text)
        if unicodedata.category(c) != 'Mn'
    )

def clean_text(text: str) -> str | None:
    if pd.isna(text):
        return None
    return remove_accents(text).strip()

def normalize_name_case(name: str) -> str | None:
    if pd.isna(name):
        return None
    cleaned = clean_text(name)
    if not cleaned:
        return cleaned
    cleaned = re.sub(r"\s+", " ", cleaned)
    return cleaned.title()


# ---------------- VALIDACIONES ---------------- #

def validate_email(email: str) -> bool:
    if not email:
        return False
    pattern = r"^[\w\.-]+@[\w\.-]+\.\w+$"
    return re.match(pattern, email) is not None

def validate_phone(phone: str) -> bool:
    if not phone:
        return False
    return phone.isdigit() and len(phone) >= 9

def validate_dni(dni: str) -> bool:
    if not dni:
        return False
    dni = dni.upper()
    match = re.match(r"(\d{8})([A-Z])", dni)
    if not match:
        return False
    number, letter = match.groups()
    letters = "TRWAGMYFPDXBNJZSQVHLCKE"
    return letters[int(number) % 23] == letter

def validate_name_case(name: str) -> bool:
    if not name:
        return False
    normalized = normalize_name_case(name)
    return normalized is not None and name == normalized


# ---------------- SEGURIDAD ---------------- #

def hash_value(value: str) -> str:
    return hashlib.sha256((SALT + value).encode()).hexdigest()

def mask_card(card: str) -> str:
    card = re.sub(r"\D", "", card)
    return "XXXX-XXXX-XXXX-" + card[-4:]


# ---------------- FECHAS ---------------- #

def parse_expiration_date(value: str) -> str | None:
    """
    Convierte MM/YY o YYYY-MM a YYYY-MM-01
    """
    if not value:
        return None
    try:
        if "/" in value:
            return datetime.strptime(value, "%m/%y").strftime("%Y-%m-01")
        return datetime.strptime(value, "%Y-%m").strftime("%Y-%m-01")
    except ValueError:
        return None


# ---------------- CSV ---------------- #

def load_csv(file_path: str) -> pd.DataFrame:
    return pd.read_csv(
        file_path,
        sep=";",
        dtype=str,
        encoding="utf-8",
        on_bad_lines="skip"
    )


# ---------------- CLIENTES ---------------- #

def cliente_exists(conn, cliente_id: str, nombre: str) -> bool:
    query = text("""
        SELECT 1
        FROM clientes
        WHERE id = :id AND nombre = :nombre
        LIMIT 1
    """)
    result = conn.execute(query, {"id": cliente_id, "nombre": nombre})
    return result.first() is not None


def process_clientes(df: pd.DataFrame) -> pd.DataFrame:
    rejected = []

    df = df.astype("string").apply(lambda col: col.map(clean_text))
    df["correo"] = df["correo"].str.lower()
    df["Nombre_OK"] = df["nombre"].apply(validate_name_case).map({True: "Y", False: "N"})
    df["nombre"] = df["nombre"].apply(normalize_name_case)

    df["DNI_OK"] = df["dni"].apply(validate_dni).map({True: "Y", False: "N"})
    df["Telefono_OK"] = df["telefono"].apply(validate_phone).map({True: "Y", False: "N"})
    df["Correo_OK"] = df["correo"].apply(validate_email).map({True: "Y", False: "N"})

    for _, row in df.iterrows():
        if row["Correo_OK"] == "N":
            rejected.append(row)

    df_valid = df[df["Correo_OK"] == "Y"]
    df_rejected = pd.DataFrame(rejected)

    if not df_rejected.empty:
        df_rejected.to_csv(f"{ERROR_DIR}/rows_rejected.csv", index=False)

    # ---- FILTRADO POR CACHE + DB ---- #

    filtered_rows = []

    with get_connection() as conn:
        for _, row in df_valid.iterrows():
            key = (row["id"], row["nombre"])

            if key in CLIENT_CACHE:
                continue

            if cliente_exists(conn, row["id"], row["nombre"]):
                CLIENT_CACHE.add(key)
                continue

            CLIENT_CACHE.add(key)
            filtered_rows.append(row)

    return pd.DataFrame(filtered_rows)


# ---------------- TARJETAS ---------------- #

def process_tarjetas(df: pd.DataFrame) -> pd.DataFrame:
    df = df.astype("string").apply(lambda col: col.map(clean_text))
    if "cod_cliente" in df.columns:
        df["cod_cliente"] = df["cod_cliente"].str.upper()

    df["numero_tarjeta_masked"] = df["numero_tarjeta"].apply(mask_card)
    df["numero_tarjeta_hash"] = df["numero_tarjeta"].apply(hash_value)

    df["fecha_expiracion"] = df["fecha_expiracion"].apply(parse_expiration_date)

    df.drop(columns=["numero_tarjeta", "cvv"], inplace=True)

    return df


# ---------------- LOAD DB ---------------- #

def load_to_db(df: pd.DataFrame, table_name: str) -> None:
    logging.info(f"Cargando datos en {table_name}")
    with get_connection() as conn:
        df.to_sql(table_name, conn, if_exists="append", index=False)


# ---------------- PIPELINE ---------------- #

def run_pipeline():
    logging.info("Inicio del pipeline ETL")

    for file in os.listdir(INPUT_DIR):

        file_path = f"{INPUT_DIR}/{file}"

        if re.match(r"Clientes-\d{4}-\d{2}-\d{2}\.csv", file):
            logging.info(f"Procesando {file}")
            df = load_csv(file_path)
            df_clean = process_clientes(df)
            df_clean.to_csv(f"{OUTPUT_DIR}/{file}.cleaned.csv", index=False)
            load_to_db(df_clean, "clientes")

        elif re.match(r"Tarjetas-\d{4}-\d{2}-\d{2}\.csv", file):
            logging.info(f"Procesando {file}")
            df = load_csv(file_path)
            df_clean = process_tarjetas(df)
            df_clean.to_csv(f"{OUTPUT_DIR}/{file}.cleaned.csv", index=False)
            load_to_db(df_clean, "tarjetas")

        else:
            logging.warning(f"Fichero ignorado: {file}")

    logging.info("Fin del pipeline ETL")


# ---------------- MAIN ---------------- #

if __name__ == "__main__":
    run_pipeline()
    print("Pipeline ETL completado.")
