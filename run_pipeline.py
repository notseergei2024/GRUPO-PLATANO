import os
import re
import hashlib
import logging
import unicodedata
from datetime import datetime

import pandas as pd
from sqlalchemy import create_engine, text

#CONFIG#

INPUT_DIR = "input"
OUTPUT_DIR = "output"
ERROR_DIR = "errors"
LOG_DIR = "logs"

DB_URL = "postgresql://user:password@localhost:5432/etl_db"
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

# UTILS#

def remove_accents(text):
    if pd.isna(text):
        return None
    return ''.join(
        c for c in unicodedata.normalize('NFD', text)
        if unicodedata.category(c) != 'Mn'
    )

def clean_text(text):
    if pd.isna(text):
        return None
    text = remove_accents(text)
    return text.strip()

def validate_email(email):
    if not email:
        return False
    pattern = r"^[\w\.-]+@[\w\.-]+\.\w+$"
    return re.match(pattern, email) is not None

def validate_phone(phone):
    if not phone:
        return False
    return phone.isdigit() and len(phone) >= 9

def validate_dni(dni):
    if not dni:
        return False
    dni = dni.upper()
    match = re.match(r"(\d{8})([A-Z])", dni)
    if not match:
        return False
    number, letter = match.groups()
    letters = "TRWAGMYFPDXBNJZSQVHLCKE"
    return letters[int(number) % 23] == letter

def hash_value(value):
    return hashlib.sha256((SALT + value).encode()).hexdigest()

def mask_card(card):
    card = re.sub(r"\D", "", card)
    return "XXXX-XXXX-XXXX-" + card[-4:]

#  LOAD CSV #

def load_csv(file_path):
    return pd.read_csv(
        file_path,
        sep=";",
        dtype=str,
        encoding="utf-8",
        on_bad_lines="skip"
    )

# ETL CLIENTES#

def process_clientes(df):
    rejected = []

    df = df.applymap(clean_text)

    df["correo"] = df["correo"].str.lower()

    df["DNI_OK"] = df["dni"].apply(validate_dni).map({True: "Y", False: "N"})
    df["DNI_KO"] = df["DNI_OK"].map({"Y": "N", "N": "Y"})

    df["Telefono_OK"] = df["telefono"].apply(validate_phone).map({True: "Y", False: "N"})
    df["Telefono_KO"] = df["Telefono_OK"].map({"Y": "N", "N": "Y"})

    df["Correo_OK"] = df["correo"].apply(validate_email).map({True: "Y", False: "N"})
    df["Correo_KO"] = df["Correo_OK"].map({"Y": "N", "N": "Y"})

    for idx, row in df.iterrows():
        if row["Correo_OK"] == "N":
            rejected.append(row)
    
    df_valid = df[df["Correo_OK"] == "Y"]
    df_rejected = pd.DataFrame(rejected)

    if not df_rejected.empty:
        df_rejected.to_csv(f"{ERROR_DIR}/rows_rejected.csv", index=False)

    return df_valid

#  ETL TARJETAS #

def process_tarjetas(df):
    df = df.applymap(clean_text)

    df["numero_tarjeta_masked"] = df["numero_tarjeta"].apply(mask_card)
    df["numero_tarjeta_hash"] = df["numero_tarjeta"].apply(hash_value)

    df.drop(columns=["numero_tarjeta", "cvv"], inplace=True)

    return df

#DATABASE #

def load_to_db(df, table_name):
    engine = create_engine(DB_URL)

    with engine.begin() as conn:
        df.to_sql(table_name, conn, if_exists="append", index=False)

#  MAIN PIPELINE #

def run_pipeline():
    logging.info("Inicio del pipeline ETL")

    for file in os.listdir(INPUT_DIR):
        if re.match(r"Clientes-\d{4}-\d{2}-\d{2}\.csv", file):
            logging.info(f"Procesando {file}")
            df = load_csv(f"{INPUT_DIR}/{file}")
            df_clean = process_clientes(df)
            output_file = f"{OUTPUT_DIR}/{file.replace('.csv', '.cleaned.csv')}"
            df_clean.to_csv(output_file, index=False)
            load_to_db(df_clean, "clientes")

        elif re.match(r"Tarjetas-\d{4}-\d{2}-\d{2}\.csv", file):
            logging.info(f"Procesando {file}")
            df = load_csv(f"{INPUT_DIR}/{file}")
            df_clean = process_tarjetas(df)
            output_file = f"{OUTPUT_DIR}/{file.replace('.csv', '.cleaned.csv')}"
            df_clean.to_csv(output_file, index=False)
            load_to_db(df_clean, "tarjetas")

        else:
            logging.warning(f"Fichero ignorado: {file}")

    logging.info("Fin del pipeline ETL")

if __name__ == "__main__":
    run_pipeline()
