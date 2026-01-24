# ---------------- IMPORTS ---------------- #
import os
import re
import hashlib
import logging
import unicodedata
from datetime import datetime

import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError


# ---------------- CONFIGURACIÓN ---------------- #

INPUT_DIR = "input"
OUTPUT_DIR = "output"
ERROR_DIR = "errors"
LOG_DIR = "logs"

DB_URL = "mysql://grupo_plata_user:Plata123!@83.32.73.143:3306/etl_db"
# Recomendado (si instalas pymysql): mysql+pymysql://user:pass@host:3306/db

SALT = "MI_SALT_SECRETA"

# Crear directorios si no existen
os.makedirs(OUTPUT_DIR, exist_ok=True)
os.makedirs(ERROR_DIR, exist_ok=True)
os.makedirs(LOG_DIR, exist_ok=True)

print("✅ Directorios listos:")
print(f"   - INPUT_DIR : {os.path.abspath(INPUT_DIR)}")
print(f"   - OUTPUT_DIR: {os.path.abspath(OUTPUT_DIR)}")
print(f"   - ERROR_DIR : {os.path.abspath(ERROR_DIR)}")
print(f"   - LOG_DIR   : {os.path.abspath(LOG_DIR)}")


# ---------------- LOGGING ---------------- #

logging.basicConfig(
    filename=f"{LOG_DIR}/etl.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

print(f"✅ Logging activo en: {os.path.abspath(LOG_DIR)}/etl.log")


# ---------------- UTILIDADES ---------------- #

def remove_accents(text_value):
    if pd.isna(text_value):
        return None
    return ''.join(
        c for c in unicodedata.normalize('NFD', text_value)
        if unicodedata.category(c) != 'Mn'
    )

def clean_text(text_value):
    if pd.isna(text_value):
        return None
    text_value = remove_accents(text_value)
    return text_value.strip()

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
    if value is None:
        return None
    return hashlib.sha256((SALT + value).encode()).hexdigest()

def mask_card(card):
    if not card:
        return None
    card_digits = re.sub(r"\D", "", card)
    if len(card_digits) < 4:
        return "XXXX-XXXX-XXXX-XXXX"
    return "XXXX-XXXX-XXXX-" + card_digits[-4:]


# ---------------- CARGA DE CSV ---------------- #

def load_csv(file_path):
    print(f"📥 Leyendo CSV: {file_path}")
    df = pd.read_csv(
        file_path,
        sep=";",
        dtype=str,
        encoding="utf-8",
        on_bad_lines="skip"
    )
    print(f"✅ CSV cargado: {os.path.basename(file_path)} | filas={len(df)} columnas={len(df.columns)}")
    print(f"   Columnas: {list(df.columns)}")
    return df


# ---------------- ETL CLIENTES ---------------- #

def process_clientes(df):
    print("🧽 ETL Clientes: limpiando texto (strip + quitar acentos)...")
    rejected = []

    df = df.applymap(clean_text)

    if "correo" in df.columns:
        df["correo"] = df["correo"].str.lower()

    print("🔎 Validando DNI / Teléfono / Correo...")
    df["DNI_OK"] = df["dni"].apply(validate_dni).map({True: "Y", False: "N"})
    df["DNI_KO"] = df["DNI_OK"].map({"Y": "N", "N": "Y"})

    df["Telefono_OK"] = df["telefono"].apply(validate_phone).map({True: "Y", False: "N"})
    df["Telefono_KO"] = df["Telefono_OK"].map({"Y": "N", "N": "Y"})

    df["Correo_OK"] = df["correo"].apply(validate_email).map({True: "Y", False: "N"})
    df["Correo_KO"] = df["Correo_OK"].map({"Y": "N", "N": "Y"})

    # Recolectar filas rechazadas por correo inválido
    rejected_count = 0
    for _, row in df.iterrows():
        if row["Correo_OK"] == "N":
            rejected.append(row)
            rejected_count += 1

    df_valid = df[df["Correo_OK"] == "Y"]
    df_rejected = pd.DataFrame(rejected)

    print(f"✅ Clientes: filas válidas={len(df_valid)} | rechazadas={rejected_count}")

    if not df_rejected.empty:
        rejected_path = f"{ERROR_DIR}/rows_rejected.csv"
        df_rejected.to_csv(rejected_path, index=False)
        print(f"⚠️ Rechazados guardados en: {rejected_path}")

    return df_valid


# ---------------- ETL TARJETAS ---------------- #

def process_tarjetas(df):
    print("🧽 ETL Tarjetas: limpiando texto (strip + quitar acentos)...")
    df = df.applymap(clean_text)

    print("🔐 Aplicando enmascarado + hashing de tarjetas...")
    df["numero_tarjeta_masked"] = df["numero_tarjeta"].apply(mask_card)
    df["numero_tarjeta_hash"] = df["numero_tarjeta"].apply(hash_value)

    # Eliminación de datos sensibles
    cols_to_drop = [c for c in ["numero_tarjeta", "cvv"] if c in df.columns]
    df.drop(columns=cols_to_drop, inplace=True)

    print(f"✅ Tarjetas procesadas: filas={len(df)} | columnas={list(df.columns)}")
    return df


# ---------------- BASE DE DATOS ---------------- #

def test_db_connection():
    """
    Test rápido para saber si:
    - resuelve DNS
    - credenciales ok
    - la DB responde
    """
    print("🔌 Probando conexión a MySQL...")
    try:
        engine = create_engine(DB_URL)
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        print("✅ Conexión a MySQL OK (SELECT 1)")
        return True
    except SQLAlchemyError as e:
        print("❌ Error conectando a MySQL:")
        print(f"   {e}")
        return False


def load_to_db(df, table_name):
    print(f"🗄️ Insertando en BD: tabla='{table_name}' filas={len(df)} ...")
    try:
        engine = create_engine(DB_URL)

        with engine.begin() as conn:
            # prueba rápida antes de insertar
            conn.execute(text("SELECT 1"))
            df.to_sql(table_name, conn, if_exists="append", index=False)

        print(f"✅ Insert OK en '{table_name}' (filas insertadas={len(df)})")
    except SQLAlchemyError as e:
        print(f"❌ Error insertando en '{table_name}': {e}")
        logging.exception(f"Error insertando en {table_name}: {e}")
        # Re-lanzar si quieres que el pipeline “rompa”:
        # raise


# ---------------- PIPELINE PRINCIPAL ---------------- #

def run_pipeline():
    logging.info("Inicio del pipeline ETL")
    print("\n🚀 INICIO PIPELINE ETL\n")

    # Comprobación de carpeta input
    if not os.path.exists(INPUT_DIR):
        print(f"❌ No existe la carpeta '{INPUT_DIR}'. Crea la carpeta y pon los CSV dentro.")
        return

    files = os.listdir(INPUT_DIR)
    print(f"📂 Archivos encontrados en '{INPUT_DIR}': {len(files)}")
    for f in files:
        print(f"   - {f}")

    # Test DB (opcional pero útil)
    db_ok = test_db_connection()
    if not db_ok:
        print("⚠️ La BD no está accesible ahora. Seguiré generando CSV limpios, pero NO insertaré en BD.")
        print("   (Revisa DB_URL, firewall, credenciales, driver pymysql/mysqlconnector)\n")

    processed_any = False
    inserted_total = 0

    for file in files:
        input_path = f"{INPUT_DIR}/{file}"

        # Procesar clientes
        if re.match(r"Clientes-\d{4}-\d{2}-\d{2}\.csv", file):
            processed_any = True
            print(f"\n🧾 Detectado fichero CLIENTES: {file}")
            logging.info(f"Procesando {file}")

            df = load_csv(input_path)
            df_clean = process_clientes(df)

            output_file = f"{OUTPUT_DIR}/{file.replace('.csv', '.cleaned.csv')}"
            df_clean.to_csv(output_file, index=False)
            print(f"💾 CSV limpio guardado: {output_file}")

            if db_ok and len(df_clean) > 0:
                load_to_db(df_clean, "clientes")
                inserted_total += len(df_clean)
            else:
                print("⏭️ Saltando inserción en BD (sin conexión o 0 filas).")

        # Procesar tarjetas
        elif re.match(r"Tarjetas-\d{4}-\d{2}-\d{2}\.csv", file):
            processed_any = True
            print(f"\n💳 Detectado fichero TARJETAS: {file}")
            logging.info(f"Procesando {file}")

            df = load_csv(input_path)
            df_clean = process_tarjetas(df)

            output_file = f"{OUTPUT_DIR}/{file.replace('.csv', '.cleaned.csv')}"
            df_clean.to_csv(output_file, index=False)
            print(f"💾 CSV limpio guardado: {output_file}")

            if db_ok and len(df_clean) > 0:
                load_to_db(df_clean, "tarjetas")
                inserted_total += len(df_clean)
            else:
                print("⏭️ Saltando inserción en BD (sin conexión o 0 filas).")

        # Ignorar otros archivos
        else:
            print(f"🚫 Ignorado (no cumple patrón): {file}")
            logging.warning(f"Fichero ignorado: {file}")

    if not processed_any:
        print("\n⚠️ No se encontró ningún CSV válido con patrón Clientes-YYYY-MM-DD.csv o Tarjetas-YYYY-MM-DD.csv")
        print("   Asegúrate de los nombres y la carpeta input/")

    print("\n📊 RESUMEN:")
    print(f"   - Archivos en input: {len(files)}")
    print(f"   - BD accesible: {'Sí' if db_ok else 'No'}")
    print(f"   - Total filas insertadas (si BD ok): {inserted_total}")

    logging.info("Fin del pipeline ETL")
    print("\n✅ FIN PIPELINE ETL\n")


# ---------------- EJECUCIÓN ---------------- #

if __name__ == "__main__":
    run_pipeline()
