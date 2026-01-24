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

# ⚠️ Si la BD está apagada, NO pasa nada: este script no debe romper
# Recomendado cuando la pongáis: mysql+pymysql://user:pass@host:3306/db (requiere pip install pymysql)
DB_URL = "mysql://grupo_plata_user:Plata123!@83.32.73.143:3306/etl_db"

SALT = "MI_SALT_SECRETA"

CLIENTES_PATTERN = r"^Clientes-\d{4}-\d{2}-\d{2}\.csv$"
TARJETAS_PATTERN = r"^Tarjetas-\d{4}-\d{2}-\d{2}\.csv$"

# Crear directorios si no existen
os.makedirs(OUTPUT_DIR, exist_ok=True)
os.makedirs(ERROR_DIR, exist_ok=True)
os.makedirs(LOG_DIR, exist_ok=True)

RUN_ID = datetime.now().strftime("%Y%m%d_%H%M%S")


# ---------------- LOGGING ---------------- #

logging.basicConfig(
    filename=f"{LOG_DIR}/etl_{RUN_ID}.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)


# ---------------- UTILIDADES (LIMPIEZA / VALIDACIÓN) ---------------- #

def debug(msg: str):
    print(msg)
    logging.info(msg)

def warn(msg: str):
    print(f"⚠️ {msg}")
    logging.warning(msg)

def err(msg: str):
    print(f"❌ {msg}")
    logging.error(msg)

def remove_accents(text_value):
    if pd.isna(text_value):
        return None
    return "".join(
        c for c in unicodedata.normalize("NFD", str(text_value))
        if unicodedata.category(c) != "Mn"
    )

def clean_text(text_value):
    if pd.isna(text_value):
        return None
    text_value = remove_accents(text_value)
    return str(text_value).strip()

def normalize_dni(dni):
    """Quita espacios/guiones y pone mayúsculas."""
    if not dni:
        return None
    dni = re.sub(r"[\s\-]", "", str(dni)).upper()
    return dni

def normalize_phone(phone):
    """Deja solo dígitos (si viene con espacios o guiones)."""
    if not phone:
        return None
    digits = re.sub(r"\D", "", str(phone))
    return digits if digits else None

def validate_email(email):
    if not email:
        return False
    pattern = r"^[\w\.-]+@[\w\.-]+\.\w+$"
    return re.match(pattern, str(email)) is not None

def validate_phone(phone):
    if not phone:
        return False
    return str(phone).isdigit() and len(str(phone)) >= 9

def validate_dni(dni):
    if not dni:
        return False
    dni = str(dni).upper()
    match = re.match(r"^(\d{8})([A-Z])$", dni)
    if not match:
        return False
    number, letter = match.groups()
    letters = "TRWAGMYFPDXBNJZSQVHLCKE"
    return letters[int(number) % 23] == letter

def hash_value(value):
    if value is None:
        return None
    return hashlib.sha256((SALT + str(value)).encode("utf-8")).hexdigest()

def mask_card(card):
    if not card:
        return None
    digits = re.sub(r"\D", "", str(card))
    if len(digits) < 4:
        return "XXXX-XXXX-XXXX-XXXX"
    return "XXXX-XXXX-XXXX-" + digits[-4:]


# ---------------- CARGA DE CSV (ROBUSTA) ---------------- #

def load_csv(file_path):
    debug(f"📥 Leyendo CSV: {file_path}")

    if not os.path.exists(file_path):
        err(f"No existe el fichero: {file_path}")
        return None

    # Intento 1: utf-8
    try:
        df = pd.read_csv(
            file_path,
            sep=";",
            dtype=str,
            encoding="utf-8",
            on_bad_lines="skip"
        )
        debug(f"✅ CSV cargado (utf-8): filas={len(df)} columnas={len(df.columns)}")
        debug(f"   Columnas: {list(df.columns)}")
        return df
    except UnicodeDecodeError:
        warn("No se pudo leer en utf-8. Intentando latin-1...")
    except Exception as e:
        err(f"Fallo leyendo CSV: {e}")
        return None

    # Intento 2: latin-1
    try:
        df = pd.read_csv(
            file_path,
            sep=";",
            dtype=str,
            encoding="latin-1",
            on_bad_lines="skip"
        )
        debug(f"✅ CSV cargado (latin-1): filas={len(df)} columnas={len(df.columns)}")
        debug(f"   Columnas: {list(df.columns)}")
        return df
    except Exception as e:
        err(f"Fallo leyendo CSV incluso con latin-1: {e}")
        return None


# ---------------- VALIDACIÓN DE COLUMNAS REQUERIDAS ---------------- #

def require_columns(df, required, file_name):
    missing = [c for c in required if c not in df.columns]
    if missing:
        err(f"{file_name}: faltan columnas requeridas: {missing}")
        warn(f"{file_name}: columnas presentes: {list(df.columns)}")
        return False
    return True


# ---------------- ETL CLIENTES ---------------- #

def process_clientes(df, source_file):
    debug("🧽 ETL Clientes: limpieza general (strip + quitar acentos)...")
    df = df.applymap(clean_text)

    required = ["cod cliente", "nombre", "apellido1", "apellido2", "dni", "correo", "telefono"]
    # Algunos CSV a veces traen "Cod cliente" o "cod_cliente". Intentamos normalizar nombres básicos:
    df.columns = [clean_text(c).lower().replace("_", " ") for c in df.columns]

    if not require_columns(df, required, source_file):
        return None, None

    # Normalizaciones específicas
    df["correo"] = df["correo"].str.lower()
    df["dni"] = df["dni"].apply(normalize_dni)
    df["telefono"] = df["telefono"].apply(normalize_phone)

    debug("🔎 Validando DNI / Teléfono / Correo...")
    df["DNI_OK"] = df["dni"].apply(validate_dni).map({True: "Y", False: "N"})
    df["DNI_KO"] = df["DNI_OK"].map({"Y": "N", "N": "Y"})

    df["Telefono_OK"] = df["telefono"].apply(validate_phone).map({True: "Y", False: "N"})
    df["Telefono_KO"] = df["Telefono_OK"].map({"Y": "N", "N": "Y"})

    df["Correo_OK"] = df["correo"].apply(validate_email).map({True: "Y", False: "N"})
    df["Correo_KO"] = df["Correo_OK"].map({"Y": "N", "N": "Y"})

    # Rechazados: por correo inválido (como mínimo). Puedes añadir más motivos si queréis.
    df_rejected = df[df["Correo_OK"] == "N"].copy()
    df_valid = df[df["Correo_OK"] == "Y"].copy()

    debug(f"✅ Clientes: válidas={len(df_valid)} | rechazadas={len(df_rejected)}")

    # Guardar rechazados con motivo
    if not df_rejected.empty:
        df_rejected["motivo_rechazo"] = "correo_invalido"
        rejected_path = f"{ERROR_DIR}/rows_rejected_clientes_{RUN_ID}.csv"
        df_rejected.to_csv(rejected_path, index=False)
        warn(f"Rechazados clientes guardados en: {rejected_path}")

    # (Opcional) anonimizar DNI en clientes: guardamos hash y eliminamos dni original
    df_valid["dni_hash"] = df_valid["dni"].apply(hash_value)
    # Si queréis mantener el dni limpio, comentad la siguiente línea:
    df_valid.drop(columns=["dni"], inplace=True)

    # Renombrado columnas a formato DB (sin espacios)
    rename_map = {
        "cod cliente": "cod_cliente",
        "apellido1": "apellido1",
        "apellido2": "apellido2",
    }
    df_valid.rename(columns=rename_map, inplace=True)

    return df_valid, df_rejected


# ---------------- ETL TARJETAS ---------------- #

def process_tarjetas(df, source_file):
    debug("🧽 ETL Tarjetas: limpieza general (strip + quitar acentos)...")
    df = df.applymap(clean_text)

    df.columns = [clean_text(c).lower().replace("_", " ") for c in df.columns]
    required = ["cod cliente", "numero tarjeta", "fecha exp", "cvv"]

    if not require_columns(df, required, source_file):
        return None

    debug("🔐 Enmascarando + hasheando numero de tarjeta...")
    df["numero_tarjeta_masked"] = df["numero tarjeta"].apply(mask_card)
    df["numero_tarjeta_hash"] = df["numero tarjeta"].apply(hash_value)

    # NUNCA guardar CVV en claro
    df.drop(columns=["numero tarjeta", "cvv"], inplace=True)

    # Renombrado columnas
    df.rename(columns={
        "cod cliente": "cod_cliente",
        "fecha exp": "fecha_exp"
    }, inplace=True)

    debug(f"✅ Tarjetas: filas={len(df)} | columnas={list(df.columns)}")
    return df


# ---------------- BASE DE DATOS (PREPARADA, PERO SKIP SI CAE) ---------------- #

def test_db_connection():
    debug("🔌 Probando conexión a MySQL (si está apagada, no rompemos)...")
    try:
        engine = create_engine(DB_URL)
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        debug("✅ Conexión a MySQL OK (SELECT 1)")
        return True
    except Exception as e:
        warn(f"No hay conexión a BD (esperado si está apagada): {e}")
        return False

def load_to_db(df, table_name):
    """Carga a BD SOLO si está disponible. Si falla, no rompe la ETL."""
    if df is None or df.empty:
        warn(f"Tabla {table_name}: no hay filas para insertar.")
        return

    try:
        engine = create_engine(DB_URL)
        with engine.begin() as conn:
            conn.execute(text("SELECT 1"))
            df.to_sql(table_name, conn, if_exists="append", index=False)
        debug(f"✅ Insertadas {len(df)} filas en '{table_name}'")
    except Exception as e:
        warn(f"SKIP insert en BD (BD no disponible o error driver): {e}")


# ---------------- PIPELINE PRINCIPAL ---------------- #

def run_pipeline():
    debug("🚀 INICIO PIPELINE ETL")

    # Check input dir
    if not os.path.exists(INPUT_DIR):
        err(f"No existe la carpeta '{INPUT_DIR}'. Créala y mete los CSV.")
        return

    files = os.listdir(INPUT_DIR)
    debug(f"📂 Archivos encontrados en '{INPUT_DIR}': {len(files)}")
    for f in files:
        debug(f"   - {f}")

    # Detectar válidos por patrón estricto
    clientes_files = [f for f in files if re.match(CLIENTES_PATTERN, f)]
    tarjetas_files = [f for f in files if re.match(TARJETAS_PATTERN, f)]
    ignored_files = [f for f in files if f not in clientes_files + tarjetas_files]

    debug(f"🧾 Clientes válidos por patrón: {len(clientes_files)} -> {clientes_files}")
    debug(f"💳 Tarjetas válidos por patrón: {len(tarjetas_files)} -> {tarjetas_files}")

    if ignored_files:
        warn(f"Se ignorarán {len(ignored_files)} ficheros por no cumplir patrón: {ignored_files}")
        warn("Ejemplo: 'Clientes.csv' NO vale. Debe ser 'Clientes-YYYY-MM-DD.csv'.")

    if not clientes_files and not tarjetas_files:
        warn("No hay ficheros válidos para procesar. Revisa nombres y patrón.")
        debug("✅ FIN PIPELINE ETL (sin procesar)")
        return

    # Probar BD (sabemos que está apagada, pero dejamos el código listo)
    db_ok = test_db_connection()

    processed = 0

    # Procesar Clientes
    for file in clientes_files:
        processed += 1
        debug(f"\n🧾 Procesando CLIENTES: {file}")
        path = os.path.join(INPUT_DIR, file)

        df = load_csv(path)
        if df is None:
            err(f"Saltando {file} (no se pudo cargar)")
            continue

        df_clean, df_rej = process_clientes(df, file)
        if df_clean is None:
            err(f"Saltando {file} (falló validación de columnas)")
            continue

        out_path = os.path.join(OUTPUT_DIR, file.replace(".csv", ".cleaned.csv"))
        df_clean.to_csv(out_path, index=False)
        debug(f"💾 Guardado output clientes: {out_path} | filas={len(df_clean)}")

        if db_ok:
            load_to_db(df_clean, "clientes")
        else:
            debug("⏭️ BD apagada: salto inserción clientes (esto es esperado ahora).")

    # Procesar Tarjetas
    for file in tarjetas_files:
        processed += 1
        debug(f"\n💳 Procesando TARJETAS: {file}")
        path = os.path.join(INPUT_DIR, file)

        df = load_csv(path)
        if df is None:
            err(f"Saltando {file} (no se pudo cargar)")
            continue

        df_clean = process_tarjetas(df, file)
        if df_clean is None:
            err(f"Saltando {file} (falló validación de columnas)")
            continue

        out_path = os.path.join(OUTPUT_DIR, file.replace(".csv", ".cleaned.csv"))
        df_clean.to_csv(out_path, index=False)
        debug(f"💾 Guardado output tarjetas: {out_path} | filas={len(df_clean)}")

        if db_ok:
            load_to_db(df_clean, "tarjetas")
        else:
            debug("⏭️ BD apagada: salto inserción tarjetas (esto es esperado ahora).")

    debug("\n📊 RESUMEN EJECUCIÓN")
    debug(f"   - Procesados: {processed}")
    debug(f"   - BD disponible: {'Sí' if db_ok else 'No'}")
    debug(f"   - Logs: {os.path.abspath(LOG_DIR)}/etl_{RUN_ID}.log")
    debug("✅ FIN PIPELINE ETL")


# ---------------- EJECUCIÓN ---------------- #

if __name__ == "__main__":
    run_pipeline()
