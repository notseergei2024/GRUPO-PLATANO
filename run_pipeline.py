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


# ---------------- CONFIG ---------------- #

INPUT_DIR = "input"
OUTPUT_DIR = "output"
ERROR_DIR = "errors"
LOG_DIR = "logs"

# ✅ Para ahora (BD apagada): NO intentamos conectar.
ENABLE_DB = False

# Cuando activéis BD, usad esto (requiere pip install pymysql):
# DB_URL = "mysql+pymysql://grupo_plata_user:Plata123!@83.32.73.143:3306/etl_db"
DB_URL = "mysql://grupo_plata_user:Plata123!@83.32.73.143:3306/etl_db"

SALT = "MI_SALT_SECRETA"

CLIENTES_PATTERN = r"^Clientes-\d{4}-\d{2}-\d{2}\.csv$"
TARJETAS_PATTERN = r"^Tarjetas-\d{4}-\d{2}-\d{2}\.csv$"

RUN_ID = datetime.now().strftime("%Y%m%d_%H%M%S")

os.makedirs(OUTPUT_DIR, exist_ok=True)
os.makedirs(ERROR_DIR, exist_ok=True)
os.makedirs(LOG_DIR, exist_ok=True)


# ---------------- LOGGING ---------------- #

logging.basicConfig(
    filename=os.path.join(LOG_DIR, f"etl_{RUN_ID}.log"),
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)


# ---------------- PRINT/LOG HELPERS ---------------- #

def logi(msg: str):
    print(msg)
    logging.info(msg)

def logw(msg: str):
    print(f"⚠️ {msg}")
    logging.warning(msg)

def loge(msg: str):
    print(f"❌ {msg}")
    logging.error(msg)


# ---------------- TEXT CLEANING ---------------- #

def remove_accents(value):
    if pd.isna(value):
        return None
    s = str(value)
    return "".join(
        c for c in unicodedata.normalize("NFD", s)
        if unicodedata.category(c) != "Mn"
    )

def clean_text(value):
    if pd.isna(value):
        return None
    return remove_accents(value).strip()

def normalize_dni(dni):
    if not dni:
        return None
    dni = str(dni)
    dni = re.sub(r"[\s\-]", "", dni).upper()
    return dni

def normalize_phone(phone):
    if not phone:
        return None
    digits = re.sub(r"\D", "", str(phone))
    return digits if digits else None


# ---------------- VALIDATORS ---------------- #

def validate_email(email):
    if not email:
        return False
    pattern = r"^[\w\.-]+@[\w\.-]+\.\w+$"
    return re.match(pattern, str(email)) is not None

def validate_phone(phone):
    if not phone:
        return False
    phone = str(phone)
    return phone.isdigit() and len(phone) >= 9

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


# ---------------- ANONYMIZATION ---------------- #

def hash_value(value):
    if value is None:
        return None
    raw = (SALT + str(value)).encode("utf-8")
    return hashlib.sha256(raw).hexdigest()

def mask_card(card):
    if not card:
        return None
    digits = re.sub(r"\D", "", str(card))
    if len(digits) < 4:
        return "XXXX-XXXX-XXXX-XXXX"
    return "XXXX-XXXX-XXXX-" + digits[-4:]


# ---------------- CSV LOADER (ROBUST) ---------------- #

def load_csv(file_path: str) -> pd.DataFrame | None:
    logi(f"📥 Leyendo CSV: {file_path}")

    if not os.path.exists(file_path):
        loge(f"No existe el fichero: {file_path}")
        return None

    # Intento UTF-8
    try:
        df = pd.read_csv(
            file_path,
            sep=";",
            dtype=str,
            encoding="utf-8",
            on_bad_lines="skip"
        )
        logi(f"✅ CSV cargado (utf-8) filas={len(df)} columnas={len(df.columns)}")
        logi(f"   Columnas: {list(df.columns)}")
        return df
    except UnicodeDecodeError:
        logw("No se pudo leer como utf-8. Intentando latin-1...")
    except Exception as e:
        loge(f"Error leyendo CSV: {e}")
        return None

    # Fallback latin-1
    try:
        df = pd.read_csv(
            file_path,
            sep=";",
            dtype=str,
            encoding="latin-1",
            on_bad_lines="skip"
        )
        logi(f"✅ CSV cargado (latin-1) filas={len(df)} columnas={len(df.columns)}")
        logi(f"   Columnas: {list(df.columns)}")
        return df
    except Exception as e:
        loge(f"Error leyendo CSV con latin-1: {e}")
        return None


# ---------------- COLUMN NORMALIZATION ---------------- #

def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Normaliza nombres de columnas para que sean comparables:
    - strip + lower
    - espacios -> _
    - elimina acentos
    """
    cleaned_cols = []
    for c in df.columns:
        c2 = clean_text(c)
        c2 = c2.lower().replace(" ", "_")
        cleaned_cols.append(c2)
    df.columns = cleaned_cols
    return df

def require_columns(df: pd.DataFrame, required: list[str], source_file: str) -> bool:
    missing = [c for c in required if c not in df.columns]
    if missing:
        loge(f"{source_file}: faltan columnas requeridas: {missing}")
        logw(f"{source_file}: columnas presentes: {list(df.columns)}")
        return False
    return True


# ---------------- ETL CLIENTES ---------------- #

def process_clientes(df: pd.DataFrame, source_file: str):
    logi("🧽 ETL Clientes: limpieza general (strip + quitar acentos)...")

    df = normalize_columns(df)

    required = ["cod_cliente", "nombre", "apellido1", "apellido2", "dni", "correo", "telefono"]
    if not require_columns(df, required, source_file):
        return None, None

    # Limpieza celda a celda
    df = df.astype("string").apply(lambda col: col.map(clean_text))
    print("DEBUG limpieza OK - filas:", len(df), "columnas:", list(df.columns))

    # Normalizaciones específicas
    df["correo"] = df["correo"].str.lower()
    df["dni"] = df["dni"].apply(normalize_dni)
    df["telefono"] = df["telefono"].apply(normalize_phone)

    logi("🔎 Validaciones: DNI / Teléfono / Correo...")
    df["DNI_OK"] = df["dni"].apply(validate_dni).map({True: "Y", False: "N"})
    df["DNI_KO"] = df["DNI_OK"].map({"Y": "N", "N": "Y"})

    df["Telefono_OK"] = df["telefono"].apply(validate_phone).map({True: "Y", False: "N"})
    df["Telefono_KO"] = df["Telefono_OK"].map({"Y": "N", "N": "Y"})

    df["Correo_OK"] = df["correo"].apply(validate_email).map({True: "Y", False: "N"})
    df["Correo_KO"] = df["Correo_OK"].map({"Y": "N", "N": "Y"})

    # Rechazados mínimos: correo inválido
    df_rejected = df[df["Correo_OK"] == "N"].copy()
    df_valid = df[df["Correo_OK"] == "Y"].copy()

    logi(f"✅ Clientes: válidas={len(df_valid)} | rechazadas={len(df_rejected)}")

    if not df_rejected.empty:
        df_rejected["motivo_rechazo"] = "correo_invalido"
        rej_path = os.path.join(ERROR_DIR, f"rows_rejected_clientes_{RUN_ID}.csv")
        df_rejected.to_csv(rej_path, index=False)
        logw(f"Rechazados guardados en: {rej_path}")

    # Anonimización DNI (hash) y eliminamos dni en claro
    df_valid["dni_hash"] = df_valid["dni"].apply(hash_value)
    df_valid.drop(columns=["dni"], inplace=True)

    return df_valid, df_rejected


# ---------------- ETL TARJETAS ---------------- #

def process_tarjetas(df: pd.DataFrame, source_file: str):
    logi("🧽 ETL Tarjetas: limpieza general (strip + quitar acentos)...")

    df = normalize_columns(df)

    required = ["cod_cliente", "numero_tarjeta", "fecha_exp", "cvv"]
    if not require_columns(df, required, source_file):
        return None

    df = df.astype("string").apply(lambda col: col.map(clean_text))
    print("DEBUG Tarjetas limpieza OK -> filas:", len(df), "columnas:", list(df.columns))
    print("DEBUG ejemplo numero_tarjeta (primeras 3):",
          df["numero_tarjeta"].head(3).tolist() if "numero_tarjeta" in df.columns else "NO EXISTE")
    print("DEBUG ejemplo cvv (primeras 3):", df["cvv"].head(3).tolist() if "cvv" in df.columns else "NO EXISTE")

    logi("🔐 Anonimizando: mask + hash. Eliminando CVV...")
    df["numero_tarjeta_masked"] = df["numero_tarjeta"].apply(mask_card)
    df["numero_tarjeta_hash"] = df["numero_tarjeta"].apply(hash_value)

    # Nunca guardar sensibles en claro
    df.drop(columns=["numero_tarjeta", "cvv"], inplace=True)

    logi(f"✅ Tarjetas: filas={len(df)} | columnas={list(df.columns)}")
    return df


# ---------------- DB (LISTA PERO DESACTIVADA) ---------------- #

def test_db_connection() -> bool:
    if not ENABLE_DB:
        logw("DB desactivada (ENABLE_DB=False). No se intenta conexión.")
        return False

    logi("🔌 Probando conexión a MySQL...")
    try:
        engine = create_engine(DB_URL)
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        logi("✅ Conexión a MySQL OK (SELECT 1)")
        return True
    except Exception as e:
        logw(f"No hay conexión a BD: {e}")
        return False

def load_to_db(df: pd.DataFrame, table_name: str, db_ok: bool):
    if not db_ok:
        logw(f"SKIP BD: no se inserta en '{table_name}' (db_ok=False).")
        return
    if df is None or df.empty:
        logw(f"SKIP BD: '{table_name}' sin filas.")
        return

    try:
        engine = create_engine(DB_URL)
        with engine.begin() as conn:
            conn.execute(text("SELECT 1"))
            df.to_sql(table_name, conn, if_exists="append", index=False)
        logi(f"✅ Insertadas {len(df)} filas en '{table_name}'")
    except SQLAlchemyError as e:
        logw(f"Error insertando en BD '{table_name}': {e}")


# ---------------- PIPELINE ---------------- #

def run_pipeline():
    logi("🚀 INICIO PIPELINE ETL")
    logi(f"📁 Rutas: input={os.path.abspath(INPUT_DIR)} output={os.path.abspath(OUTPUT_DIR)} errors={os.path.abspath(ERROR_DIR)}")
    logi(f"🧾 Patrones: {CLIENTES_PATTERN} | {TARJETAS_PATTERN}")

    if not os.path.exists(INPUT_DIR):
        loge(f"No existe la carpeta '{INPUT_DIR}'. Crea input/ y mete los CSV.")
        return

    files = os.listdir(INPUT_DIR)
    logi(f"📂 Archivos encontrados en 'input': {len(files)}")
    for f in files:
        logi(f"   - {f}")

    clientes_files = [f for f in files if re.match(CLIENTES_PATTERN, f)]
    tarjetas_files = [f for f in files if re.match(TARJETAS_PATTERN, f)]
    ignored = [f for f in files if f not in (clientes_files + tarjetas_files)]

    logi(f"🧾 Clientes válidos por patrón: {len(clientes_files)} -> {clientes_files}")
    logi(f"💳 Tarjetas válidos por patrón: {len(tarjetas_files)} -> {tarjetas_files}")

    if ignored:
        logw(f"Ficheros ignorados por nombre: {ignored}")
        logw("Recuerda: deben llamarse EXACTO Clientes-YYYY-MM-DD.csv / Tarjetas-YYYY-MM-DD.csv")

    if not clientes_files and not tarjetas_files:
        logw("No hay ficheros válidos. Fin.")
        return

    db_ok = test_db_connection()

    # --- CLIENTES ---
    for file in clientes_files:
        logi(f"\n🧾 Procesando CLIENTES: {file}")
        in_path = os.path.join(INPUT_DIR, file)

        df = load_csv(in_path)
        if df is None:
            loge(f"Saltando {file} (no se pudo leer)")
            continue

        df_clean, df_rej = process_clientes(df, file)
        if df_clean is None:
            loge(f"Saltando {file} (fallo columnas requeridas)")
            continue

        out_path = os.path.join(OUTPUT_DIR, file.replace(".csv", ".cleaned.csv"))
        df_clean.to_csv(out_path, index=False)
        logi(f"💾 Output clientes guardado: {out_path} | filas={len(df_clean)}")

        load_to_db(df_clean, "clientes", db_ok)

    # --- TARJETAS ---
    for file in tarjetas_files:
        logi(f"\n💳 Procesando TARJETAS: {file}")
        in_path = os.path.join(INPUT_DIR, file)

        df = load_csv(in_path)
        if df is None:
            loge(f"Saltando {file} (no se pudo leer)")
            continue

        df_clean = process_tarjetas(df, file)
        if df_clean is None:
            loge(f"Saltando {file} (fallo columnas requeridas)")
            continue

        out_path = os.path.join(OUTPUT_DIR, file.replace(".csv", ".cleaned.csv"))
        df_clean.to_csv(out_path, index=False)
        logi(f"💾 Output tarjetas guardado: {out_path} | filas={len(df_clean)}")

        load_to_db(df_clean, "tarjetas", db_ok)

    logi("\n📊 RESUMEN FINAL")
    logi(f"   - ENABLE_DB: {ENABLE_DB}")
    logi(f"   - Log file: {os.path.abspath(os.path.join(LOG_DIR, f'etl_{RUN_ID}.log'))}")
    logi("✅ FIN PIPELINE ETL")


# ---------------- MAIN ---------------- #

if __name__ == "__main__":
    run_pipeline()
