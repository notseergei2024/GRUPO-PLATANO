
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

#  Para ahora (BD apagada): NO intentamos conectar.
ENABLE_DB = True

# Cuando activéis BD, usad esto (requiere pip install pymysql):
# DB_URL = "mysql+pymysql://grupo_plata_user:Plata123!@83.32.73.143:3306/etl_db"
DB_URL = "mysql+pymysql://grupo_plata_user:Plata123!@83.32.73.143:3306/grupo_plata"


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
    print(f" {msg}")
    logging.warning(msg)

def loge(msg: str):
    print(f" {msg}")
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
    logi(f" Leyendo CSV: {file_path}")

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
        logi(f" CSV cargado (utf-8) filas={len(df)} columnas={len(df.columns)}")
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
        logi(f" CSV cargado (latin-1) filas={len(df)} columnas={len(df.columns)}")
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
    logi("ETL Clientes: limpieza general (strip + quitar acentos)...")

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

    logi(" Validaciones: DNI / Teléfono / Correo...")
    df["DNI_OK"] = df["dni"].apply(validate_dni).map({True: "Y", False: "N"})
    df["DNI_KO"] = df["DNI_OK"].map({"Y": "N", "N": "Y"})

    df["Telefono_OK"] = df["telefono"].apply(validate_phone).map({True: "Y", False: "N"})
    df["Telefono_KO"] = df["Telefono_OK"].map({"Y": "N", "N": "Y"})

    df["Correo_OK"] = df["correo"].apply(validate_email).map({True: "Y", False: "N"})
    df["Correo_KO"] = df["Correo_OK"].map({"Y": "N", "N": "Y"})

    # Rechazados mínimos: correo inválido
    #  No rechazamos por correo: insertamos TODOS para no romper FK
    df_rejected = df[df["Correo_OK"] == "N"].copy()
    df_valid = df.copy()

    #  Hash dentro de la MISMA columna 'dni'
    df_valid["dni"] = df_valid["dni"].apply(hash_value)

    logi(f" Clientes: válidas={len(df_valid)} | rechazadas={len(df_rejected)}")

    if not df_rejected.empty:
        df_rejected["motivo_rechazo"] = "correo_invalido"
        rej_path = os.path.join(ERROR_DIR, f"rows_rejected_clientes_{RUN_ID}.csv")
        df_rejected.to_csv(rej_path, index=False)
        logw(f"Rechazados guardados en: {rej_path}")

    #  DataFrame EXACTO para BD (solo columnas reales)
    clientes_cols_db = ["cod_cliente", "nombre", "apellido1", "apellido2", "dni", "correo", "telefono"]
    df_db = df_valid[clientes_cols_db].copy()

    # df_valid puede mantener columnas extra (OK/KO) para el .cleaned.csv si quieres
    return df_db, df_rejected



# ---------------- ETL TARJETAS ---------------- #

def process_tarjetas(df: pd.DataFrame, source_file: str, valid_clientes: set[str]):

    logi("ETL Tarjetas: limpieza general (strip + quitar acentos)...")

    df = normalize_columns(df)

    required = ["cod_cliente", "numero_tarjeta", "fecha_exp", "cvv"]
    if not require_columns(df, required, source_file):
        return None

    df = df.astype("string").apply(lambda col: col.map(clean_text))
    print("DEBUG Tarjetas limpieza OK -> filas:", len(df), "columnas:", list(df.columns))
    print("DEBUG ejemplo numero_tarjeta (primeras 3):",
          df["numero_tarjeta"].head(3).tolist() if "numero_tarjeta" in df.columns else "NO EXISTE")
    print("DEBUG ejemplo cvv (primeras 3):", df["cvv"].head(3).tolist() if "cvv" in df.columns else "NO EXISTE")

    # Normaliza cod_cliente para comparar bien
    df["cod_cliente"] = df["cod_cliente"].astype("string").map(clean_text)

    #  Filtrado FK: solo tarjetas cuyo cod_cliente exista en clientes
    mask_ok = df["cod_cliente"].isin(valid_clientes)
    df_rejected = df[~mask_ok].copy()
    df_valid = df[mask_ok].copy()

    logi(f" Tarjetas: válidas={len(df_valid)} | rechazadas={len(df_rejected)} (FK cod_cliente->clientes)")

    if not df_rejected.empty:
        df_rejected["motivo_rechazo"] = "fk_cliente_no_existe"
        rej_path = os.path.join(ERROR_DIR, f"rows_rejected_tarjetas_{RUN_ID}.csv")
        df_rejected.to_csv(rej_path, index=False)
        logw(f"Rechazados guardados en: {rej_path}")

    # Hash dentro de las MISMAS columnas (sin añadir columnas)
    df_valid["numero_tarjeta"] = df_valid["numero_tarjeta"].apply(hash_value)
    df_valid["cvv"] = df_valid["cvv"].apply(hash_value)

    # DataFrame EXACTO para BD (solo columnas reales)
    tarjetas_cols_db = ["cod_cliente", "numero_tarjeta", "fecha_exp", "cvv"]
    df_db = df_valid[tarjetas_cols_db].copy()

    logi(f" Tarjetas (DB): filas={len(df_db)} | columnas={list(df_db.columns)}")
    return df_db

# ---------------- DB (LISTA PERO DESACTIVADA) ---------------- #

def test_db_connection(engine) -> bool:
    if not ENABLE_DB:
        logw("DB desactivada (ENABLE_DB=False). No se intenta conexión.")
        return False

    logi(" Probando conexión a MySQL...")
    try:
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        logi(" Conexión a MySQL OK (SELECT 1)")
        return True
    except Exception as e:
        logw(f"No hay conexión a BD: {e}")
        return False

def fetch_existing_client_codes(engine, db_ok: bool) -> set[str]:
    if not db_ok or engine is None:
        return set()
    try:
        with engine.connect() as conn:
            rows = conn.execute(text("SELECT cod_cliente FROM clientes")).fetchall()
        return {str(r[0]).strip() for r in rows if r[0] is not None}
    except Exception as e:
        logw(f"No se pudieron leer cod_cliente existentes de clientes: {e}")
        return set()


def load_to_db(df: pd.DataFrame, table_name: str, db_ok: bool, engine) -> bool:
    if not db_ok:
        logw(f"SKIP BD: no se inserta en '{table_name}' (db_ok=False).")
        return False
    if df is None or df.empty:
        logw(f"SKIP BD: '{table_name}' sin filas.")
        return False

    try:
        inserted_rows = []
        ignored_rows = []
        with engine.begin() as conn:
            for index, row in df.iterrows():
                query = text(f"""
                    INSERT IGNORE INTO {table_name} ({", ".join(row.index)})
                    VALUES ({", ".join([f":" + col for col in row.index])});
                """)
                result = conn.execute(query, row.to_dict())
                
                # Comprobar cuántas filas han sido insertadas
                if result.rowcount > 0:
                    inserted_rows.append(row.to_dict())
                else:
                    ignored_rows.append(row.to_dict())
        logi(f"Filas insertadas: {len(inserted_rows)}")
        logi(f"Filas ignoradas (duplicados): {len(ignored_rows)}")
        #logi(f"Ingreso procesado en '{table_name}', ignorando duplicados.")
        return True
    except SQLAlchemyError as e:
        loge(f"Error insertando en BD '{table_name}': {e}")
        return False


# ---------------- PIPELINE ---------------- #

def run_pipeline():
    logi(" INICIO PIPELINE ETL")
    logi(f"Rutas: input={os.path.abspath(INPUT_DIR)} output={os.path.abspath(OUTPUT_DIR)} errors={os.path.abspath(ERROR_DIR)}")
    logi(f"Patrones: {CLIENTES_PATTERN} | {TARJETAS_PATTERN}")

    if not os.path.exists(INPUT_DIR):
        loge(f"No existe la carpeta '{INPUT_DIR}'. Crea input/ y mete los CSV.")
        return

    files = os.listdir(INPUT_DIR)
    logi(f" Archivos encontrados en 'input': {len(files)}")
    for f in files:
        logi(f"   - {f}")

    clientes_files = [f for f in files if re.match(CLIENTES_PATTERN, f)]
    tarjetas_files = [f for f in files if re.match(TARJETAS_PATTERN, f)]
    ignored = [f for f in files if f not in (clientes_files + tarjetas_files)]

    logi(f" Clientes válidos por patrón: {len(clientes_files)} -> {clientes_files}")
    logi(f" Tarjetas válidos por patrón: {len(tarjetas_files)} -> {tarjetas_files}")

    if ignored:
        logw(f"Ficheros ignorados por nombre: {ignored}")
        logw("Recuerda: deben llamarse EXACTO Clientes-YYYY-MM-DD.csv / Tarjetas-YYYY-MM-DD.csv")

    if not clientes_files and not tarjetas_files:
        logw("No hay ficheros válidos. Fin.")
        return

    engine = create_engine(DB_URL) if ENABLE_DB else None
    db_ok = test_db_connection(engine) if engine else False
    valid_clientes = fetch_existing_client_codes(engine, db_ok)
    logi(f" Clientes existentes en BD: {len(valid_clientes)}")

    # --- CLIENTES ---
    for file in clientes_files:
        logi(f"\n Procesando CLIENTES: {file}")
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
        logi(f" Output clientes guardado: {out_path} | filas={len(df_clean)}")

        ok_insert = load_to_db(df_clean, "clientes", db_ok, engine)

        # Solo añadimos al set si REALMENTE se insertó en BD
        if ok_insert:
            valid_clientes.update(
                df_clean["cod_cliente"].dropna().astype(str).map(str.strip).tolist()
            )
        else:
            logw("No se actualiza valid_clientes porque el insert de CLIENTES falló.")

    #  Refrescamos desde BD para garantizar FK
    valid_clientes = fetch_existing_client_codes(engine, db_ok)
    logi(f" Clientes existentes en BD (refresco): {len(valid_clientes)}")

    # --- TARJETAS ---
    for file in tarjetas_files:
        logi(f"\n Procesando TARJETAS: {file}")
        in_path = os.path.join(INPUT_DIR, file)

        df = load_csv(in_path)
        if df is None:
            loge(f"Saltando {file} (no se pudo leer)")
            continue

        df_clean = process_tarjetas(df, file, valid_clientes)
        if df_clean is None:
            loge(f"Saltando {file} (fallo columnas requeridas)")
            continue

        out_path = os.path.join(OUTPUT_DIR, file.replace(".csv", ".cleaned.csv"))
        df_clean.to_csv(out_path, index=False)
        logi(f" Output tarjetas guardado: {out_path} | filas={len(df_clean)}")

        load_to_db(df_clean, "tarjetas", db_ok, engine)

    logi("\nRESUMEN FINAL")
    logi(f"   - ENABLE_DB: {ENABLE_DB}")
    logi(f"   - Log file: {os.path.abspath(os.path.join(LOG_DIR, f'etl_{RUN_ID}.log'))}")
    logi(" FIN PIPELINE ETL")


# ---------------- MAIN ---------------- #

if __name__ == "__main__":
    run_pipeline()
