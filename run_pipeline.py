# ---------------- IMPORTS ---------------- #

import os              # Manejo de rutas y directorios
import re              # Expresiones regulares para validaciones y patrones
import hashlib         # Hashing seguro (SHA-256)
import logging         # Sistema de logging para registrar el ETL
import unicodedata     # Normalización de texto (eliminar acentos)
from datetime import datetime  # Manejo de fechas (no se usa en este script)

import pandas as pd    # Manipulación de datos
from sqlalchemy import create_engine, text  # Conexión y carga a base de datos


# ---------------- CONFIGURACIÓN ---------------- #

INPUT_DIR = "input"     # Carpeta de entrada de archivos CSV
OUTPUT_DIR = "output"   # Carpeta donde se guardan los CSV procesados
ERROR_DIR = "errors"    # Carpeta donde se guardan los registros rechazados
LOG_DIR = "logs"        # Carpeta donde se guardan los logs

DB_URL = "mysql://grupo_plata_user:Plata123!@localhost:3307/etl_db"  # Conexión a PostgreSQL
SALT = "MI_SALT_SECRETA"  # Sal para hashing seguro

# Crear directorios si no existen
os.makedirs(OUTPUT_DIR, exist_ok=True)
os.makedirs(ERROR_DIR, exist_ok=True)
os.makedirs(LOG_DIR, exist_ok=True)


# ---------------- LOGGING ---------------- #

logging.basicConfig(
    filename=f"{LOG_DIR}/etl.log",  # Archivo de log
    level=logging.INFO,             # Nivel de detalle
    format="%(asctime)s - %(levelname)s - %(message)s"  # Formato del log
)


# ---------------- UTILIDADES ---------------- #

def remove_accents(text):
    # Elimina acentos y caracteres diacríticos
    if pd.isna(text):
        return None
    return ''.join(
        c for c in unicodedata.normalize('NFD', text)
        if unicodedata.category(c) != 'Mn'
    )

def clean_text(text):
    # Limpieza general: elimina acentos y espacios
    if pd.isna(text):
        return None
    text = remove_accents(text)
    return text.strip()

def validate_email(email):
    # Valida formato básico de correo electrónico
    if not email:
        return False
    pattern = r"^[\w\.-]+@[\w\.-]+\.\w+$"
    return re.match(pattern, email) is not None

def validate_phone(phone):
    # Valida que el teléfono tenga solo dígitos y mínimo 9 caracteres
    if not phone:
        return False
    return phone.isdigit() and len(phone) >= 9

def validate_dni(dni):
    # Valida DNI español (8 números + letra correcta)
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
    # Genera hash SHA-256 con sal
    return hashlib.sha256((SALT + value).encode()).hexdigest()

def mask_card(card):
    # Enmascara tarjeta mostrando solo los últimos 4 dígitos
    card = re.sub(r"\D", "", card)  # Elimina caracteres no numéricos
    return "XXXX-XXXX-XXXX-" + card[-4:]


# ---------------- CARGA DE CSV ---------------- #

def load_csv(file_path):
    # Carga CSV con separador ; y todo como texto
    return pd.read_csv(
        file_path,
        sep=";",
        dtype=str,
        encoding="utf-8",
        on_bad_lines="skip"  # Ignora líneas corruptas
    )


# ---------------- ETL CLIENTES ---------------- #

def process_clientes(df):
    rejected = []  # Lista para almacenar filas rechazadas

    df = df.applymap(clean_text)  # Limpieza general de texto

    df["correo"] = df["correo"].str.lower()  # Normaliza correos a minúsculas

    # Validación de DNI
    df["DNI_OK"] = df["dni"].apply(validate_dni).map({True: "Y", False: "N"})
    df["DNI_KO"] = df["DNI_OK"].map({"Y": "N", "N": "Y"})

    # Validación de teléfono
    df["Telefono_OK"] = df["telefono"].apply(validate_phone).map({True: "Y", False: "N"})
    df["Telefono_KO"] = df["Telefono_OK"].map({"Y": "N", "N": "Y"})

    # Validación de correo
    df["Correo_OK"] = df["correo"].apply(validate_email).map({True: "Y", False: "N"})
    df["Correo_KO"] = df["Correo_OK"].map({"Y": "N", "N": "Y"})

    # Recolectar filas rechazadas por correo inválido
    for idx, row in df.iterrows():
        if row["Correo_OK"] == "N":
            rejected.append(row)

    # Filas válidas
    df_valid = df[df["Correo_OK"] == "Y"]

    # Filas rechazadas
    df_rejected = pd.DataFrame(rejected)

    # Guardar rechazados si existen
    if not df_rejected.empty:
        df_rejected.to_csv(f"{ERROR_DIR}/rows_rejected.csv", index=False)

    return df_valid


# ---------------- ETL TARJETAS ---------------- #

def process_tarjetas(df):
    df = df.applymap(clean_text)  # Limpieza general

    # Enmascarado y hashing de tarjeta
    df["numero_tarjeta_masked"] = df["numero_tarjeta"].apply(mask_card)
    df["numero_tarjeta_hash"] = df["numero_tarjeta"].apply(hash_value)

    # Eliminación de datos sensibles
    df.drop(columns=["numero_tarjeta", "cvv"], inplace=True)

    return df


# ---------------- CARGA A BASE DE DATOS ---------------- #

def load_to_db(df, table_name):
    engine = create_engine(DB_URL)  # Crear engine de conexión

    # Inserción en la tabla correspondiente
    with engine.begin() as conn:
        df.to_sql(table_name, conn, if_exists="append", index=False)


# ---------------- PIPELINE PRINCIPAL ---------------- #

def run_pipeline():
    logging.info("Inicio del pipeline ETL")

    # Recorrer archivos del directorio de entrada
    for file in os.listdir(INPUT_DIR):

        # Procesar clientes
        if re.match(r"Clientes-\d{4}-\d{2}-\d{2}\.csv", file):
            logging.info(f"Procesando {file}")
            df = load_csv(f"{INPUT_DIR}/{file}")
            df_clean = process_clientes(df)
            output_file = f"{OUTPUT_DIR}/{file.replace('.csv', '.cleaned.csv')}"
            df_clean.to_csv(output_file, index=False)
            load_to_db(df_clean, "clientes")

        # Procesar tarjetas
        elif re.match(r"Tarjetas-\d{4}-\d{2}-\d{2}\.csv", file):
            logging.info(f"Procesando {file}")
            df = load_csv(f"{INPUT_DIR}/{file}")
            df_clean = process_tarjetas(df)
            output_file = f"{OUTPUT_DIR}/{file.replace('.csv', '.cleaned.csv')}"
            df_clean.to_csv(output_file, index=False)
            load_to_db(df_clean, "tarjetas")

        # Ignorar otros archivos
        else:
            logging.warning(f"Fichero ignorado: {file}")

    logging.info("Fin del pipeline ETL")


# ---------------- EJECUCIÓN ---------------- #

if _name_ == "_main_":
    run_pipeline()  # Ejecuta el pipeline completo
