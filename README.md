# GRUPO-PLATA


proyecto-etl/
│
├── data/
│   ├── input/                # Aquí se dejan los CSV originales (Clientes-YYYY-MM-DD.csv, Tarjetas-YYYY-MM-DD.csv)
│   ├── output/               # CSV transformados (cleaned)
│   └── errors/               # Filas rechazadas + logs específicos
│
├── db/
│   ├── create_tables.sql     # Scripts DDL (crear tablas si no existen)
│   ├── insert_data.sql       # Opcional, si se usa SQL manual para inserts
│   └── docker/               # PostgreSQL/MySQL con docker-compose
│       └── docker-compose.yml
│
├── etl/
│   ├── extract.py            # Lectura de ficheros, filtrado por patrón, validaciones iniciales
│   ├── transform.py          # Limpieza, normalización, anonimizaciones, validaciones
│   ├── load.py               # Inserción en base de datos (SQLAlchemy)
│   ├── utils/                # Funciones compartidas
│   │   ├── validators.py     # DNI, email, teléfono
│   │   ├── anonymizer.py     # Hashing / enmascarado tarjetas y DNI
│   │   ├── logger.py         # Logging estructurado
│   │   └── helpers.py
│   └── config.py             # Variables globales, rutas, DB URI, SALT para hasheos (no subir)
│
├── pipeline/
│   ├── run_pipeline.py       # Ejecuta todo: extract → transform → load
│   └── scheduler/
│       ├── cron_example.txt  # Ejemplo de cron
│       └── github_actions.yml
│
├── tests/
│   ├── test_transform.py     # Tests de limpieza y normalización
│   ├── test_anonymizer.py    # Tests hashing/enmascarado
│   └── test_validators.py
│
├── docs/
│   ├── memoria_proyecto.pdf  # Memoria final
│   ├── arquitectura.png      # Diagrama del pipeline
│   ├── gantt.png             # Diagrama de Gantt
│   └── decisiones.md         # Justificación decisiones ETL/DB/Anonimización
│
├── .env.example              # Variables (SALTS, DB_URI) sin datos sensibles
├── .gitignore
├── requirements.txt          # Librerías del proyecto
├── README.md                 # Cómo ejecutar el pipeline, instalación, estructura
└── LICENSE (opcional)
