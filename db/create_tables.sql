CREATE TABLE IF NOT EXISTS clientes (
    cod_cliente VARCHAR(10) PRIMARY KEY,
    nombre VARCHAR(100),
    apellido1 VARCHAR(100),
    apellido2 VARCHAR(100),
    dni_hash VARCHAR(256),
    dni_ok CHAR(1),
    dni_ko CHAR(1),
    correo VARCHAR(150),
    correo_ok CHAR(1),
    correo_ko CHAR(1),
    telefono VARCHAR(20),
    telefono_ok CHAR(1),
    telefono_ko CHAR(1),
    fecha_carga DATE
);
