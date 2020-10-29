-----------------------------------------------
--Crear 3 base de datos: landing, operacional, business
--En landign formato TEXTFILE, en operacional cliente,empresa-avro, transacciones-parquet
--Trabajo Grupal
--Aporte de Luis Alberto Holgado Apaza
------------------------------------------------------------
--------------Bases de datos
CREATE DATABASE IF NOT EXISTS LANDING
COMMENT 'Base de datos landing'
LOCATION '/datalake/dbshive/landing';
------------------------------------------------------------
--CREAMOS LA TABLA CLIENTE_TEXTFILE
CREATE EXTERNAL TABLE IF NOT EXISTS LANDING.CLIENTE_TEXTFILE(
ID_CLIENTE STRING,
NOMBRE STRING,
TELEFONO STRING,
CORREO STRING,
FECHA_INGRESO STRING,
EDAD INT,
SALARIO DOUBLE,
ID_EMPRESA STRING
)
COMMENT 'Tabla de clientes formato text file'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
LINES TERMINATED BY '\n'
STORED AS TEXTFILE
TBLPROPERTIES('creator'='Luis Holgado','created_at'='2020-10-24','skip.header.line.count'='1');
--CARGAMOS A LA DATABASE
LOAD DATA LOCAL INPATH 'data/cliente.data' INTO TABLE LANDING.CLIENTE_TEXTFILE;

--CREAMOS LA TABLA EMPRESA_TEXTFILE
CREATE EXTERNAL TABLE IF NOT EXISTS LANDING.EMPRESA_TEXTFILE(
ID_EMPRESA STRING,
NOMBRE STRING
)
COMMENT 'Tabla de Empresa formato text file'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
LINES TERMINATED BY '\n'
STORED AS TEXTFILE
TBLPROPERTIES('creator'='Luis Holgado','created_at'='2020-10-28','skip.header.line.count'='1');
--CARGAMOS A el fichero plano a la tabla empresa_textfile
LOAD DATA LOCAL INPATH 'data/empresa.data' INTO TABLE LANDING.EMPRESA_TEXTFILE;

--CREAMOS LA TABLA TRANSACCION_TEXTFILE
CREATE EXTERNAL TABLE IF NOT EXISTS LANDING.TRANSACCIONES_TEXTFILE(
ID_CLIENTE STRING,
ID_EMPRESA STRING,
MONTO DOUBLE,
FECHA STRING
)
COMMENT 'Tabla de Transacciones formato text file'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
LINES TERMINATED BY '\n'
STORED AS TEXTFILE
TBLPROPERTIES('creator'='Luis Holgado','created_at'='2020-10-28','skip.header.line.count'='1');
--CARGAMOS el fichro plano transacciones.data a la tabla empresa_textfile
LOAD DATA LOCAL INPATH 'data/transacciones.data' INTO TABLE LANDING.TRANSACCIONES_TEXTFILE;

-----------------------------------------
--Creacion de la base de datos operacional

CREATE DATABASE IF NOT EXISTS OPERACIONAL
COMMENT 'Base de datos operacional'
LOCATION '/datalake/dbshive/operacional';

---------------------------------------------------------------------------
-- CREAMOS EL SCHEMA Y LOS DIRECTORIOS PARA LOS ACHIVOS AVRO
hdfs dfs -mkdir -p /datalake/schema/operacional
hdfs dfs -put data/cliente_avro.avsc /datalake/schema/operacional

-------------------------------------------------------------------------
--CREAMOS LA TABLA CLIENTE AVRO COMPRESSION SNAPPY;
CREATE EXTERNAL TABLE IF NOT EXISTS OPERACIONAL.CLIENTE_AVRO_SNAPPY
COMMENT 'Tabla de clientes formato AVRO SNAPPY'
STORED AS AVRO
LOCATION '/datalake/dbshive/operacional/cliente_avro_snappy'
TBLPROPERTIES(
'avro.schema.url'='hdfs:///datalake/schema/operacional/cliente_avro.avsc',
'avro.output.codec'='snnapy'
);
------------
--Seteat valores
SET hive.exec.compress.output=true;
SET avro.output.codec=snappy;

--insertamos la data de la tabla textfile en el CLIENTE_AVRO_SNAPPY
INSERT OVERWRITE TABLE OPERACIONAL.CLIENTE_AVRO_SNAPPY
SELECT * FROM LANDING.CLIENTE_TEXTFILE 
WHERE 
ID_CLIENTE!='ID_CLIENTE';


---------------------------------------------------------------------------
-- CREAMOS EL SCHEMA Y LOS DIRECTORIOS PARA LOS ACHIVOS AVRO
hdfs dfs -mkdir -p /datalake/schema/operacional
hdfs dfs -put data/empresa_avro.avsc /datalake/schema/operacional

-------------------------------------------------------------------------
--CREAMOS LA TABLA EMPRESA AVRO COMPRESSION SNAPPY;
CREATE EXTERNAL TABLE IF NOT EXISTS OPERACIONAL.EMPRESA_AVRO_SNAPPY
COMMENT 'Tabla de EMPRESA formato AVRO SNAPPY'
STORED AS AVRO
LOCATION '/datalake/dbshive/operacional/empresa_avro_snappy'
TBLPROPERTIES(
'avro.schema.url'='hdfs:///datalake/schema/operacional/empresa_avro.avsc',
'avro.output.codec'='snnapy'
);
----------------------------------------------------------------------------
--Seteat valores
SET hive.exec.compress.output=true;
SET avro.output.codec=snappy;

--insertamos la data de la tabla textfile en el CLIENTE_AVRO_SNAPPY
INSERT OVERWRITE TABLE OPERACIONAL.EMPRESA_AVRO_SNAPPY
SELECT * FROM LANDING.EMPRESA_TEXTFILE 
WHERE 
ID_EMPRESA!='ID_EMPRESA';

---------------------------------------------------------------------------
-- CREAMOS EL SCHEMA Y LOS DIRECTORIOS PARA LOS ACHIVOS AVRO
hdfs dfs -mkdir -p /datalake/schema/operacional
hdfs dfs -put data/transacciones_avro.avsc /datalake/schema/operacional

-------------------------------------------------------------------------
--CREAMOS LA TABLA EMPRESA AVRO COMPRESSION SNAPPY;
CREATE EXTERNAL TABLE IF NOT EXISTS OPERACIONAL.TRANSACCIONES_AVRO_SNAPPY
COMMENT 'Tabla de TRANSACCIONES_AVRO_SNAPPY formato AVRO SNAPPY'
STORED AS AVRO
LOCATION '/datalake/dbshive/operacional/transacciones_avro_snappy'
TBLPROPERTIES(
'avro.schema.url'='hdfs:///datalake/schema/operacional/transacciones_avro.avsc',
'avro.output.codec'='snnapy'
);
----------------------------------------------------------------------------
--Seteat valores
SET hive.exec.compress.output=true;
SET avro.output.codec=snappy;

--insertamos la data de la tabla textfile en el TRANSACCIONES_AVRO_SNAPPY
INSERT OVERWRITE TABLE OPERACIONAL.TRANSACCIONES_AVRO_SNAPPY
SELECT * FROM LANDING.TRANSACCIONES_TEXTFILE 
WHERE 
ID_EMPRESA!='ID_EMPRESA' AND ID_EMPRESA!='ID_EMPRESA';

----------------------------------------------------------------------------
--CREACION DE LA BASE DE DATOS BUSINESS


CREATE DATABASE IF NOT EXISTS BUSINESS
COMMENT 'Base de datos business'
LOCATION '/datalake/dbshive/business';

---------------------------------------------------------------------------
--Creacion de la tabla historica transaccion

CREATE EXTERNAL TABLE IF NOT EXISTS BUSINESS.HIS_TRANSACCONES_PARQUET_SNAPPY(
ID_CLIENTE STRING,
NOMBRE_CLIENTE STRING,
TELEFONO_CLIENTE STRING,
CORREO_CLIENTE STRING,
FECHA_INGRESO STRING,
EDAD_CLIENTE INT,
SALARIO_CLIENTE DOUBLE,
ID_EMPRESA STRING,
NOMBRE_EMPRESA STRING,
MONTO_TRANSACCION DOUBLE,
FECHA_TRANSACCION STRING
)
COMMENT 'Tabla de HIST_TRANSACCIONES formato PARQUET-COMPRESION SNAPPY'
STORED AS PARQUET
TBLPROPERTIES('creator'='Luis Holgado','created_at'='2020-10-29','parquet.compression'='SNAPPY');
--
--Debemos seteriar los parametros
SET hive,exec.compress.output=true;
SET parquet.compression=SNAPPY;

--insertamos la data de la tabla textfile en el HIS_TRANSACCONES_PARQUET_SNAPPY
INSERT OVERWRITE TABLE BUSINESS.HIS_TRANSACCONES_PARQUET_SNAPPY
SELECT 
C.ID_CLIENTE,
C.NOMBRE,
C.TELEFONO,
C.CORREO,
C.FECHA_INGRESO,
CAST(C.EDAD AS INT),
CAST(C.SALARIO AS DOUBLE),
E.ID_EMPRESA,
E.NOMBRE,
CAST(T.MONTO AS DOUBLE),
T.FECHA
FROM OPERACIONAL.TRANSACCIONES_AVRO_SNAPPY T
JOIN OPERACIONAL.EMPRESA_AVRO_SNAPPY E ON T.ID_EMPRESA=E.ID_EMPRESA
JOIN OPERACIONAL.CLIENTE_AVRO_SNAPPY C ON C.ID_CLIENTE=T.ID_CLIENTE;

-------------------------------------------------------------------------
--Consultas y vistas
SELECT UPPER(NOMBRE_CLIENTE) AS CLIENTE,SALARIO_CLIENTE, ROUND(SALARIO_CLIENTE*0.12) IMPORTE_AFP
FROM BUSINESS.HIS_TRANSACCONES_PARQUET_SNAPPY LIMIT 5;
SET hive.map.aggr=true;

SELECT 
COUNT(*) as NRO_CLI, 
AVG(SALARIO_CLIENTE) SALARIO_PROMEDIO 
FROM BUSINESS.HIS_TRANSACCONES_PARQUET_SNAPPY LIMIT 10;

SELECT COUNT(*) FROM BUSINESS.HIS_TRANSACCONES_PARQUET_SNAPPY LIMIT 10;

-- consultas con sentencias CASE WHEN
SELECT 
COUNT(*) AS CTD_CLIENTE,
CASE
WHEN SALARIO_CLIENTE < 11708 THEN 'DEBAJO DEL PROMEDIO'
ELSE 'SUPERIOR AL PROMEDIO'
END RANGO_SUELDO,
AVG(SALARIO_CLIENTE) PROMEDIO_SUELDO
FROM
BUSINESS.HIS_TRANSACCONES_PARQUET_SNAPPY
GROUP BY 
CASE
WHEN SALARIO_CLIENTE < 11708 THEN 'DEBAJO DEL PROMEDIO'
ELSE 'SUPERIOR AL PROMEDIO'
END; 

-- creando vistas en HIVE
CREATE VIEW VENTAS_EMPRESA_1 AS
SELECT 
NOMBRE_EMPRESA,
COUNT(*) AS CTD,
ROUND(SUM(MONTO_TRANSACCION)) MONTO_TRANSACCION
FROM
BUSINESS.HIS_TRANSACCONES_PARQUET_SNAPPY
GROUP BY 
NOMBRE_EMPRESA;

SELECT * FROM BUSINESS.VENTAS_EMPRESA_1;
-----------------------------------------------------------
--------------Grabamos

INSERT OVERWRITE LOCAL DIRECTORY '/home/luis/data'
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
SELECT * FROM BUSINESS.HIS_TRANSACCONES_PARQUET_SNAPPY WHERE SALARIO_CLIENTE < 11708;














