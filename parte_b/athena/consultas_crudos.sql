-- Trabajo 2 · Parte B · Consultas opcionales de los eventos crudos con Athena
-- Reemplace <BUCKET_B> por t2-streaming-<ACCOUNT_ID> y <LOTE> por el lote del
-- productor. Ejecute una sentencia a la vez. La base trabajo2 es la de la parte A.

-- 1. Base de datos (si no existe por la parte A)
CREATE DATABASE IF NOT EXISTS trabajo2;

-- 2. Una fila por línea de los archivos, sin interpretar el JSON: así caben
--    también las líneas malformadas, que un lector JSON rechazaría.
CREATE EXTERNAL TABLE IF NOT EXISTS trabajo2.eventos_crudos (
  linea string
)
PARTITIONED BY (fecha string, hora string)
ROW FORMAT DELIMITED
STORED AS TEXTFILE
LOCATION 's3://<BUCKET_B>/crudos/';

-- 3. Registrar las carpetas fecha=/hora= como particiones
MSCK REPAIR TABLE trabajo2.eventos_crudos;

-- 4. Eventos crudos del lote: debe ser igual a los publicados
SELECT count(*) AS eventos_crudos
FROM trabajo2.eventos_crudos
WHERE linea LIKE '%<LOTE>%';

-- 5. Los repetidos siguen en los crudos: event_id que aparecen más de una vez
SELECT json_extract_scalar(linea, '$.event_id') AS event_id, count(*) AS veces
FROM trabajo2.eventos_crudos
WHERE linea LIKE '%<LOTE>%'
GROUP BY 1
HAVING count(*) > 1
ORDER BY veces DESC
LIMIT 20;

-- 6. Las líneas que ni siquiera son JSON válido (los inválidos E0)
SELECT linea
FROM trabajo2.eventos_crudos
WHERE linea LIKE '%<LOTE>%' AND try(json_parse(linea)) IS NULL;
