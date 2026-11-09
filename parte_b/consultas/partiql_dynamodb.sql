-- Trabajo 2 · Parte B · Consultas PartiQL para la consola de DynamoDB
-- (DynamoDB → PartiQL editor). Ejecute una a la vez.
-- En PartiQL los nombres de tablas y atributos van entre comillas dobles
-- y los valores de texto entre comillas simples.

-- 1. Pregunta del proyecto, por categoría: órdenes y ticket promedio minuto a
--    minuto de Electronics. Filtra por la llave de partición, así que DynamoDB
--    hace un Query (lee solo esa partición). Cambie 'Electronics' por
--    'Home', 'Fashion', 'Beauty' o 'Grocery' para comparar.
SELECT "ventana", "hora_colombia", "conteo", "promedio_monto", "monto_minimo", "monto_maximo"
FROM "t2-ventanas"
WHERE "categoria" = 'Electronics';

-- 2. Todas las categorías de un mismo minuto (cambie la hora por una de la
--    consulta 1). No filtra por la llave de partición: es un Scan con filtro.
SELECT "categoria", "conteo", "promedio_monto"
FROM "t2-ventanas"
WHERE "ventana" = '2026-11-03T19:07:00Z';

-- 3. Eventos inválidos apartados, con su motivo
SELECT "seq", "motivo", "payload", "llegada"
FROM "t2-descartados"
WHERE "tipo" = 'INVALIDO';

-- 4. Eventos repetidos descartados: event_id, y el seq de la primera vez que llegó
SELECT "event_id", "seq", "seq_original", "llegada"
FROM "t2-descartados"
WHERE "tipo" = 'REPETIDO';

-- 5. Buscar un evento por su event_id (copie uno de la consulta 4): muestra la
--    primera llegada, la que sí se contó en su ventana.
SELECT *
FROM "t2-eventos-vistos"
WHERE "event_id" = '<EVENT_ID>';
