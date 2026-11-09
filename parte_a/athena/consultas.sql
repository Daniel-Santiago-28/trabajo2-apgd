-- Trabajo 2 · Parte A · Consultas en Amazon Athena
-- Ejecute una sentencia a la vez en el editor de consultas de Athena.
-- Reemplace aws-glue-t2-3207127784 por el nombre de su bucket (por ejemplo aws-glue-t2-123456789012).

-- 1. Base de datos del trabajo
CREATE DATABASE IF NOT EXISTS trabajo2;

-- 2. Tabla sobre los datos transformados (Parquet particionado por order_year)
CREATE EXTERNAL TABLE IF NOT EXISTS trabajo2.orders_enriched (
  customer_id bigint,
  order_id bigint,
  order_date date,
  product_category string,
  product_name string,
  quantity int,
  unit_price double,
  discount_percent int,
  payment_method string,
  coupon_used string,
  delivery_days int,
  order_status string,
  return_reason string,
  seller_rating double,
  customer_rating int,
  order_amount double,
  returned string,
  age int,
  gender string,
  city string,
  state string,
  tenure_months int,
  membership_tier string,
  occupation string,
  income_group string,
  marital_status string,
  education string,
  preferred_device string,
  referral_source string
)
PARTITIONED BY (order_year int)
STORED AS PARQUET
LOCATION 's3://aws-glue-t2-3207127784/curated/orders_enriched/';

-- 3. Registrar en el catálogo las particiones (carpetas order_year=...) que escribió Glue
MSCK REPAIR TABLE trabajo2.orders_enriched;

-- 4. Registros por partición: deben sumar 5.224.028
SELECT order_year, count(*) AS registros
FROM trabajo2.orders_enriched
GROUP BY order_year
ORDER BY order_year;

-- 5. Poda de particiones: compare "Data scanned" de esta consulta con la anterior
SELECT count(*) AS registros_2024
FROM trabajo2.orders_enriched
WHERE order_year = 2024;

-- 6. Tabla de la rama 1: ingresos por categoría y año
CREATE EXTERNAL TABLE IF NOT EXISTS trabajo2.ventas_categoria_anio (
  order_year int,
  product_category string,
  total_orders bigint,
  delivered_orders bigint,
  net_revenue double,
  avg_ticket double,
  revenue_share_pct double,
  rank_in_year int
)
STORED AS PARQUET
LOCATION 's3://aws-glue-t2-3207127784/analytics/ventas_categoria_anio/';

-- 7. Pregunta 1: ¿qué categoría genera más ingresos netos cada año, y qué parte del ingreso del año representa?
SELECT order_year, product_category, net_revenue, revenue_share_pct, delivered_orders, avg_ticket
FROM trabajo2.ventas_categoria_anio
WHERE rank_in_year = 1
ORDER BY order_year;

-- 8. Pregunta 1 (detalle): las 5 categorías de un año, de mayor a menor ingreso
SELECT order_year, rank_in_year, product_category, net_revenue, revenue_share_pct
FROM trabajo2.ventas_categoria_anio
WHERE order_year = 2025
ORDER BY rank_in_year;

-- 9. Tabla de la rama 2: devoluciones por segmento de cliente
CREATE EXTERNAL TABLE IF NOT EXISTS trabajo2.devoluciones_segmento (
  membership_tier string,
  income_group string,
  customers bigint,
  total_orders bigint,
  returned_orders bigint,
  return_rate_pct double,
  cancel_rate_pct double,
  returned_amount double,
  avg_customer_rating double,
  top_return_reason string,
  top_reason_orders bigint,
  rank_by_return_rate int
)
STORED AS PARQUET
LOCATION 's3://aws-glue-t2-3207127784/analytics/devoluciones_segmento/';

-- 10. Pregunta 2: ¿qué segmento (membresía e ingresos) devuelve más, cuánto dinero y por qué motivo?
SELECT rank_by_return_rate, membership_tier, income_group, total_orders, returned_orders,
       return_rate_pct, returned_amount, top_return_reason
FROM trabajo2.devoluciones_segmento
ORDER BY rank_by_return_rate, membership_tier, income_group;

-- 11. Pregunta 2 (diferencia entre el segmento que más y el que menos devuelve)
SELECT max(return_rate_pct) AS tasa_maxima,
       min(return_rate_pct) AS tasa_minima,
       round(max(return_rate_pct) - min(return_rate_pct), 2) AS diferencia_puntos
FROM trabajo2.devoluciones_segmento;
