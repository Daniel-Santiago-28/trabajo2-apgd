"""Trabajo 2 · Parte A · Trabajo de Glue inicial: limpieza, join y Parquet.

Lee las órdenes (CSV) y los clientes (CSV) desde S3, limpia las órdenes,
las une con los clientes por customer_id y escribe el resultado en S3 en
Parquet, particionado por order_year.

Parámetros del trabajo (Job parameters en la consola de Glue):
    --input_orders     s3://<bucket>/raw/orders/
    --input_customers  s3://<bucket>/raw/customers/
    --output_path      s3://<bucket>/curated/orders_enriched/
"""
import sys

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark import SparkContext
from pyspark.sql import functions as F

ORDER_COLUMNS = [
    'order_id', 'customer_id', 'order_date', 'product_category',
    'product_name', 'quantity', 'unit_price', 'discount_percent',
    'order_amount', 'payment_method', 'coupon_used', 'delivery_days',
    'order_status', 'returned', 'return_reason', 'seller_rating',
    'customer_rating'
]
# Todo se lee como texto: así un valor sucio no rompe la lectura y la
# limpieza decide qué hacer con él.
ORDERS_SCHEMA = ', '.join(f'{name} string' for name in ORDER_COLUMNS)

CUSTOMERS_SCHEMA = (
    'customer_id long, first_name string, last_name string, age int, '
    'gender string, city string, state string, country string, '
    'signup_date date, tenure_months int, membership_tier string, '
    'occupation string, income_group string, marital_status string, '
    'education string, preferred_device string, referral_source string')
# Columnas de clientes que se agregan a cada orden. Se omiten nombre y
# apellido: no aportan al análisis y son datos personales.
CUSTOMER_COLUMNS = [
    'customer_id', 'age', 'gender', 'city', 'state', 'tenure_months',
    'membership_tier', 'occupation', 'income_group', 'marital_status',
    'education', 'preferred_device', 'referral_source'
]

DOMAINS = {
    'product_category': ['Beauty', 'Electronics', 'Fashion', 'Grocery', 'Home'],
    'product_name': [
        'Cream', 'Perfume', 'Shampoo', 'Headphones', 'Laptop', 'Phone',
        'Jeans', 'Shoes', 'T-Shirt', 'Milk', 'Rice', 'Snacks', 'Chair', 'Lamp',
        'Table'
    ],
    'payment_method': ['COD', 'Credit Card', 'Debit Card', 'UPI', 'Wallet'],
    'coupon_used': ['Yes', 'No'],
    'order_status': ['Cancelled', 'Delivered', 'Returned'],
}
RETURN_REASONS = ['Changed Mind', 'Damaged', 'Late Delivery', 'Wrong Item']


def canonical(column, values):
    """Lleva un texto a su forma oficial sin importar espacios ni mayúsculas.

    '  beauty ' -> 'Beauty', 'DEBIT CARD' -> 'Debit Card'. Si el valor no
    está en el dominio, devuelve null.
    """
    key = F.lower(F.trim(F.col(column)))
    result = None
    for value in values:
        condition = key == value.lower()
        result = (F.when(condition, value) if result is None else result.when(
            condition, value))
    return result


def try_cast(column, kind):
    """Convierte sin fallar: si el texto no es válido devuelve null.

    Glue 6.0 (Spark 4) trae el modo ANSI activo, en el que un cast() de un
    texto inválido detiene el trabajo con un error; try_cast no.
    """
    return F.expr(f'try_cast(trim({column}) AS {kind})')


args = getResolvedOptions(
    sys.argv, ['JOB_NAME', 'input_orders', 'input_customers', 'output_path'])
glue = GlueContext(SparkContext.getOrCreate())
spark = glue.spark_session
job = Job(glue)
job.init(args['JOB_NAME'], args)

# 1. Lectura ----------------------------------------------------------------
raw = spark.read.csv(args['input_orders'], header=True, schema=ORDERS_SCHEMA)
customers = spark.read.csv(args['input_customers'],
                           header=True,
                           schema=CUSTOMERS_SCHEMA)
raw.cache()
read_count = raw.count()
print(f'[1] Registros leídos de orders: {read_count}')
print(f'[1] Registros leídos de customers: {customers.count()}')

# 2. Duplicados exactos -----------------------------------------------------
unique = raw.dropDuplicates()
unique.cache()
unique_count = unique.count()
print(f'[2] Duplicados exactos descartados: {read_count - unique_count}')

# 3. Tipos y textos normalizados (corrige; lo que no se puede leer queda
#    null y se descarta en el paso 4) ---------------------------------------
dirty_text = unique.where(
    ~F.col('product_category').isin(DOMAINS['product_category'])
    | ~F.col('payment_method').isin(DOMAINS['payment_method'])).count()
print(f'[3] Registros con textos por normalizar (espacios, mayúsculas): '
      f'{dirty_text}')
typed = unique.select(
    try_cast('order_id', 'BIGINT').alias('order_id'),
    try_cast('customer_id', 'BIGINT').alias('customer_id'),
    F.to_date(F.try_to_timestamp(F.trim('order_date'),
                                 F.lit('yyyy-MM-dd'))).alias('order_date'),
    canonical('product_category',
              DOMAINS['product_category']).alias('product_category'),
    canonical('product_name', DOMAINS['product_name']).alias('product_name'),
    try_cast('quantity', 'INT').alias('quantity'),
    try_cast('unit_price', 'DOUBLE').alias('unit_price'),
    try_cast('discount_percent', 'INT').alias('discount_percent'),
    try_cast('order_amount', 'DOUBLE').alias('order_amount_raw'),
    canonical('payment_method',
              DOMAINS['payment_method']).alias('payment_method'),
    canonical('coupon_used', DOMAINS['coupon_used']).alias('coupon_used'),
    try_cast('delivery_days', 'INT').alias('delivery_days'),
    canonical('order_status', DOMAINS['order_status']).alias('order_status'),
    canonical('return_reason', RETURN_REASONS).alias('return_reason'),
    try_cast('seller_rating', 'DOUBLE').alias('seller_rating'),
    try_cast('customer_rating', 'INT').alias('customer_rating'),
)

# 4. Reglas de validez: cada registro que no cumple se descarta, y se cuenta
#    el primer motivo por el que falla ---------------------------------------
RULES = [
    ('order_id vacío o no numérico', F.col('order_id').isNull()),
    ('customer_id vacío o no numérico', F.col('customer_id').isNull()),
    ('order_date vacía o inválida', F.col('order_date').isNull()),
    ('categoría o producto fuera del catálogo',
     F.col('product_category').isNull() | F.col('product_name').isNull()),
    ('quantity fuera de 1 a 5', ~F.coalesce(
        F.col('quantity').between(1, 5), F.lit(False))),
    ('unit_price no positivo', ~F.coalesce(
        F.col('unit_price') > 0, F.lit(False))),
    ('discount_percent fuera de 0 a 100', ~F.coalesce(
        F.col('discount_percent').between(0, 100), F.lit(False))),
    ('payment_method, coupon_used u order_status fuera del dominio',
     F.col('payment_method').isNull() | F.col('coupon_used').isNull()
     | F.col('order_status').isNull()),
]
reason = None
for name, failed in RULES:
    reason = (F.when(failed, name) if reason is None else reason.when(
        failed, name))
checked = typed.withColumn('invalid_reason', reason)
print('[4] Registros descartados por regla:')
for row in (checked.where(F.col('invalid_reason').isNotNull()).groupBy(
        'invalid_reason').count().orderBy('invalid_reason').collect()):
    print(f"      {row['invalid_reason']}: {row['count']}")

# 5. Correcciones ----------------------------------------------------------
# order_amount debe ser quantity * unit_price * (1 - descuento). Una
# diferencia de centavos es redondeo; una de más de 1 es un error y se
# reemplaza por el valor calculado.
recomputed = F.round(
    F.col('quantity') * F.col('unit_price') *
    (1 - F.col('discount_percent') / 100), 2)
mismatch = (F.col('order_amount_raw').isNull()
            | (F.abs(F.col('order_amount_raw') - recomputed) > 1))
valid = checked.where(F.col('invalid_reason').isNull()).drop('invalid_reason')
mismatch_count = valid.where(mismatch).count()
print(f'[5] order_amount corregidos (no cuadraban): {mismatch_count}')
clean = (valid.withColumn(
    'order_amount',
    F.when(mismatch, recomputed).otherwise(F.col('order_amount_raw'))
).withColumn(
    'returned',
    F.when(F.col('order_status') == 'Returned', 'Yes').otherwise('No')
).withColumn(
    'return_reason',
    F.when(F.col('order_status') == 'Returned',
           F.coalesce('return_reason', F.lit('Not Specified')))).drop(
               'order_amount_raw'))

# 6. Duplicados por order_id (misma orden con contenido distinto) -----------
clean = clean.dropDuplicates(['order_id'])
clean.cache()
clean_count = clean.count()
print(f'[6] Registros válidos después de limpiar: {clean_count}')

# 7. Join con clientes ------------------------------------------------------
customers = customers.select(*CUSTOMER_COLUMNS).dropDuplicates(['customer_id'])
orphans = clean.join(customers, 'customer_id', 'left_anti').count()
print(f'[7] Órdenes sin cliente en customers (descartadas): {orphans}')
enriched = clean.join(customers, 'customer_id', 'inner').withColumn(
    'order_year', F.year('order_date'))

# 8. Escritura en Parquet particionado por order_year -----------------------
(enriched.repartition('order_year').write.mode('overwrite').partitionBy(
    'order_year').parquet(args['output_path']))
written = spark.read.parquet(args['output_path'])
print(f'[8] Registros escritos en Parquet: {written.count()}')
for row in written.groupBy('order_year').count().orderBy(
        'order_year').collect():
    print(f"      order_year={row['order_year']}: {row['count']}")

job.commit()
