"""Trabajo 2 · Parte A · Rama 2: devoluciones por segmento de cliente.

Pregunta de analítica: ¿qué segmento de clientes (nivel de membresía y grupo
de ingresos) tiene la mayor tasa de devolución, cuánto dinero se devuelve y
cuál es su motivo de devolución más frecuente?

Usa columnas que solo existen gracias al join con customers
(membership_tier, income_group).

Parámetros del trabajo:
    --input_path   s3://<bucket>/curated/orders_enriched/
    --output_path  s3://<bucket>/analytics/devoluciones_segmento/
"""
import sys

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark import SparkContext
from pyspark.sql import Window
from pyspark.sql import functions as F

SEGMENT = ['membership_tier', 'income_group']

args = getResolvedOptions(sys.argv, ['JOB_NAME', 'input_path', 'output_path'])
glue = GlueContext(SparkContext.getOrCreate())
spark = glue.spark_session
job = Job(glue)
job.init(args['JOB_NAME'], args)

orders = spark.read.parquet(args['input_path'])
print(f'Registros leídos: {orders.count()}')

returned = F.col('order_status') == 'Returned'
by_segment = orders.groupBy(*SEGMENT).agg(
    F.countDistinct('customer_id').alias('customers'),
    F.count('*').alias('total_orders'),
    F.sum(F.when(returned, 1).otherwise(0)).alias('returned_orders'),
    F.round(
        100 * F.avg(F.when(returned, 1).otherwise(0)), 2).alias('return_rate_pct'),
    F.round(
        100 * F.avg(F.when(F.col('order_status') == 'Cancelled', 1).otherwise(0)),
        2).alias('cancel_rate_pct'),
    F.round(F.sum(F.when(returned, F.col('order_amount')).otherwise(0)),
            2).alias('returned_amount'),
    F.round(F.avg('customer_rating'), 2).alias('avg_customer_rating'),
)

reasons = orders.where(returned).groupBy(*SEGMENT, 'return_reason').count()
top_reason = reasons.withColumn(
    'position',
    F.row_number().over(
        Window.partitionBy(*SEGMENT).orderBy(F.desc('count'),
                                             'return_reason'))).where(
                                                 F.col('position') == 1).select(
                                                     *SEGMENT,
                                                     F.col('return_reason').alias(
                                                         'top_return_reason'),
                                                     F.col('count').alias(
                                                         'top_reason_orders'))

result = by_segment.join(top_reason, SEGMENT, 'left').withColumn(
    'rank_by_return_rate',
    F.rank().over(Window.orderBy(F.desc('return_rate_pct'))))
result = result.orderBy('rank_by_return_rate').coalesce(1)
result.write.mode('overwrite').parquet(args['output_path'])
print(f'Filas de la agregación: {result.count()}')

job.commit()
