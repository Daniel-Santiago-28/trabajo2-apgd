"""Trabajo 2 · Parte A · Rama 1: ingresos por categoría y año.

Pregunta de analítica: ¿qué categoría de producto genera más ingresos netos
(órdenes entregadas) cada año, y qué parte del ingreso del año representa?

Parámetros del trabajo:
    --input_path   s3://<bucket>/curated/orders_enriched/
    --output_path  s3://<bucket>/analytics/ventas_categoria_anio/
"""
import sys

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark import SparkContext
from pyspark.sql import Window
from pyspark.sql import functions as F

args = getResolvedOptions(sys.argv, ['JOB_NAME', 'input_path', 'output_path'])
glue = GlueContext(SparkContext.getOrCreate())
spark = glue.spark_session
job = Job(glue)
job.init(args['JOB_NAME'], args)

orders = spark.read.parquet(args['input_path'])
print(f'Registros leídos: {orders.count()}')

delivered = F.col('order_status') == 'Delivered'
by_group = orders.groupBy('order_year', 'product_category').agg(
    F.count('*').alias('total_orders'),
    F.sum(F.when(delivered, 1).otherwise(0)).alias('delivered_orders'),
    F.round(F.sum(F.when(delivered, F.col('order_amount')).otherwise(0)),
            2).alias('net_revenue'),
    F.round(F.avg(F.when(delivered, F.col('order_amount'))),
            2).alias('avg_ticket'),
)
year = Window.partitionBy('order_year')
result = by_group.withColumn(
    'revenue_share_pct',
    F.round(100 * F.col('net_revenue') / F.sum('net_revenue').over(year),
            2)).withColumn(
                'rank_in_year',
                F.rank().over(year.orderBy(F.desc('net_revenue'))))

result = result.orderBy('order_year', 'rank_in_year').coalesce(1)
result.write.mode('overwrite').parquet(args['output_path'])
print(f'Filas de la agregación: {result.count()}')

job.commit()
