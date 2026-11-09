"""
Trabajo 2 · Parte B · Lambda t2-crudos-s3 (segundo consumidor del stream).

Flujo: productor.py -> Kinesis (t2-eventos-ordenes) -> esta Lambda -> S3

Lee el mismo stream que t2-ventanas, de forma independiente (cada disparador
lleva su propia posición en el stream), y guarda en S3 todos los eventos tal
como llegan: sin validar, sin quitar repetidos ni inválidos. Cada lote de
registros que entrega Lambda se escribe como un archivo JSON Lines (un evento
por línea) en:

    s3://<BUCKET>/crudos/fecha=AAAA-MM-DD/hora=HH/<llegada>-<shard>-<primer seq>.jsonl

El nombre usa el número de secuencia del primer registro: si Lambda reintenta
el mismo lote, sobrescribe el mismo archivo en vez de crear otro.

Variables de entorno: BUCKET, PREFIJO (por defecto crudos/)
"""
import base64
import json
import os
from datetime import datetime, timezone

import boto3

s3 = boto3.client("s3")
BUCKET = os.environ["BUCKET"]
PREFIJO = os.environ.get("PREFIJO", "crudos/")


def lambda_handler(event, context):
    registros = event.get("Records", [])
    if not registros:
        return {"lineas": 0}

    lineas = []
    for record in registros:
        datos = base64.b64decode(record["kinesis"]["data"])
        # El productor termina cada evento en salto de línea; si alguno no lo
        # trae, se agrega para que cada evento quede en su propia línea.
        lineas.append(datos if datos.endswith(b"\n") else datos + b"\n")

    primero = registros[0]
    llegada = datetime.fromtimestamp(primero["kinesis"]["approximateArrivalTimestamp"], tz=timezone.utc)
    shard = primero["eventID"].split(":")[0]          # "shardId-000000000000:4959..."
    clave = (f"{PREFIJO}fecha={llegada:%Y-%m-%d}/hora={llegada:%H}/"
             f"{llegada:%Y%m%dT%H%M%S}-{shard}-{primero['kinesis']['sequenceNumber']}.jsonl")

    s3.put_object(Bucket=BUCKET, Key=clave, Body=b"".join(lineas),
                  ContentType="application/x-ndjson")
    print(json.dumps({"archivo": f"s3://{BUCKET}/{clave}", "lineas": len(lineas)}))
    return {"lineas": len(lineas)}
