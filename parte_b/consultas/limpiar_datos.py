"""
Trabajo 2 · Parte B · Vacía las tablas y los eventos crudos de las pruebas.

Uso (en AWS CloudShell), antes de la ejecución que se graba en el video:
    python3 limpiar_datos.py --bucket t2-streaming-<ACCOUNT_ID>

Borra todos los ítems de t2-ventanas, t2-eventos-vistos y t2-descartados, y
los objetos bajo crudos/ del bucket. No borra las tablas, el bucket, el stream
ni las Lambdas. Pide confirmación antes de borrar.

Los eventos que sigan en el stream (se retienen 24 horas) no se vuelven a
procesar: cada disparador ya avanzó su posición más allá de ellos.
"""
import argparse
import os

import boto3

# CloudShell define AWS_REGION con la región elegida en la consola
REGION = os.environ.get("AWS_REGION", "us-east-2")
TABLAS = {  # tabla: atributos de la llave primaria
    "t2-ventanas": ["categoria", "ventana"],
    "t2-eventos-vistos": ["event_id"],
    "t2-descartados": ["seq"],
}


def vaciar_tabla(dynamodb, nombre, llave):
    tabla = dynamodb.Table(nombre)
    borrados, kwargs = 0, {"ProjectionExpression": ", ".join(f"#k{i}" for i in range(len(llave))),
                           "ExpressionAttributeNames": {f"#k{i}": k for i, k in enumerate(llave)}}
    with tabla.batch_writer() as lote:
        while True:
            r = tabla.scan(**kwargs)
            for item in r["Items"]:
                lote.delete_item(Key={k: item[k] for k in llave})
                borrados += 1
            if "LastEvaluatedKey" not in r:
                break
            kwargs["ExclusiveStartKey"] = r["LastEvaluatedKey"]
    return borrados


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--bucket", required=True)
    ap.add_argument("--prefijo", default="crudos/")
    ap.add_argument("--region", default=REGION)
    args = ap.parse_args()

    print(f"Se borrarán todos los ítems de {', '.join(TABLAS)} y s3://{args.bucket}/{args.prefijo}")
    if input("Escriba 'si' para continuar: ").strip().lower() != "si":
        print("Cancelado.")
        return

    dynamodb = boto3.resource("dynamodb", region_name=args.region)
    for nombre, llave in TABLAS.items():
        print(f"{nombre}: {vaciar_tabla(dynamodb, nombre, llave)} ítems borrados")

    s3 = boto3.client("s3", region_name=args.region)
    borrados = 0
    for pagina in s3.get_paginator("list_objects_v2").paginate(Bucket=args.bucket, Prefix=args.prefijo):
        objetos = [{"Key": o["Key"]} for o in pagina.get("Contents", [])]
        if objetos:
            s3.delete_objects(Bucket=args.bucket, Delete={"Objects": objetos})
            borrados += len(objetos)
    print(f"s3://{args.bucket}/{args.prefijo}: {borrados} archivos borrados")


if __name__ == "__main__":
    main()
