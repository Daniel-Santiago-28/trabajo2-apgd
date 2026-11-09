"""
Trabajo 2 · Parte B · Cuadre de conteos de un lote del productor.

Uso (en AWS CloudShell, en la carpeta donde quedó el manifiesto):
    python3 verificar_conteos.py --lote lote-20261103-190512 --bucket t2-streaming-<ACCOUNT_ID>

Comprueba que:
    publicados = únicos procesados + repetidos descartados + inválidos descartados

  - publicados: filas de manifiesto_<lote>.csv (registros que Kinesis aceptó).
  - únicos procesados: ítems de t2-eventos-vistos cuyo seq está en el manifiesto.
  - repetidos e inválidos: ítems de t2-descartados cuyo seq está en el manifiesto.
Se cruza por número de secuencia de Kinesis, que es único por registro, así
que pruebas anteriores que sigan en las tablas no afectan el resultado.

Comprobaciones cruzadas:
  - la suma de los conteos de las ventanas (t2-ventanas) es igual a los únicos;
  - las líneas guardadas en S3 por el segundo consumidor son iguales a los publicados;
  - cada registro quedó clasificado como el productor esperaba.
"""
import argparse
import csv
import os
from collections import Counter

import boto3

# CloudShell define AWS_REGION con la región elegida en la consola
REGION = os.environ.get("AWS_REGION", "us-east-2")
TABLA_VENTANAS = "t2-ventanas"
TABLA_VISTOS = "t2-eventos-vistos"
TABLA_DESCARTADOS = "t2-descartados"


def escanear(dynamodb, nombre):
    tabla = dynamodb.Table(nombre)
    items, kwargs = [], {}
    while True:
        r = tabla.scan(**kwargs)
        items += r["Items"]
        if "LastEvaluatedKey" not in r:
            return items
        kwargs["ExclusiveStartKey"] = r["LastEvaluatedKey"]


def lineas_en_s3(s3, bucket, prefijo, lote):
    """Cuenta las líneas de los archivos crudos que mencionan el lote. Todos los
    eventos del productor, incluso el JSON malformado, contienen su lote."""
    total, archivos = 0, 0
    for pagina in s3.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=prefijo):
        for obj in pagina.get("Contents", []):
            cuerpo = s3.get_object(Bucket=bucket, Key=obj["Key"])["Body"].read().decode("utf-8", "replace")
            n = sum(1 for linea in cuerpo.splitlines() if lote in linea)
            if n:
                archivos += 1
                total += n
    return total, archivos


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--lote", required=True)
    ap.add_argument("--bucket", help="bucket de los eventos crudos (t2-streaming-<ACCOUNT_ID>)")
    ap.add_argument("--prefijo", default="crudos/")
    ap.add_argument("--manifiesto")
    ap.add_argument("--region", default=REGION)
    args = ap.parse_args()

    with open(args.manifiesto or f"manifiesto_{args.lote}.csv", encoding="utf-8") as fh:
        manifiesto = {m["seq"]: m for m in csv.DictReader(fh)}

    dynamodb = boto3.resource("dynamodb", region_name=args.region)
    vistos = [i for i in escanear(dynamodb, TABLA_VISTOS) if i["seq"] in manifiesto]
    descartados = [i for i in escanear(dynamodb, TABLA_DESCARTADOS) if i["seq"] in manifiesto]
    repetidos = [i for i in descartados if i["tipo"] == "REPETIDO"]
    invalidos = [i for i in descartados if i["tipo"] == "INVALIDO"]
    suma_ventanas = sum(int(i.get("conteo_por_lote", {}).get(args.lote, 0))
                        for i in escanear(dynamodb, TABLA_VENTANAS))

    publicados = len(manifiesto)
    total = len(vistos) + len(repetidos) + len(invalidos)
    esperado = Counter(m["tipo_esperado"] for m in manifiesto.values())

    print(f"=== Conteos del lote {args.lote} ===")
    print(f"Publicados (manifiesto del productor)          : {publicados:>6}")
    print(f"Únicos procesados        (t2-eventos-vistos)   : {len(vistos):>6}   esperado {esperado['NUEVO']}")
    print(f"Repetidos descartados    (t2-descartados)      : {len(repetidos):>6}   esperado {esperado['REPETIDO']}")
    print(f"Inválidos descartados    (t2-descartados)      : {len(invalidos):>6}   esperado {esperado['INVALIDO']}")
    print(f"Únicos + repetidos + inválidos                 : {total:>6}")
    print(f"¿Cuadra? (publicados = únicos + rep. + inv.)   : {'SÍ' if total == publicados else 'NO'}")
    if total < publicados:
        print(f"  Faltan {publicados - total} registros: espere un minuto y vuelva a ejecutarlo.")

    print("\nComprobaciones cruzadas")
    print(f"Suma de 'conteo' de las ventanas (t2-ventanas) : {suma_ventanas:>6}   "
          f"{'= únicos' if suma_ventanas == len(vistos) else '≠ únicos (¿falta cerrar la última ventana?)'}")
    if args.bucket:
        lineas, archivos = lineas_en_s3(boto3.client("s3", region_name=args.region), args.bucket, args.prefijo, args.lote)
        print(f"{f'Eventos crudos en S3 ({archivos} archivos)':<47}: {lineas:>6}   "
              f"{'= publicados' if lineas == publicados else '≠ publicados (el consumidor escribe cada ~60 s)'}")

    real = {i["seq"]: "NUEVO" for i in vistos}
    real.update({i["seq"]: i["tipo"] for i in descartados})
    motivo_real = {i["seq"]: i["motivo"] for i in descartados}
    coinciden = sum(1 for seq, m in manifiesto.items()
                    if real.get(seq) == m["tipo_esperado"] and motivo_real.get(seq, "") == m["motivo_esperado"])
    print(f"Registros clasificados como se esperaba        : {coinciden:>6} de {publicados}")

    print("\nInválidos descartados por motivo")
    for motivo, n in sorted(Counter(i["motivo"] for i in invalidos).items()):
        print(f"  {motivo:<62}{n:>5}")


if __name__ == "__main__":
    main()
