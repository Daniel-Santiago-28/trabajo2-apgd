"""
Trabajo 2 · Parte B · Consulta la tabla t2-ventanas para responder la pregunta:

    ¿Qué categoría de producto tiene el ticket promedio (order_amount) más alto
    y cuál recibe más órdenes por minuto?

Uso (en AWS CloudShell):
    python3 consultar_ventanas.py                         # todas las ventanas
    python3 consultar_ventanas.py --lote lote-20261103-190512
    python3 consultar_ventanas.py --categoria Electronics # Query de una sola categoría

Con --categoria hace un Query (lee solo la partición de esa categoría); sin él
hace un Scan de la tabla, que es pequeña (5 categorías x minutos de ejecución).
"""
import argparse
import os
from collections import defaultdict

import boto3
from boto3.dynamodb.conditions import Key

# CloudShell define AWS_REGION con la región elegida en la consola
REGION = os.environ.get("AWS_REGION", "us-east-2")
TABLA = "t2-ventanas"
ORDEN = ["Electronics", "Home", "Fashion", "Beauty", "Grocery"]


def leer(tabla, categoria):
    kwargs = {"KeyConditionExpression": Key("categoria").eq(categoria)} if categoria else {}
    operacion = tabla.query if categoria else tabla.scan
    items = []
    while True:
        r = operacion(**kwargs)
        items += r["Items"]
        if "LastEvaluatedKey" not in r:
            return items
        kwargs["ExclusiveStartKey"] = r["LastEvaluatedKey"]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--region", default=REGION)
    ap.add_argument("--lote", help="solo ventanas con eventos de este lote")
    ap.add_argument("--categoria", help="consultar una sola categoría (Query)")
    args = ap.parse_args()

    tabla = boto3.resource("dynamodb", region_name=args.region).Table(TABLA)
    items = leer(tabla, args.categoria)
    if args.lote:
        items = [i for i in items if args.lote in i.get("conteo_por_lote", {})]
    if not items:
        print("No hay ventanas. ¿Ya terminó el primer minuto? ¿El disparador está activo?")
        return

    # ---- Detalle minuto a minuto ----------------------------------------
    por_ventana = defaultdict(dict)
    for i in items:
        por_ventana[(i["ventana"], i.get("hora_colombia", ""))][i["categoria"]] = i
    categorias = [c for c in ORDEN if any(c in v for v in por_ventana.values())]

    print("Órdenes únicas y ticket promedio por ventana de 1 minuto (n / promedio)\n")
    print(f"{'ventana (UTC)':<22}{'hora Col.':<11}" + "".join(f"{c:>22}" for c in categorias))
    for (ventana, hora_col), fila in sorted(por_ventana.items()):
        celdas = []
        for c in categorias:
            i = fila.get(c)
            celdas.append(f"{int(i['conteo']):>6} / {float(i['promedio_monto']):>12,.2f}" if i else f"{'-':>22}")
        print(f"{ventana:<22}{hora_col[-5:]:<11}" + "".join(f"{x:>22}" for x in celdas))

    # ---- Resumen por categoría ------------------------------------------
    print("\nResumen por categoría")
    print(f"{'categoría':<14}{'ventanas':>9}{'órdenes':>9}{'órdenes/min':>13}{'ticket promedio':>17}{'máx. por min':>14}")
    resumen = {}
    for c in categorias:
        filas = [i for i in items if i["categoria"] == c]
        n = sum(int(i["conteo"]) for i in filas)
        suma = sum(float(i["suma_montos"]) for i in filas)
        resumen[c] = {"n": n, "por_min": n / len(filas), "ticket": suma / n,
                      "max_min": max(int(i["conteo"]) for i in filas), "ventanas": len(filas)}
        r = resumen[c]
        print(f"{c:<14}{r['ventanas']:>9}{n:>9}{r['por_min']:>13.1f}{r['ticket']:>17,.2f}{r['max_min']:>14}")

    # El ticket promedio global de una categoría es suma de montos / número de
    # órdenes, no el promedio de los promedios de cada minuto (cada minuto tiene
    # un número distinto de órdenes).
    if len(resumen) < 2:
        return
    mayor_ticket = max(resumen, key=lambda c: resumen[c]["ticket"])
    mas_ordenes = max(resumen, key=lambda c: resumen[c]["por_min"])
    print("\nRespuesta:")
    print(f"  Ticket promedio más alto : {mayor_ticket} ({resumen[mayor_ticket]['ticket']:,.2f} por orden)")
    print(f"  Más órdenes por minuto   : {mas_ordenes} ({resumen[mas_ordenes]['por_min']:.1f} órdenes/min)")


if __name__ == "__main__":
    main()
