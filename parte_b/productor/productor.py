"""
Trabajo 2 · Parte B · Productor: publica eventos de órdenes en Kinesis Data Streams.

Uso (en AWS CloudShell, región us-east-2; boto3 ya viene instalado):
    python3 productor.py                          # 6 minutos, 10 eventos/s
    python3 productor.py --duracion 60            # prueba corta de 1 minuto
    python3 productor.py --duracion 360 --tasa 10 --pct-repetidos 5 --pct-invalidos 5

Cada segundo arma un grupo de eventos y lo envía con PutRecords. De cada evento
decide si es:
  - NUEVO: una orden válida con un event_id nuevo.
  - REPETIDO: una copia idéntica (los mismos bytes) de un evento NUEVO ya
    publicado. La mitad de las copias son de los últimos segundos (caen en la
    misma ventana) y la otra mitad de cualquier momento anterior (caen en otra).
  - INVALIDO: un evento con un defecto a propósito (tabla DEFECTOS).

Al terminar escribe manifiesto_<lote>.csv: una fila por registro aceptado por
Kinesis, con su número de secuencia y lo que se espera que haga el consumidor.
verificar_conteos.py usa ese archivo para cuadrar los conteos.
"""
import argparse
import csv
import os
import json
import random
import time
import uuid
from datetime import datetime, timezone

import boto3

STREAM = "t2-eventos-ordenes"
# CloudShell define AWS_REGION con la región elegida en la consola
REGION = os.environ.get("AWS_REGION", "us-east-2")

# Categorías del proyecto (las mismas de la parte A), con un peso (qué tan
# frecuentes son) y un rango de precio unitario distinto para cada una.
CATEGORIAS = {
    #  categoría     peso  precio mín  precio máx
    "Grocery":     (30,      100,      1500),
    "Fashion":     (25,      500,      8000),
    "Beauty":      (20,      200,      4000),
    "Home":        (15,     2000,     20000),
    "Electronics": (10,     8000,     50000),
}
DESCUENTOS = [0, 5, 10, 15, 20, 25, 30]
MONTO_MAXIMO = 250000

# Defectos inyectados a propósito y el motivo exacto que debe asignar t2-ventanas
DEFECTOS = {
    "json_malformado":       "E0 - JSON malformado",
    "sin_categoria":         "E1 - Campo obligatorio faltante: product_category",
    "sin_monto":             "E1 - Campo obligatorio faltante: order_amount",
    "monto_texto":           "E2 - Tipo inválido en order_amount",
    "categoria_desconocida": "E3 - product_category fuera del dominio",
    "cantidad_cero":         "E4 - quantity fuera de rango (1 a 5)",
    "monto_fuera_de_rango":  f"E4 - order_amount fuera de rango (mayor que 0 y hasta {MONTO_MAXIMO})",
}
MOTIVO_REPETIDO = "R1 - Evento repetido: event_id ya recibido"


def evento_valido(rng, lote):
    categoria = rng.choices(list(CATEGORIAS), weights=[c[0] for c in CATEGORIAS.values()])[0]
    _, pmin, pmax = CATEGORIAS[categoria]
    cantidad = rng.randint(1, 5)
    precio = round(rng.uniform(pmin, pmax), 2)
    descuento = rng.choice(DESCUENTOS)
    return {
        "event_id": str(uuid.uuid4()),
        "lote": lote,
        "order_id": rng.randint(7000001, 7999999),
        "customer_id": rng.randint(1000001, 1500000),
        "product_category": categoria,
        "quantity": cantidad,
        "order_amount": round(cantidad * precio * (1 - descuento / 100), 2),
        "event_time": datetime.now(timezone.utc).isoformat(timespec="milliseconds"),
    }


def a_bytes(evento):
    """Un evento por línea: así quedan también en S3 (JSON Lines)."""
    return (json.dumps(evento, ensure_ascii=False) + "\n").encode("utf-8")


def evento_invalido(rng, lote, defecto):
    """Devuelve (bytes, evento) de un evento con el defecto pedido."""
    ev = evento_valido(rng, lote)
    if defecto == "json_malformado":
        texto = json.dumps(ev)
        return (texto[: len(texto) - 25] + "\n").encode("utf-8"), ev   # JSON cortado
    if defecto == "sin_categoria":
        del ev["product_category"]
    elif defecto == "sin_monto":
        del ev["order_amount"]
    elif defecto == "monto_texto":
        ev["order_amount"] = "N/A"
    elif defecto == "categoria_desconocida":
        ev["product_category"] = rng.choice(["Toys", "Books", "electronics "])
    elif defecto == "cantidad_cero":
        ev["quantity"] = 0
    elif defecto == "monto_fuera_de_rango":
        ev["order_amount"] = rng.choice([-1500.0, 0, 999999.99])
    return a_bytes(ev), ev


def enviar(kinesis, stream, pendientes, max_intentos=6):
    """PutRecords con reintentos. Kinesis puede aceptar unos registros del
    grupo y rechazar otros (por ejemplo por exceder la capacidad del shard):
    solo se reintentan los rechazados. Devuelve (aceptados, fallidos)."""
    aceptados = []
    for intento in range(max_intentos):
        resp = kinesis.put_records(
            StreamName=stream,
            Records=[{"Data": p["data"], "PartitionKey": p["partition_key"]} for p in pendientes],
        )
        rechazados = []
        for p, r in zip(pendientes, resp["Records"]):
            if "SequenceNumber" in r:
                p["seq"], p["shard"] = r["SequenceNumber"], r["ShardId"]
                aceptados.append(p)
            else:
                rechazados.append(p)
        pendientes = rechazados
        if not pendientes:
            break
        time.sleep(0.2 * 2 ** intento)
    return aceptados, pendientes


def main():
    ap = argparse.ArgumentParser(description="Publica eventos de órdenes en Kinesis.")
    ap.add_argument("--stream", default=STREAM)
    ap.add_argument("--region", default=REGION)
    ap.add_argument("--duracion", type=int, default=360, help="segundos (mínimo 300 para la entrega)")
    ap.add_argument("--tasa", type=int, default=10, help="eventos por segundo")
    ap.add_argument("--pct-repetidos", type=float, default=5.0)
    ap.add_argument("--pct-invalidos", type=float, default=5.0)
    ap.add_argument("--semilla", type=int, default=None, help="semilla para categorías y montos")
    args = ap.parse_args()

    kinesis = boto3.client("kinesis", region_name=args.region)
    rng = random.Random(args.semilla)
    lote = datetime.now(timezone.utc).strftime("lote-%Y%m%d-%H%M%S")
    manifiesto = f"manifiesto_{lote}.csv"

    publicados = []          # registros aceptados por Kinesis
    historial = []           # bytes de los eventos NUEVOS ya publicados (para repetirlos)
    cuenta = {"NUEVO": 0, "REPETIDO": 0, "INVALIDO": 0}
    fallidos = 0
    lista_defectos = list(DEFECTOS)
    n_defecto = 0

    print(f"Lote {lote}: publicando en '{args.stream}' ({args.region}) durante {args.duracion} s, "
          f"{args.tasa} eventos/s, ~{args.pct_repetidos:g} % repetidos y ~{args.pct_invalidos:g} % inválidos")
    print("Hora Colombia = UTC - 5. Ctrl+C detiene el productor (el manifiesto se escribe igual).\n")

    inicio = time.monotonic()
    segundo = 0
    try:
        while segundo < args.duracion:
            grupo = []
            for _ in range(args.tasa):
                r = rng.random() * 100
                if r < args.pct_invalidos:
                    defecto = lista_defectos[n_defecto % len(lista_defectos)]
                    n_defecto += 1
                    data, ev = evento_invalido(rng, lote, defecto)
                    grupo.append({"data": data, "partition_key": str(uuid.uuid4()),
                                  "tipo": "INVALIDO", "motivo": DEFECTOS[defecto],
                                  "event_id": ev.get("event_id", ""), "categoria": ev.get("product_category", ""),
                                  "monto": ev.get("order_amount", "")})
                elif r < args.pct_invalidos + args.pct_repetidos and historial:
                    original = rng.choice(historial[-30:]) if rng.random() < 0.5 else rng.choice(historial)
                    grupo.append(dict(original, tipo="REPETIDO", motivo=MOTIVO_REPETIDO))
                else:
                    ev = evento_valido(rng, lote)
                    grupo.append({"data": a_bytes(ev), "partition_key": ev["event_id"],
                                  "tipo": "NUEVO", "motivo": "", "event_id": ev["event_id"],
                                  "categoria": ev["product_category"], "monto": ev["order_amount"]})

            aceptados, rechazados = enviar(kinesis, args.stream, grupo)
            fallidos += len(rechazados)
            ahora = int(time.time() * 1000)
            for p in aceptados:
                p["sent_ts"] = ahora
                publicados.append(p)
                cuenta[p["tipo"]] += 1
                if p["tipo"] == "NUEVO":
                    historial.append({k: p[k] for k in ("data", "partition_key", "event_id", "categoria", "monto")})

            segundo += 1
            if segundo % 10 == 0:
                print(f"  t={segundo:>4} s  publicados={len(publicados):>5}  "
                      f"(nuevos {cuenta['NUEVO']}, repetidos {cuenta['REPETIDO']}, "
                      f"inválidos {cuenta['INVALIDO']})  rechazados={fallidos}")
            # Espera al siguiente segundo para mantener una tasa constante
            time.sleep(max(0.0, inicio + segundo - time.monotonic()))
    except KeyboardInterrupt:
        print("\nDetenido con Ctrl+C.")

    duracion = time.monotonic() - inicio
    with open(manifiesto, "w", newline="", encoding="utf-8") as fh:
        w = csv.writer(fh)
        w.writerow(["seq", "shard", "tipo_esperado", "motivo_esperado", "event_id", "categoria", "monto", "sent_ts"])
        for p in publicados:
            w.writerow([p["seq"], p["shard"], p["tipo"], p["motivo"], p["event_id"],
                        p["categoria"], p["monto"], p["sent_ts"]])

    print(f"\nLote: {lote}")
    print(f"Publicados (aceptados por Kinesis): {len(publicados)} en {duracion:.1f} s "
          f"({len(publicados) / max(duracion, 1):.1f} eventos/s)")
    print(f"  nuevos (deben quedar como únicos) : {cuenta['NUEVO']}")
    print(f"  repetidos a propósito             : {cuenta['REPETIDO']}")
    print(f"  inválidos a propósito             : {cuenta['INVALIDO']}")
    print(f"Rechazados por Kinesis tras reintentos (no cuentan como publicados): {fallidos}")
    print(f"Manifiesto: {manifiesto}")
    print("\nEspere 2 minutos (cierre de la última ventana) y verifique con:")
    print(f"  python3 verificar_conteos.py --lote {lote} --bucket t2-streaming-<ACCOUNT_ID>")


if __name__ == "__main__":
    main()
