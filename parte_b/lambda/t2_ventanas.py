"""
Trabajo 2 · Parte B · Lambda t2-ventanas (primer consumidor del stream).

Flujo: productor.py -> Kinesis (t2-eventos-ordenes) -> esta Lambda -> DynamoDB

El disparador de Kinesis se configura con una ventana de tiempo fija
(tumbling window) de 60 segundos. Lambda agrupa los registros por el minuto en
que llegaron al stream y llama a la función varias veces por ventana,
pasándole el estado acumulado (event["state"]). Al terminar el minuto la llama
una última vez con isFinalInvokeForWindow = true: ahí se escribe el resultado.

Por cada registro del stream:
  1. Si no es un evento válido      -> t2-descartados, tipo INVALIDO, con su motivo.
  2. Si su event_id ya se recibió   -> t2-descartados, tipo REPETIDO.
  3. Si es nuevo                    -> se registra en t2-eventos-vistos y se suma
                                       al conteo y al monto de su categoría.
Al cerrar la ventana: un ítem por categoría en t2-ventanas con el conteo y el
promedio de order_amount.

Variables de entorno: TABLA_VENTANAS, TABLA_VISTOS, TABLA_DESCARTADOS
"""
import base64
import json
import os
import time
from datetime import datetime, timedelta, timezone
from decimal import Decimal

import boto3
from botocore.exceptions import ClientError

dynamodb = boto3.resource("dynamodb")
tabla_ventanas = dynamodb.Table(os.environ.get("TABLA_VENTANAS", "t2-ventanas"))
tabla_vistos = dynamodb.Table(os.environ.get("TABLA_VISTOS", "t2-eventos-vistos"))
tabla_descartados = dynamodb.Table(os.environ.get("TABLA_DESCARTADOS", "t2-descartados"))

# ---- Reglas de validez (las mismas que usa productor.py) --------------
CATEGORIAS = {"Electronics", "Home", "Fashion", "Beauty", "Grocery"}
MONTO_MAXIMO = 250000          # 5 unidades x 50.000, el mayor precio del catálogo
DIAS_EN_VISTOS = 14            # TTL opcional de t2-eventos-vistos
COLOMBIA = timezone(timedelta(hours=-5))

ESQUEMA = {  # campo: tipos aceptados, en el orden en que se revisan
    "event_id": (str,),
    "lote": (str,),
    "order_id": (int,),
    "customer_id": (int,),
    "product_category": (str,),
    "quantity": (int,),
    "order_amount": (int, float),
    "event_time": (str,),
}


def validar(evento):
    """Devuelve None si el evento es válido, o el motivo del descarte."""
    if not isinstance(evento, dict):
        return "E0 - JSON malformado"
    for campo in ESQUEMA:
        if campo not in evento or evento[campo] is None or evento[campo] == "":
            return f"E1 - Campo obligatorio faltante: {campo}"
    for campo, tipos in ESQUEMA.items():
        valor = evento[campo]
        # bool es subclase de int en Python: se rechaza explícitamente
        if isinstance(valor, bool) or not isinstance(valor, tipos):
            return f"E2 - Tipo inválido en {campo}"
    if evento["product_category"] not in CATEGORIAS:
        return "E3 - product_category fuera del dominio"
    if not 1 <= evento["quantity"] <= 5:
        return "E4 - quantity fuera de rango (1 a 5)"
    if not 0 < evento["order_amount"] <= MONTO_MAXIMO:
        return f"E4 - order_amount fuera de rango (mayor que 0 y hasta {MONTO_MAXIMO})"
    return None


def leer_registro(record):
    """Decodifica un registro de Kinesis. Devuelve (texto, evento, motivo)."""
    datos = base64.b64decode(record["kinesis"]["data"])
    try:
        texto = datos.decode("utf-8")
        evento = json.loads(texto)
    except (UnicodeDecodeError, json.JSONDecodeError):
        return datos.decode("utf-8", errors="replace"), None, "E0 - JSON malformado"
    return texto, evento, validar(evento)


def registrar_si_es_nuevo(evento, seq, llegada, ventana):
    """Escritura condicional en t2-eventos-vistos.

    Devuelve (True, None) si el event_id es nuevo, o (False, seq_original) si
    ya se había recibido. La condición acepta también el mismo número de
    secuencia: si Lambda reintenta un lote, el mismo registro de Kinesis no se
    confunde con un repetido (la escritura es idempotente).
    """
    try:
        tabla_vistos.put_item(
            Item={
                "event_id": evento["event_id"],
                "seq": seq,
                "lote": evento["lote"],
                "categoria": evento["product_category"],
                "order_amount": Decimal(str(evento["order_amount"])),
                "llegada": llegada,
                "ventana": ventana,
                "expira": int(time.time()) + DIAS_EN_VISTOS * 86400,
            },
            ConditionExpression="attribute_not_exists(event_id) OR seq = :seq",
            ExpressionAttributeValues={":seq": seq},
            ReturnValuesOnConditionCheckFailure="ALL_OLD",
        )
        return True, None
    except ClientError as e:
        if e.response["Error"]["Code"] != "ConditionalCheckFailedException":
            raise
        original = e.response.get("Item", {}).get("seq", {}).get("S", "desconocido")
        return False, original


def descartar(seq, tipo, motivo, texto, evento, llegada, ventana, seq_original=None):
    """Aparta el evento en t2-descartados. La llave es el número de secuencia
    de Kinesis, único por registro, así que reintentar no duplica el ítem."""
    item = {
        "seq": seq,
        "tipo": tipo,
        "motivo": motivo,
        "payload": texto[:2000],
        "llegada": llegada,
        "ventana": ventana,
    }
    if isinstance(evento, dict):
        for campo in ("event_id", "lote"):
            if isinstance(evento.get(campo), str) and evento[campo]:
                item[campo] = evento[campo]
    if seq_original:
        item["seq_original"] = seq_original
    tabla_descartados.put_item(Item=item)


def a_decimal(x):
    """DynamoDB no acepta float: se guardan como Decimal con 2 decimales."""
    return Decimal(str(round(x, 2)))


def escribir_ventana(ventana, estado, shard):
    """Un ítem por categoría en t2-ventanas. La llave (categoria, ventana) hace
    que escribir dos veces la misma ventana la sobrescriba, no la duplique."""
    inicio = ventana["start"]
    inicio_col = datetime.fromisoformat(inicio.replace("Z", "+00:00")).astimezone(COLOMBIA)
    resumen = []
    for categoria, a in sorted(estado["categorias"].items()):
        promedio = a["suma"] / a["n"]
        tabla_ventanas.put_item(Item={
            "categoria": categoria,
            "ventana": inicio,
            "ventana_fin": ventana["end"],
            "hora_colombia": inicio_col.strftime("%Y-%m-%d %H:%M"),
            "conteo": a["n"],
            "promedio_monto": a_decimal(promedio),
            "suma_montos": a_decimal(a["suma"]),
            "monto_minimo": a_decimal(a["min"]),
            "monto_maximo": a_decimal(a["max"]),
            "conteo_por_lote": a["lotes"],
            "shard": shard or "desconocido",
            "calculado_en": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        })
        resumen.append(f"{categoria}: n={a['n']} promedio={promedio:,.2f}")
    print(f"VENTANA {inicio} -> {ventana['end']} (Colombia {inicio_col:%H:%M}) | "
          + " | ".join(resumen)
          + f" | repetidos descartados={estado['repetidos']} inválidos descartados={estado['invalidos']}")


def lambda_handler(event, context):
    # El estado viaja entre invocaciones de la misma ventana. Se guarda como un
    # texto JSON dentro de un mapa de texto a texto, el formato que Lambda acepta.
    estado = json.loads((event.get("state") or {}).get("agregados", "{}"))
    estado.setdefault("categorias", {})
    estado.setdefault("repetidos", 0)
    estado.setdefault("invalidos", 0)
    ventana = event.get("window") or {"start": "sin-ventana", "end": "sin-ventana"}

    unicos = repetidos = invalidos = 0
    for record in event.get("Records", []):
        seq = record["kinesis"]["sequenceNumber"]
        llegada = datetime.fromtimestamp(
            record["kinesis"]["approximateArrivalTimestamp"], tz=timezone.utc
        ).isoformat(timespec="milliseconds")
        texto, evento, motivo = leer_registro(record)

        if motivo:
            descartar(seq, "INVALIDO", motivo, texto, evento, llegada, ventana["start"])
            invalidos += 1
            continue

        es_nuevo, seq_original = registrar_si_es_nuevo(evento, seq, llegada, ventana["start"])
        if not es_nuevo:
            descartar(seq, "REPETIDO", "R1 - Evento repetido: event_id ya recibido",
                      texto, evento, llegada, ventana["start"], seq_original)
            repetidos += 1
            continue

        monto = float(evento["order_amount"])
        a = estado["categorias"].setdefault(
            evento["product_category"], {"n": 0, "suma": 0.0, "min": monto, "max": monto, "lotes": {}})
        a["n"] += 1
        a["suma"] += monto
        a["min"] = min(a["min"], monto)
        a["max"] = max(a["max"], monto)
        a["lotes"][evento["lote"]] = a["lotes"].get(evento["lote"], 0) + 1
        unicos += 1

    estado["repetidos"] += repetidos
    estado["invalidos"] += invalidos
    print(json.dumps({"ventana": ventana["start"], "registros": len(event.get("Records", [])),
                      "unicos": unicos, "repetidos": repetidos, "invalidos": invalidos,
                      "final": bool(event.get("isFinalInvokeForWindow"))}))

    if event.get("isFinalInvokeForWindow") or event.get("isWindowTerminatedEarly"):
        if estado["categorias"]:
            escribir_ventana(ventana, estado, event.get("shardId"))
        return {"state": {}}
    return {"state": {"agregados": json.dumps(estado)}}
