# README

Trabajo 2 · Almacenamiento y Procesamiento de Grandes Datos: procesamiento batch y streaming en AWS

**Integrantes del equipo:**

| Nombre completo | Cédula         |
| --------------- | -------------- |
| Daniel Amaya Yepes      | 1037646508 |
| Daniel Santiago Cadavid      | 1000646110 |
| Luis Camilo Valencia      | 1037670493 |

Link del repositorio: https://github.com/Daniel-Santiago-28/trabajo2-apgd
Link del video de YouTube: https://youtu.be/fr8hRCsVIrA

- [README](#readme)
  - [Resumen](#resumen)
  - [Las dos partes](#las-dos-partes)
  - [Estructura de esta carpeta](#estructura-de-esta-carpeta)
  - [Condiciones comunes](#condiciones-comunes)
  - [Declaración de uso de IA](#declaración-de-uso-de-ia)

---

## Resumen

El trabajo construye en AWS dos formas de procesar los datos de una misma tienda en línea: sus órdenes y sus clientes, con cinco categorías de producto (`Electronics`, `Home`, `Fashion`, `Beauty`, `Grocery`).

- La **parte A** procesa los datos **por lotes (batch)**: un horario dispara un pipeline que limpia y une más de 5 millones de órdenes y deja resúmenes listos para analítica.
- La **parte B** procesa los datos **en streaming**: las órdenes llegan una a una, de forma continua, y se agregan minuto a minuto a medida que llegan.

Cada parte tiene su propio README con el paso a paso completo, los conceptos, el diseño, los costos, la solución de problemas y cómo borrar los recursos. Este README solo da la vista general.

## Las dos partes

| | [Parte A: batch](parte_a/README.md) | [Parte B: streaming](parte_b/README_ACADEMY.md) |
| --- | --- | --- |
| Entrada | `orders.csv` y `customers.csv` en S3, aumentados hasta 5.531.599 órdenes | Un productor que publica 10 órdenes por segundo durante 6 minutos |
| Servicios | Amazon S3, AWS Glue (PySpark), AWS Step Functions, Amazon EventBridge Scheduler, Amazon Athena | Amazon Kinesis Data Streams, AWS Lambda, Amazon DynamoDB, Amazon S3, AWS CloudShell |
| Procesamiento | Limpieza, join con clientes y Parquet particionado por año; luego dos agregaciones en paralelo | Validación, descarte de repetidos y ventanas fijas de un minuto por categoría |
| Preguntas | ¿Qué categoría genera más ingresos cada año? ¿Qué segmento de clientes devuelve más y por qué? | ¿Qué categoría tiene el ticket promedio más alto y cuál recibe más órdenes por minuto? |
| Resultado | Tablas consultadas con Athena | Ventanas en DynamoDB, eventos crudos en S3 y un cuadre de conteos |

## Estructura de esta carpeta

```text
trabajo2/
├── README.md                   # esta vista general
├── parte_a/                    # pipeline batch
│   ├── README.md               # paso a paso de la parte A
│   └── ...                     # generador, scripts de Glue, Step Functions, Athena
└── parte_b/                    # flujo de streaming
    ├── README_ACADEMY.md       # paso a paso de la parte B en AWS Academy (Learner Lab)
    └── ...                     # productor, Lambdas, políticas de IAM, consultas
```

## Condiciones comunes

- La parte A usa la región **`us-east-2` (Ohio)**. La parte B se monta en **AWS Academy**, en **`us-east-1` (N. Virginia)**, porque la cuenta del Free Tier no tiene disponible Kinesis Data Streams: siga [README_ACADEMY.md](parte_b/README_ACADEMY.md).
- Todos los recursos se crean **a mano en la consola de AWS**, sin CloudFormation, Serverless Framework ni Terraform.
- Las partes son independientes: se pueden construir en cualquier orden, aunque la parte B hace referencias a la A para comparar batch con streaming.

## Declaración de uso de IA

Cada parte tiene su respectiva declaración de uso de IA al final de su README:

- [Parte A · Declaración de uso de IA](parte_a/README.md#declaración-de-uso-de-ia)
- [Parte B · Declaración de uso de IA](parte_b/README_ACADEMY.md#declaración-de-uso-de-ia)

Este README también fue redactado con Claude y revisado por el equipo.
