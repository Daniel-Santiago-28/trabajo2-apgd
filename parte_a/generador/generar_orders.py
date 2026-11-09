"""Trabajo 2 · Parte A · Aumenta orders.csv hasta superar 5.000.000 de registros.

Basado en labs/shared/generator/generate_data.py del repositorio del curso.
Toma de ese generador:

- Una semilla fija (random.Random(seed)): la misma semilla produce los mismos
  archivos, así cualquiera puede reproducir exactamente los datos.
- Un catálogo fijo de productos y dominios de valores (como PRODUCTS y CITIES).
- Un archivo CSV por mes (la clase MonthlyFiles), con el mismo encabezado.
- Defectos inyectados a propósito en una fracción de las líneas (como
  DEFECT_RATES, pick_defect y apply_defect del lote de octubre), para que el
  trabajo de Glue tenga algo real que limpiar.

Lo nuevo frente a ese generador:

- Las columnas y los dominios son los de orders.csv del proyecto, medidos con
  un perfilamiento del archivo original (ver README).
- Los customer_id se leen de customers.csv, para que la unión (join) funcione.
- El primer order_id continúa después del mayor order_id de orders.csv.

Uso:
    python generar_orders.py --orders-csv ../datos/orders.csv \
        --customers-csv ../datos/customers.csv --output ../datos/generados

Solo usa la biblioteca estándar de Python (no hay que instalar nada).
"""
import argparse
import csv
from contextlib import ExitStack
from datetime import date, timedelta
from pathlib import Path
import random

# Mismo rango de fechas que orders.csv (2019-01-01 a 2026-05-24).
START_DATE = date(2019, 1, 1)
END_DATE = date(2026, 5, 24)

# Categoría -> productos. Igual que en orders.csv: 5 categorías, 3 productos.
CATALOG = {
    'Beauty': ['Cream', 'Perfume', 'Shampoo'],
    'Electronics': ['Headphones', 'Laptop', 'Phone'],
    'Fashion': ['Jeans', 'Shoes', 'T-Shirt'],
    'Grocery': ['Milk', 'Rice', 'Snacks'],
    'Home': ['Chair', 'Lamp', 'Table'],
}
CATEGORIES = list(CATALOG)
PAYMENT_METHODS = ['COD', 'Credit Card', 'Debit Card', 'UPI', 'Wallet']
STATUSES = ['Cancelled', 'Delivered', 'Returned']
# En orders.csv 1 de cada 5 devoluciones no tiene motivo: se conserva.
RETURN_REASONS = ['Changed Mind', 'Damaged', 'Late Delivery', 'Wrong Item', '']
DISCOUNTS = [0, 5, 10, 15, 20, 25, 30]
MIN_PRICE, MAX_PRICE = 100.0, 50000.0

ORDER_COLUMNS = [
    'order_id', 'customer_id', 'order_date', 'product_category',
    'product_name', 'quantity', 'unit_price', 'discount_percent',
    'order_amount', 'payment_method', 'coupon_used', 'delivery_days',
    'order_status', 'returned', 'return_reason', 'seller_rating',
    'customer_rating'
]

# Fracción de las líneas generadas que lleva cada defecto.
DEFECT_RATES = {
    'duplicate': 0.03,  # la línea se escribe dos veces, idéntica
    'invalid_date': 0.01,  # fecha vacía, inexistente o en otro formato
    'invalid_quantity': 0.01,  # cantidad 0 o negativa
    'amount_mismatch': 0.02,  # order_amount no cuadra con el cálculo
    'missing_customer': 0.01,  # customer_id vacío
    'unknown_customer': 0.01,  # customer_id que no existe en customers.csv
    'dirty_text': 0.02,  # espacios y mayúsculas/minúsculas mezcladas
}


def random_date(rng, first, last):
    return first + timedelta(days=rng.randint(0, (last - first).days))


def month_key(day):
    return day.strftime('%Y-%m')


def order_amount(quantity, unit_price, discount):
    return round(quantity * unit_price * (1 - discount / 100), 2)


def read_customer_ids(path):
    with open(path, newline='', encoding='utf-8') as handle:
        return [int(row['customer_id']) for row in csv.DictReader(handle)]


def read_last_order_id(path):
    with open(path, newline='', encoding='utf-8') as handle:
        return max(int(row['order_id']) for row in csv.DictReader(handle))


class MonthlyFiles:
    """Abre un archivo por mes, la primera vez que se necesita ese mes."""

    def __init__(self, folder, pattern, newline=None):
        folder.mkdir(parents=True, exist_ok=True)
        self.folder = folder
        self.pattern = pattern
        self.newline = newline
        self.handles = {}
        self.stack = ExitStack()

    def __enter__(self):
        return self

    def __exit__(self, *_):
        self.stack.close()

    def __contains__(self, key):
        return key in self.handles

    def __getitem__(self, key):
        if key not in self.handles:
            path = self.folder / self.pattern.format(key)
            self.handles[key] = self.stack.enter_context(
                open(path, 'w', newline=self.newline, encoding='utf-8'))
        return self.handles[key]


def make_order(rng, order_id, customer_ids):
    """Devuelve (fecha, fila) de una orden limpia."""
    order_date = random_date(rng, START_DATE, END_DATE)
    category = rng.choice(CATEGORIES)
    product = rng.choice(CATALOG[category])
    quantity = rng.randint(1, 5)
    unit_price = round(rng.uniform(MIN_PRICE, MAX_PRICE), 2)
    discount = rng.choice(DISCOUNTS)
    status = rng.choice(STATUSES)
    returned = 'Yes' if status == 'Returned' else 'No'
    reason = rng.choice(RETURN_REASONS) if returned == 'Yes' else ''
    row = [
        order_id,
        rng.choice(customer_ids),
        order_date.isoformat(), category, product, quantity, unit_price,
        discount,
        order_amount(quantity, unit_price, discount),
        rng.choice(PAYMENT_METHODS),
        rng.choice(['Yes', 'No']),
        rng.randint(1, 10), status, returned, reason,
        rng.randint(30, 50) / 10,
        rng.randint(1, 5)
    ]
    return order_date, row


def pick_defect(rng):
    """Devuelve el defecto de una línea, o None si la línea queda limpia."""
    roll = rng.random()
    threshold = 0.0
    for name, rate in DEFECT_RATES.items():
        threshold += rate
        if roll < threshold:
            return name
    return None


def apply_defect(rng, row, defect, max_customer_id):
    """Devuelve una copia de la fila con el defecto aplicado."""
    row = list(row)
    column = ORDER_COLUMNS.index
    if defect == 'invalid_date':
        day = row[column('order_date')]
        row[column('order_date')] = rng.choice(
            ['', '2024-02-30', '2025-13-01', day[8:] + '/' + day[5:7] + '/' +
             day[:4]])
    elif defect == 'invalid_quantity':
        quantity = rng.choice([0, -1, -2])
        row[column('quantity')] = quantity
        row[column('order_amount')] = order_amount(
            quantity, row[column('unit_price')], row[column('discount_percent')])
    elif defect == 'amount_mismatch':
        row[column('order_amount')] = round(
            row[column('order_amount')] + rng.choice([-1000, 1000, 5000]), 2)
    elif defect == 'missing_customer':
        row[column('customer_id')] = ''
    elif defect == 'unknown_customer':
        row[column('customer_id')] = max_customer_id + rng.randint(1, 100000)
    elif defect == 'dirty_text':
        row[column('product_category')] = '  ' + row[column(
            'product_category')].lower() + ' '
        row[column('payment_method')] = rng.choice(
            [str.lower, str.upper])(row[column('payment_method')]) + '  '
    return row


def write_orders(rng, count, first_order_id, customer_ids, folder):
    """Escribe las órdenes por mes y devuelve los conteos."""
    counts = dict.fromkeys(DEFECT_RATES, 0)
    counts.update(orders=count, lines=0)
    max_customer_id = max(customer_ids)
    writers = {}
    with MonthlyFiles(folder, 'orders-{}.csv', newline='') as files:
        for index in range(count):
            order_date, row = make_order(rng, first_order_id + index,
                                         customer_ids)
            key = month_key(order_date)
            if key not in files:
                writers[key] = csv.writer(files[key])
                writers[key].writerow(ORDER_COLUMNS)
            defect = pick_defect(rng)
            if defect and defect != 'duplicate':
                row = apply_defect(rng, row, defect, max_customer_id)
            writers[key].writerow(row)
            counts['lines'] += 1
            if defect:
                counts[defect] += 1
            if defect == 'duplicate':
                writers[key].writerow(row)
                counts['lines'] += 1
    counts['files'] = len(writers)
    return counts


def parse_args():
    parser = argparse.ArgumentParser(
        description=__doc__.split('\n', maxsplit=1)[0])
    parser.add_argument('--orders-csv', type=Path, required=True)
    parser.add_argument('--customers-csv', type=Path, required=True)
    parser.add_argument('--orders', type=int, default=4400000)
    parser.add_argument('--seed', type=int, default=42)
    parser.add_argument('--output', type=Path, required=True)
    return parser.parse_args()


def main():
    args = parse_args()
    rng = random.Random(args.seed)
    customer_ids = read_customer_ids(args.customers_csv)
    first_order_id = read_last_order_id(args.orders_csv) + 1
    print(f'customers.csv: {len(customer_ids)} clientes')
    print(f'primer order_id generado: {first_order_id}')
    counts = write_orders(rng, args.orders, first_order_id, customer_ids,
                          args.output)
    print(f"generadas: {counts['orders']} órdenes, {counts['lines']} líneas, "
          f"{counts['files']} archivos")
    for defect in DEFECT_RATES:
        print(f'  {defect}: {counts[defect]}')
    print(f'Archivos escritos en {args.output}')


if __name__ == '__main__':
    main()
