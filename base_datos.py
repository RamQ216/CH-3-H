import sqlite3
import pandas as pd

conexion = sqlite3.connect('penguin_colony_final.db')
cursor = conexion.cursor()

cursor.execute("PRAGMA foreign_keys = ON;")

# 1. CREACIÓN DE TABLAS CON TODAS LAS COLUMNAS DEL CSV
# Agregamos created_at, is_active y deleted_at que faltaban

cursor.execute("""
    CREATE TABLE IF NOT EXISTS customers (
        customer_id INTEGER PRIMARY KEY,
        full_name TEXT NOT NULL,
        email TEXT,
        phone TEXT,
        city TEXT,
        segment TEXT,
        created_at TEXT,
        is_active INTEGER,
        deleted_at TEXT
    )
""")

cursor.execute("""
    CREATE TABLE IF NOT EXISTS products (
        product_id INTEGER PRIMARY KEY,
        sku TEXT UNIQUE NOT NULL,
        product_name TEXT NOT NULL,
        category TEXT,
        brand TEXT,
        unit_price REAL CHECK(unit_price >= 0),
        unit_cost REAL,
        created_at TEXT,
        is_active INTEGER,
        deleted_at TEXT
    )
""")

cursor.execute("""
    CREATE TABLE IF NOT EXISTS orders (
        order_id INTEGER PRIMARY KEY,
        customer_id INTEGER NOT NULL,
        order_datetime TEXT,
        channel TEXT,
        currency TEXT,
        current_status TEXT,
        is_active INTEGER,
        deleted_at TEXT,
        order_total REAL,
        FOREIGN KEY (customer_id) REFERENCES customers (customer_id)
    )
""")

# Esta tabla tiene columnas de descuentos y totales que hay que incluir
cursor.execute("""
    CREATE TABLE IF NOT EXISTS order_items (
        order_item_id INTEGER PRIMARY KEY,
        order_id INTEGER NOT NULL,
        product_id INTEGER NOT NULL,
        quantity INTEGER CHECK(quantity > 0),
        unit_price REAL,
        discount_rate REAL,
        line_total REAL,
        FOREIGN KEY (order_id) REFERENCES orders (order_id),
        FOREIGN KEY (product_id) REFERENCES products (product_id)
    )
""")

cursor.execute("""
    CREATE TABLE IF NOT EXISTS payments (
        payment_id INTEGER PRIMARY KEY,
        order_id INTEGER NOT NULL,
        payment_datetime TEXT,
        method TEXT,
        payment_status TEXT,
        amount REAL,
        currency TEXT,
        FOREIGN KEY (order_id) REFERENCES orders (order_id)
    )
""")

# Agregamos la columna 'reason' que viene en el CSV
cursor.execute("""
    CREATE TABLE IF NOT EXISTS order_status_history (
        status_history_id INTEGER PRIMARY KEY,
        order_id INTEGER NOT NULL,
        status TEXT,
        changed_at TEXT,
        changed_by TEXT,
        reason TEXT,
        FOREIGN KEY (order_id) REFERENCES orders (order_id)
    )
""")

cursor.execute("""
    CREATE TABLE IF NOT EXISTS order_audit (
        audit_id INTEGER PRIMARY KEY,
        order_id INTEGER NOT NULL,
        field_name TEXT,
        old_value TEXT,
        new_value TEXT,
        changed_at TEXT,
        changed_by TEXT,
        FOREIGN KEY (order_id) REFERENCES orders (order_id)
    )
""")

# 2. CARGA DE DATOS
archivos_carga = [
    ('customers.csv', 'customers'),
    ('products.csv', 'products'),
    ('orders.csv', 'orders'),
    ('order_items.csv', 'order_items'),
    ('payments.csv', 'payments'),
    ('order_status_history.csv', 'order_status_history'),
    ('order_audit.csv', 'order_audit')
]

for archivo, tabla in archivos_carga:
    print(f"Cargando {archivo}...")
    df = pd.read_csv(archivo)
    # Ahora sí, las columnas coinciden perfectamente
    df.to_sql(tabla, conexion, if_exists='append', index=False)

conexion.commit()
conexion.close()
print("\n¡Desafío superado! Base de datos creada sin errores de columnas.")