import sqlite3
import pandas as pd
import os

BaseDedatos = 'hendyla.db' 


if os.path.exists(BaseDedatos):
    os.remove(BaseDedatos)
    print(f"Base de datos {BaseDedatos} reseteada.")


conexion = sqlite3.connect(BaseDedatos)
cursor = conexion.cursor()


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
    df.to_sql(tabla, conexion, if_exists='append', index=False)#interpreta 

# --- 3. CONSULTAS ESTRUCTURALES (Punto 5.4) ---
print("\n=== CONSULTAS ESTRUCTURALES (Navegando el Modelo) ===")

# A. Rastrear pedidos y sus clientes (Relacionar entidades)
consulta_pedidos = """
SELECT o.order_id, c.full_name, o.order_datetime, o.order_total
FROM orders o
JOIN customers c ON o.customer_id = c.customer_id
WHERE o.order_total > 300
ORDER BY o.order_total DESC;
"""
print("\nPedidos de alto valor:")
print(pd.read_sql_query(consulta_pedidos, conexion))

# B. Ver detalle de productos en cada orden (Rastrear eventos)
consulta_detalles = """
SELECT o.order_id, p.product_name, oi.quantity, oi.line_total
FROM order_items oi
JOIN orders o ON oi.order_id = o.order_id
JOIN products p ON oi.product_id = p.product_id
ORDER BY o.order_id ASC;
"""
print("\nDesglose de productos por pedido:")
print(pd.read_sql_query(consulta_detalles, conexion).head(10)) # Mostramos los primeros 10


# --- 4. VALIDACIÓN DE INTEGRIDAD (Punto 5.5) ---
print("\n=== VALIDACIÓN DE INTEGRIDAD (Buscando fallas) ===")

# A. Detectar Relaciones Rotas (Pedidos sin items)
# Si un pedido existe pero no tiene productos cargados, es una inconsistencia.
validacion_items = """
SELECT o.order_id, o.order_datetime
FROM orders o
LEFT JOIN order_items oi ON o.order_id = oi.order_id
WHERE oi.order_item_id IS NULL;
"""
print("\nPedidos sin productos asociados (Relación incompleta):")
print(pd.read_sql_query(validacion_items, conexion))

# B. Detectar Estados Imposibles (Productos con costo mayor al precio)
# Aunque pusimos un CHECK, esta consulta sirve para auditar la tabla.
validacion_precios = """
SELECT product_id, product_name, unit_cost, unit_price
FROM products
WHERE unit_cost > unit_price;
"""
print("\nProductos con margen negativo (Error de negocio):")
print(pd.read_sql_query(validacion_precios, conexion))

# C. Detectar Entidades Incompletas (Clientes sin email o teléfono)
validacion_contacto = """
SELECT customer_id, full_name
FROM customers
WHERE email IS NULL OR phone IS NULL;
"""
print("\nClientes con datos de contacto faltantes:")
print(pd.read_sql_query(validacion_contacto, conexion))

# Definimos el parámetro que queremos buscar
nombre_a_buscar = "Camila Benitez"

# El '?' es el marcador de posición
query_cliente = "SELECT * FROM customers WHERE full_name = ?"

# Ejecutamos pasando el parámetro dentro de una TUPLA (por eso lleva la coma al final)
df_cliente = pd.read_sql_query(query_cliente, conexion, params=(nombre_a_buscar,))#le pasamos en tupla porque necesitamos un valor iterable porque pandas no acepta valores sueltos

print(f"Resultado de búsqueda para: {nombre_a_buscar}")
print(df_cliente)

# Filtros que podrían venir de un formulario o input
canal_filtro = 'web'
estado_filtro = 'shipped'

query_pedidos = """
    SELECT order_id, order_datetime, order_total 
    FROM orders 
    WHERE channel = ? AND current_status = ?
"""

# Pasamos los dos parámetros en orden
df_filtrado = pd.read_sql_query(query_pedidos, conexion, params=(canal_filtro, estado_filtro))

print(f"Pedidos en {canal_filtro} que están {estado_filtro}:")
print(df_filtrado.head())


conexion.commit()
conexion.close()