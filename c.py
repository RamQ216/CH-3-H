import sqlite3


conexion=sqlite3.connect("DB_CH3_H")

cursor = conexion.cursor()

cursor.execute("""CREATE TABLE customers( customer_id INTEGER PRIMARY KEY, 
 full_name TEXT NOT NULL,
 email TEXT UNIQUE,
 phone VARCHAR(20), 
 city TEXT, 
 segment TEXT,
 created_at DATETIME,
 is_active INTEGER, 
 deleted_at DATETIME
               )"""
)

cursor.execute("""CREATE TABLE dataset_diccionary(file INTEGER, 
               rows INEGER, 
               columns INTEGER)"""
               )

cursor.execute("""CREATE TABLE order_audit(audit_id INTEGER, 
               order_id INTEGER, 
               field_name TEXT, 
               old_value VARCHAR(20), 
               new_value VARCHAR(20), 
               changed_at DATETIME, 
               changed_by TEXT)""")

cursor.execute("""CREATE TABLE order_items(
               order_item_id INTEGER,
               order_id INTERGER,
               product_id INTEGER,
               quantity INTEGER,
               unit_price INTEGER,
               discount_rate FLOAT,
               lineal_total FLOAT,)""")

cursor.execute("""CREATE TABLE order_status_history(
               status_history_id INTEGER, 
               order_id INTEGER, 
               status TEXT, 
               changed_at DATETIME, 
               changed_by TEXT, 
               reason TEXT)""")

cursor.execute("""CREATE TABLE order(
               order_id INTEGER,
               customer_id INTEGER
               order_datetime DATETIME
               channel TEXT,
               currency TEXT,
               current_status TEXT,
               is_active INTEGER,
               delete_at DATETIME,
               order_total FLOAT)""")

cursor.execute("""CREATE TABLE payments(
               payment_id INTEGER,
               order_id INTEGER,
               payment_datetime DATETIME,
               method TEXT,
               amount FLOAT,
               currency TEXT)""")

cursor.execute("""CREATE TABLE products(
               product_id INTEGER,
               sku VARCHR(30),
               product_name TEXT,
               category TEXT,
               brand TEXT,
               unit_price FLOAT,
               unit_cost FLOAT,
               created_at DATETIME,
               is_active INTEGER,
               deleted_at DATETIME)""")



conexion.commit()

# Cerramos la conexión
conexion.close()

print("¡Tabla 'customers' creada exitosamente!")
