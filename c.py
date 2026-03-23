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

conexion.commit()

# Cerramos la conexión
conexion.close()

print("¡Tabla 'customers' creada exitosamente!")
