import sqlite3

# Create sample SQLite database
conn = sqlite3.connect("local_data.db")
conn.execute("CREATE TABLE IF NOT EXISTS users (id INTEGER, name TEXT)")
conn.execute("INSERT INTO users VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Charlie')")
conn.commit()
conn.close()
print("Sample database created: local_data.db")
