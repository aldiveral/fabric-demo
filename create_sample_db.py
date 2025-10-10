import os
import sqlite3
import pandas as pd

def create_local_db(db_path="./data/local_sample.db"):
    os.makedirs(os.path.dirname(db_path), exist_ok=True)
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()

    # Create sample table
    cur.execute("""
        CREATE TABLE IF NOT EXISTS users (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT,
            email TEXT,
            role TEXT
        )
    """)

    # Add some test data
    users = [
        ("Alice", "alice@example.com", "Admin"),
        ("Bob", "bob@example.com", "Manager"),
        ("Charlie", "charlie@example.com", "Analyst"),
    ]
    cur.executemany("INSERT INTO users (name, email, role) VALUES (?, ?, ?)", users)
    conn.commit()
    conn.close()
    print(f"✅ Created sample DB at {db_path}")

if __name__ == "__main__":
    create_local_db()
