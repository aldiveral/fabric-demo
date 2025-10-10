import os
import pandas as pd
import pyodbc
from sqlalchemy import create_engine
from dotenv import load_dotenv

def load_env():
    """Loads environment config based on ENV (defaults to dev)."""
    env = os.getenv("ENV", "dev")
    env_file = f"{env}.env"
    if not os.path.exists(env_file):
        raise FileNotFoundError(f"Environment file '{env_file}' not found.")
    load_dotenv(env_file)
    print(f"🌱 Loaded environment: {env}")
    return env

def get_local_dataframe():
    """Load data from local SQLite DB (for testing)."""
    db_path = os.getenv("DB_PATH", "./data/local_sample.db")
    engine = create_engine(f"sqlite:///{db_path}")
    df = pd.read_sql("SELECT * FROM users", engine)
    return df

def get_fabric_dataframe():
    """Load data from Fabric SQL or Lakehouse via pyodbc."""
    db_driver = os.getenv("DB_DRIVER", "ODBC Driver 18 for SQL Server")
    db_server = os.getenv("DB_SERVER")
    db_name = os.getenv("DB_NAME")
    db_uid = os.getenv("DB_UID")
    db_pwd = os.getenv("DB_PWD")

    conn_str = (
        f"DRIVER={{{db_driver}}};"
        f"SERVER={db_server};"
        f"DATABASE={db_name};"
        f"UID={db_uid};"
        f"PWD={db_pwd}"
    )

    with pyodbc.connect(conn_str) as conn:
        df = pd.read_sql("SELECT TOP 10 * FROM dbo.Users", conn)
    return df
