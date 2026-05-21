import duckdb
import pandas as pd
from pathlib import Path

DB_PATH = Path(__file__).parent / "dev.duckdb"
SEEDS_PATH = Path(__file__).parent / "seeds"

def setup():
    print(f"Création de la base DuckDB : {DB_PATH}")
    con = duckdb.connect(str(DB_PATH))
    con.execute("CREATE SCHEMA IF NOT EXISTS domyos_dwh")

    tables = {
        "dim_model": SEEDS_PATH / "dim_model.csv",
        "dim_store": SEEDS_PATH / "dim_store.csv",
        "fact_sales": SEEDS_PATH / "fact_sales.csv",
        "fact_stock": SEEDS_PATH / "fact_stock.csv",
    }

    for table_name, csv_path in tables.items():
        if not csv_path.exists():
            print(f"  [ERREUR] Fichier manquant : {csv_path}")
            continue

        df = pd.read_csv(csv_path)

        if table_name == "fact_sales":
            df["transaction_date"] = pd.to_datetime(df["transaction_date"])
        if table_name == "fact_stock":
            df["stock_date"] = pd.to_datetime(df["stock_date"])

        con.execute(f"DROP TABLE IF EXISTS domyos_dwh.{table_name}")
        con.register("df_temp", df)
        con.execute(f"CREATE TABLE domyos_dwh.{table_name} AS SELECT * FROM df_temp")
        con.unregister("df_temp")

        count = con.execute(f"SELECT count(*) FROM domyos_dwh.{table_name}").fetchone()[0]
        print(f"  [OK] {table_name} : {count:,} lignes")

    con.close()
    print(f"\nBase prête : {DB_PATH}")

if __name__ == "__main__":
    setup()