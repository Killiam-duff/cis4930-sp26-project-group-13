"""
storage.py — Data storage module for the weather pipeline

"""

import os
import sqlite3
import logging
import pandas as pd

# Output paths (override here or pass explicit paths to each function)
CSV_PATH    = os.path.join("data", "processed", "weather_data.csv")
DB_PATH     = os.path.join("data", "processed", "weather_data.db")
TABLE_NAME  = "weather"

# SQLite schema
CREATE_TABLE_SQL = f"""
CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    city            TEXT    NOT NULL,
    date            TEXT    NOT NULL,
    temp_max_f      REAL,
    temp_min_f      REAL,
    precipitation_in REAL,
    windspeed_max_mph REAL,
    run_timestamp   TEXT,
    UNIQUE(city, date)          -- prevent exact duplicate rows across runs
);
"""


# Public helpers

def save_csv(rows: list[dict], path: str = CSV_PATH) -> None:
    """
    Save weather rows to a CSV file
    """
    if not rows:
        logging.warning("save_csv: no rows to write, skipping.")
        return

    os.makedirs(os.path.dirname(path), exist_ok=True)
    df = _rows_to_dataframe(rows)
    df.to_csv(path, index=False)
    logging.info(f"Saved CSV to {path} ({len(df)} rows)")
    print(f"Saved CSV to {path}")


def save_sqlite(rows: list[dict], db_path: str = DB_PATH) -> None:
    """
    Insert weather rows into a SQLite database
    """
    if not rows:
        logging.warning("save_sqlite: no rows to write, skipping.")
        return

    os.makedirs(os.path.dirname(db_path), exist_ok=True)
    df = _rows_to_dataframe(rows)

    with sqlite3.connect(db_path) as conn:
        _ensure_table(conn)
        inserted = _insert_rows(conn, df)

    logging.info(f"Saved SQLite to {db_path} ({inserted} new rows inserted)")
    print(f"Saved SQLite to {db_path} ({inserted} new rows inserted)")


def load_sqlite(db_path: str = DB_PATH) -> pd.DataFrame:
    """
    Load all weather records from SQLite into a DataFrame
    """
    if not os.path.exists(db_path):
        logging.warning(f"load_sqlite: database not found at {db_path}")
        return pd.DataFrame()

    with sqlite3.connect(db_path) as conn:
        try:
            df = pd.read_sql_query(f"SELECT * FROM {TABLE_NAME}", conn)
        except Exception as e:
            logging.error(f"load_sqlite: query failed — {e}")
            return pd.DataFrame()

    return df


# Internal helpers

def _rows_to_dataframe(rows: list[dict]) -> pd.DataFrame:
    """Convert list of row dicts to a tidy DataFrame with consistent column order"""
    df = pd.DataFrame(rows)

    # Enforce column order (extra columns like run_timestamp are kept at the end)
    core_cols = ["city", "date", "temp_max_f", "temp_min_f",
                 "precipitation_in", "windspeed_max_mph"]
    extra_cols = [c for c in df.columns if c not in core_cols]
    return df[core_cols + extra_cols]


def _ensure_table(conn: sqlite3.Connection) -> None:
    """Create the weather table if it doesn't already exist"""
    conn.execute(CREATE_TABLE_SQL)
    conn.commit()


def _insert_rows(conn: sqlite3.Connection, df: pd.DataFrame) -> int:
    """
    Insert rows using INSERT OR IGNORE to handle duplicates gracefully

    Returns the number of newly inserted rows
    """
    db_cols = ["city", "date", "temp_max_f", "temp_min_f",
               "precipitation_in", "windspeed_max_mph", "run_timestamp"]

    # Only include columns that actually exist in the DataFrame
    cols_to_insert = [c for c in db_cols if c in df.columns]
    placeholders = ", ".join("?" for _ in cols_to_insert)
    col_list = ", ".join(cols_to_insert)

    sql = (
        f"INSERT OR IGNORE INTO {TABLE_NAME} ({col_list}) "
        f"VALUES ({placeholders})"
    )

    rows_before = conn.execute(f"SELECT COUNT(*) FROM {TABLE_NAME}").fetchone()[0]
    conn.executemany(sql, df[cols_to_insert].values.tolist())
    conn.commit()
    rows_after = conn.execute(f"SELECT COUNT(*) FROM {TABLE_NAME}").fetchone()[0]

    return rows_after - rows_before