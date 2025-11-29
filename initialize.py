import os
import json
import shutil
import psycopg2
from psycopg2 import sql
import subprocess
import sys

# --- CONFIGURATION ---
ADMIN_DB = "postgres"
TARGET_DB = "temp"
DB_USER = None
DB_PASS = None
DB_HOST = "localhost" 

FILE_DIR = "DB"
NAMES = [
    "name.basics.tsv", "title.basics.tsv", "title.crew.tsv",
    "title.principals.tsv", "title.ratings.tsv"
]

# --- 1. DATABASE CREATION PHASE ---
TABLE_SCHEMAS = {
    "name_basics": {
        "nconst": "VARCHAR(15)",
        "primaryname": "TEXT",
        "birthyear": "INTEGER",
        "deathyear": "INTEGER",
        "primaryprofession": "TEXT",
        "knownfortitles": "TEXT"
    },
    "title_akas": {
        "titleid": "VARCHAR(15)",
        "ordering": "INTEGER",
        "title": "TEXT",
        "region": "VARCHAR(10)",
        "language": "VARCHAR(10)",
        "types": "TEXT",
        "attributes": "TEXT",
        "isoriginaltitle": "BOOLEAN"
    },
    "title_basics": {
        "tconst": "VARCHAR(15)",
        "titletype": "VARCHAR(50)",
        "primarytitle": "TEXT",
        "originaltitle": "TEXT",
        "isadult": "BOOLEAN",
        "startyear": "INTEGER",
        "endyear": "INTEGER",
        "runtimeminutes": "INTEGER",
        "genres": "TEXT"
    },
    "title_crew": {
        "tconst": "VARCHAR(15)",
        "directors": "TEXT",
        "writers": "TEXT"
    },
    "title_episode": {
        "tconst": "VARCHAR(15)",
        "parenttconst": "VARCHAR(15)",
        "seasonnumber": "INTEGER",
        "episodenumber": "INTEGER"
    },
    "title_principals": {
        "tconst": "VARCHAR(15)",
        "ordering": "INTEGER",
        "nconst": "VARCHAR(15)",
        "category": "VARCHAR(50)",
        "job": "TEXT",
        "characters": "TEXT"
    },
    "title_ratings": {
        "tconst": "VARCHAR(15)",
        "averagerating": "REAL",
        "numvotes": "INTEGER"
    }
}

def initialize_db_credentials():
    global DB_USER, DB_PASS
    try:
        with open('credentials.json', 'r') as f:
            creds = json.load(f)
            DB_USER = creds['database']['user']
            DB_PASS = creds['database']['password']
    except Exception as e:
        print("Error loading database credentials from 'credentials.json'. Ensure the file exists and is properly formatted.")
        print(e)
        sys.exit(1)

def run_decompression_script(script_name="decompress.sh"):
    # 1. Check if the script exists in the current folder
    if not os.path.exists(script_name):
        print(f"Error: The file '{script_name}' was not found in {os.getcwd()}")
        return

    command = []

    # Windows-specific handling (using Git Bash)
    if sys.platform == "win32":
        bash_executable = shutil.which("bash")
        if not bash_executable:
            print("Error: 'bash' not found. Please install Git Bash and add it to PATH.")
            return
        command = [bash_executable, script_name]
    # Linux/Mac handling
    else:
        subprocess.run(["chmod", "+x", script_name], check=True)
        command = ["./" + script_name]

    # 2. Execute
    try:
        print(f"Running {script_name}...")
        result = subprocess.run(command, check=True, text=True, capture_output=True)
        print("Success!\n", result.stdout)
    except subprocess.CalledProcessError as e:
        print("Error running script:\n", e.stderr)
    except Exception as e:
        print(f"Unexpected error: {e}")

def setup_database():
    print(f"Setting up database '{TARGET_DB}'")
    
    # Connect to the default admin DB
    try:
        admin_conn = psycopg2.connect(
            dbname=ADMIN_DB, user=DB_USER, password=DB_PASS, host=DB_HOST
        )
        admin_conn.autocommit = True
        admin_cur = admin_conn.cursor()
        print(f"Connected to administrative database '{ADMIN_DB}'.")
    except Exception as e:
        print(f"Error connecting to the administrative database '{ADMIN_DB}'. Check credentials/host.")
        print(e)
        sys.exit(1)

    # Check if 'temp' database exists
    admin_cur.execute(f"SELECT 1 FROM pg_database WHERE datname = '{TARGET_DB}'")
    if admin_cur.fetchone():
        print(f"Database '{TARGET_DB}' already exists.")
    else:
        print(f"Creating database '{TARGET_DB}'...")
        try:
            admin_cur.execute(sql.SQL(f"CREATE DATABASE {TARGET_DB}"))
            print("Database created successfully.")
        except Exception as e:
            print(f"Could not create database. Check user privileges. Error: {e}")
            sys.exit(1)
            
    admin_conn.close()

# --- 2. TABLE CREATION & DATA LOADING PHASE ---

def create_tables_and_load_data():
    """Connects to target DB, creates tables, and loads data."""
    print(f"\n--- 2. Connecting to '{TARGET_DB}' and loading data ---")

    # Connect to the newly created/verified target database
    try:
        conn = psycopg2.connect(
            dbname=TARGET_DB, user=DB_USER, password=DB_PASS, host=DB_HOST
        )
        cur = conn.cursor()
    except Exception as e:
        print(f"Error connecting to target database '{TARGET_DB}'. Error: {e}")
        sys.exit(1)

    # A. CREATE TABLES
    for table_name, columns in TABLE_SCHEMAS.items():
        column_defs = ", ".join(f"{col} {data_type}" for col, data_type in columns.items())
        create_table_sql = sql.SQL(
            "CREATE TABLE IF NOT EXISTS {} ({});"
        ).format(
            sql.Identifier(table_name),
            sql.SQL(column_defs)
        )
        
        try:
            cur.execute(create_table_sql)
            print(f"Table '{table_name}' checked/created.")
        except Exception as e:
            print(f"Error creating table {table_name}: {e}")

    # B. LOAD DATA
    for file_name in FILE_NAMES:
        table_name = file_name.replace('.', '_').replace('_tsv', '')
        file_path = f"{FILE_DIR}/{file_name}"
        
        absolute_path = os.path.abspath(file_path)
        print(f"DEBUG: Python is looking for file at: {absolute_path}")
        
        try:
            current_columns = list(TABLE_SCHEMAS[table_name].keys())
        except KeyError:
            print(f"ERROR: Schema for {table_name} not found in TABLE_SCHEMAS. Skipping.")
            continue
        columns = sql.SQL(', ').join(map(sql.Identifier, current_columns))
        
        copy_sql = sql.SQL(
            "COPY {} FROM STDIN WITH (FORMAT csv, DELIMITER E'\t', HEADER, QUOTE E'\b', NULL '\\N')"
        ).format(sql.Identifier(table_name))
        
        print(f"Loading {file_name} into {table_name}...")

        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                # cur.copy_from(f, table_name, sep='\t', null='\\N', columns=TABLE_SCHEMAS[table_name].keys())
                cur.copy_expert(copy_sql, f)
            print("Load complete.")
        except FileNotFoundError:
            print(f"ERROR: File not found at {file_path}. Skipping.")
        except psycopg2.Error as e:
            print(f"ERROR loading {file_name}: {e}")
            conn.rollback() 
            
    conn.commit()
    conn.close()
    print("\nAll operations complete and committed.")

if __name__ == "__main__":
    initialize_db_credentials()
    run_decompression_script("decompress.sh")
    
    setup_database()
    create_tables_and_load_data()