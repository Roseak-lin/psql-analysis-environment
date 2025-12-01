import json
import sys
import os

import psycopg2

DB_NAME = "imdb"
DB_USER = None
DB_PASS = None

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
        
def run_plan(plan_path):
    try:
        conn = psycopg2.connect(
            dbname=DB_NAME, user=DB_USER, password=DB_PASS, host='localhost')
        cur = conn.cursor()
        cur.execute(open(plan_path, "r").read())
        res = cur.fetchall()
        
        if not os.path.exists("Output"):
            os.makedirs("Output")
        output_file = plan_path.replace("Plans", "Output").replace(".sql", ".txt")
        with open(output_file, "w") as f:
            for line in res:
                f.write(str(line) + "\n")
        
    except Exception as e:
        print(e)
        sys.exit(1)
        

def main():
    initialize_db_credentials()
    
    # get file names inside "plans" folder
    plan_files = os.listdir("Plans")
    for file in plan_files:
        file_path = os.path.join("Plans", file)
        print(f"Running plan: {file_path}")
        
        run_plan(file_path)
        

if __name__ == "__main__":
    main()