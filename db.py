import sqlite3
import csv
import os


def init_and_populate_db(db_name='finance.db'):
    # Check if the database file exists
    db_exists = os.path.exists(db_name)

    # Connect to the database (creates it if it doesn't exist)
    conn = sqlite3.connect(db_name)
    cursor = conn.cursor()
    
    if not db_exists:
        print(f"Initializing new database: {db_name}")

        # Create tables
        cursor.execute('''
            CREATE TABLE deals (
                id INTEGER PRIMARY KEY,
                user_id INTEGER,
                deal_direction INTEGER,
                deal_status INTEGER,
                deal_time_mcs INTEGER,
                symbol TEXT,
                price REAL,
                requested_volume REAL,
                profit REAL,
                position_id INTEGER,
                filled_volume REAL,
                bid REAL,
                ask REAL
            )
        ''')

        cursor.execute('''
            CREATE TABLE symbols (
                symbol TEXT PRIMARY KEY,
                asset_text TEXT,
                asset_type TEXT,
                contractsize INTEGER
            )
        ''')

        cursor.execute('''
            CREATE TABLE users (
                user_id INTEGER PRIMARY KEY,
                last_access INTEGER,
                name TEXT,
                country TEXT,
                language TEXT,
                balance REAL
            )
        ''')

        # Populate tables from CSV files
        csv_files = {
            'deals': 'data/deals.csv',
            'symbols': 'data/symbols.csv',
            'users': 'data/users.csv'
        }

        for table, file_path in csv_files.items():
            with open(file_path, 'r') as csvfile:
                csv_reader = csv.reader(csvfile)
                next(csv_reader)  # Skip header row
                for row in csv_reader:
                    placeholders = ','.join(['?' for _ in row])
                    cursor.execute(f'INSERT INTO {table} VALUES ({placeholders})', row)

        conn.commit()
        print("Database initialized and populated.")
    else:
        print(f"Database {db_name} already exists.")

        # Check if tables are populated
        for table in ['deals', 'symbols', 'users']:
            cursor.execute(f"SELECT COUNT(*) FROM {table}")
            count = cursor.fetchone()[0]
            print(f"Table '{table}' has {count} rows.")

    conn.close()


def execute_readonly_query(query: str, db_name='finance.db') -> list[dict]:
    conn = sqlite3.connect(db_name)
    conn.set_trace_callback(print)  # This will print SQL statements for debugging
    conn.row_factory = sqlite3.Row  # This allows us to access columns by name
    cursor = conn.cursor()

    try:
        cursor.execute(query)
        rows = cursor.fetchall()
        
        # Convert rows to list of dictionaries
        result = [dict(row) for row in rows]
        
        return result
    except sqlite3.Error as e:
        return f"An error occurred: {e}"
    finally:
        conn.close()



def get_db_schema(db_name='finance.db'):
    conn = sqlite3.connect(db_name)
    cursor = conn.cursor()

    schema = []
    
    # Get table names
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
    tables = cursor.fetchall()
    
    for table in tables:
        table_name = table[0]
        schema.append(f"Table: {table_name}")
        
        # Get column information for each table
        cursor.execute(f"PRAGMA table_info({table_name})")
        columns = cursor.fetchall()
        
        for column in columns:
            col_name = column[1]
            col_type = column[2]
            is_pk = "PRIMARY KEY" if column[5] == 1 else ""
            schema.append(f"  - {col_name} ({col_type}) {is_pk}")
        
        schema.append("")  # Empty line between tables
    
    conn.close()
    
    return "\n".join(schema)


def get_db_description() -> str:
    return """
## Table: users
Holds information about the user account.

| Column name   | Simple description                                                                                 |
|---------------|----------------------------------------------------------------------------------------------------|
| user_id       | Id number for the user                                                                             |
| last_access   | Last time the user logged in to his account, in Unix epoch (seconds since 1970-01- 01 00:00:00+00) |
| name          | Name of the user                                                                                   |
| country       | Country of the user                                                                                |
| language      | Language of the user                                                                               |
| balance       | Current balance of the user in USD.                                                                |

## Table: symbols
Holds information about the financial instruments.

| Column name   | Simple description                                                                                |
|---------------|---------------------------------------------------------------------------------------------------|
| symbol        | The symbol for the financial instrument being traded. Described as (base_currency/quote_currency) |
| asset_text    | A description of the financial instrument being traded.                                           |
| asset_type    | Type of the financial instrument being traded, can be FX or commodities.                          |
| contractsize  | Contract size of the financial instrument being traded, would be useful in metrics  calculation.  |

## Table: deals
Holds information about deals made by the user, whither Trades or internal Transactions (Deposits/Withdrawals) on his account.

| Column name      | Simple description                                                                                                                                         |
|------------------|------------------------------------------------------------------------------------------------------------------------------------------------------------|
| id               | Id for the deal                                                                                                                                            |
| user_id          | Id number for the user                                                                                                                                     |
| deal_direction   | direction of the deal, { 0: BUY Trade , 1: SELL Trade, 2: Internal Transaction}                                                                            |
| deal_status      | Statues of the deal with respect to the position, can have the following values  {0: Opening position, 1: closing position, 3: partially closing position} |
| deal_time_mcs    | Time of recording the deal, millisecond in Unix epoch (millisecond since 1970- 01-01 00:00:00+00)                                                          |
| symbol           | Financial instrument being Traded, have a null value in case of internal  transactions.                                                                    |
| price            | The price of the deal.                                                                                                                                     |
| requested_volume | The requested volume of the deal as Integer, Real Value can be aquired by  dividing on 10000                                                               |
| profit           | Profit made from the deal.                                                                                                                                 |
| position_id      | The position Id associated with the deal.                                                                                                                  |
| filled_volume    | The volume closed by the deal.                                                                                                                             |
| bid              | Bid price at the time of the deal.                                                                                                                         |
| ask              | Ask price at the time of the deal.                                                                                                                         |
"""