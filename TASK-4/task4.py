import psycopg2

def connect_to_postgresql(pg_host, pg_port, pg_database, pg_user, pg_password):
    try:
        pg_conn = psycopg2.connect(
            host=pg_host,
            port=pg_port,
            database=pg_database,
            user=pg_user,
            password=pg_password
        )
        pg_conn.autocommit = True
        return pg_conn
    except psycopg2.Error as e:
        print(f"Error connecting to PostgreSQL: {e}")
        return None

def connect_to_citus(citus_host, citus_port, citus_database, citus_user, citus_password):
    try:
        citus_conn = psycopg2.connect(
            host=citus_host,
            port=citus_port,
            database=citus_database,
            user=citus_user,
            password=citus_password
        )
        citus_conn.autocommit = True
        return citus_conn
    except psycopg2.Error as e:
        print(f"Error connecting to Citus: {e}")
        return None

def fetch_data_from_postgresql(pg_conn, table_name):
    try:
        with pg_conn.cursor() as pg_cursor:
            pg_cursor.execute(f'SELECT * FROM {table_name}')
            rows = pg_cursor.fetchall()
        return rows
    except psycopg2.Error as e:
        print(f"Error fetching data from PostgreSQL: {e}")
        return None

def ingest_data_into_citus(citus_conn, table_name, rows):
    try:
        with citus_conn.cursor() as citus_cursor:
            for row in rows:
                values = ', '.join(f"'{value}'" for value in row)
                insert_query = f'INSERT INTO {table_name} VALUES ({values})'
                citus_cursor.execute(insert_query)
            print(f"Data from table {table_name} ingested into Citus successfully.")
    except psycopg2.Error as e:
        print(f"Error ingesting data into Citus: {e}")

def main():
    # PostgreSQL connection details
    pg_host = 'localhost'
    pg_port = 5439
    pg_database = 'store'
    pg_user = 'postgres'
    pg_password = ''

    # Citus connection details
    citus_host = 'localhost'
    citus_port = 15432
    citus_database = 'store'
    citus_user = 'postgres'
    citus_password = ''

    # Tables to ingest
    tables = [
        {'name': 'order_details', 'schema': 'CREATE TABLE IF NOT EXISTS order_details (order_detail_id SERIAL PRIMARY KEY, order_id INT, product_id INT, quantity INT, price  NUMERIC(10, 2))'},
        {'name': 'orders', 'schema': 'CREATE TABLE IF NOT EXISTS orders (order_id SERIAL PRIMARY KEY, order_date TIMESTAMP, customer_phone VARCHAR(20))'},
        {'name': 'brands', 'schema': 'CREATE TABLE IF NOT EXISTS brands (brand_id SERIAL PRIMARY KEY, name VARCHAR(100))'},
        {'name': 'products', 'schema': 'CREATE TABLE IF NOT EXISTS products (product_id SERIAL PRIMARY KEY, brand_id INT, name VARCHAR(255), price NUMERIC(10, 2))'}
    ]

    # Connect to PostgreSQL
    pg_conn = connect_to_postgresql(pg_host, pg_port, pg_database, pg_user, pg_password)

    # Connect to Citus
    citus_conn = connect_to_citus(citus_host, citus_port, citus_database, citus_user, citus_password)

    if pg_conn and citus_conn:
        for table_info in tables:
            table_name = table_info['name']
            schema = table_info['schema']

            # Check if table exists in Citus, create if not
            with citus_conn.cursor() as cursor:
                cursor.execute(schema)
                print(f"Table {table_name} created in Citus.")

            # Fetch data from PostgreSQL
            rows = fetch_data_from_postgresql(pg_conn, table_name)
            if rows:
                # Ingest data into Citus
                ingest_data_into_citus(citus_conn, table_name, rows)

        # Close connections
        pg_conn.close()
        citus_conn.close()

if __name__ == "__main__":
    main()