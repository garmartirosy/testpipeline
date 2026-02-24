import psycopg2


def insert_message(message_text, table_name):
    conn = psycopg2.connect(
        host="airflow-metadata-postgres-db-server.postgres.database.azure.com",
        dbname="postgres",
        user="postgres",
        password="Mqcgnysu1-",
        port=5432,
        sslmode="require"
    )

    cur = conn.cursor()

    query = f"""
        INSERT INTO {table_name} (date, message)
        VALUES (NOW(), %s)
    """

    cur.execute(query, (message_text,))

    conn.commit()
    cur.close()
    conn.close()

    print("Message inserted successfully.")
