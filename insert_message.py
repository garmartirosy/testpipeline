import psycopg2
from psycopg2 import sql
import argparse
import sys


def insert_message(message_text, table_name):
    try:
        conn = psycopg2.connect(
            host="modelearth-postgres-server.postgres.database.azure.com",
            dbname="industrydb",
            user="postgresadmin",
            password="ModelEarth11!!",
            port=5432,
            sslmode="require"
        )

        cur = conn.cursor()

        query = sql.SQL("""
            INSERT INTO {} (message)
            VALUES (%s)
        """).format(sql.Identifier(table_name))

        cur.execute(query, (message_text,))
        conn.commit()

        print("Message inserted successfully.")

    except Exception as e:
        print("Error inserting message:", e)
        sys.exit(1)

    finally:
        if 'cur' in locals():
            cur.close()

        if 'conn' in locals():
            conn.close()


def main():
    parser = argparse.ArgumentParser(
        description="Insert a message into a PostgreSQL table"
    )

    parser.add_argument(
        "--message_text",
        required=True,
        help="Message text to insert"
    )

    parser.add_argument(
        "--table_name",
        required=True,
        help="Table name to insert into"
    )

    args = parser.parse_args()

    insert_message(
        message_text=args.message_text,
        table_name=args.table_name
    )


if __name__ == "__main__":
    main()
