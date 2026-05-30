import psycopg2
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

        query = f"""
            INSERT INTO {table_name} (date, message)
            VALUES (NOW(), %s)
        """

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
    parser = argparse.ArgumentParser(description="Insert message into Postgres table")

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

    insert_message(args.message_text, args.table_name)


if __name__ == "__main__":
    main()
