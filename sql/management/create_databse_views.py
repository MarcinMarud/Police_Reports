import os
import psycopg2
import pandas as pd
from dotenv import load_dotenv

dotenv_path = os.path.join(os.path.dirname(__file__), "../../.env")
load_dotenv(dotenv_path=dotenv_path)

DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")


def connect_to_database():
    return psycopg2.connect(
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
        host=DB_HOST,
        port=DB_PORT
    )


def get_sql_files():
    queries_dir = os.path.join(os.path.dirname(__file__), "../queries")
    sql_files = []

    for file in os.listdir(queries_dir):
        if file.endswith('.sql'):
            sql_files.append(os.path.join(queries_dir, file))

    return sql_files


def read_sql_content(file_path):
    with open(file_path, 'r', encoding='utf-8') as file:
        return file.read()


def create_view_from_sql(cursor, sql_content, view_name):
    drop_view_sql = f"DROP VIEW IF EXISTS {view_name} CASCADE;"
    cursor.execute(drop_view_sql)

    create_view_sql = f"CREATE VIEW {view_name} AS {sql_content}"
    cursor.execute(create_view_sql)


def insert_into_views():
    conn = connect_to_database()
    cursor = conn.cursor()

    try:
        sql_files = get_sql_files()

        for sql_file_path in sql_files:
            file_name = os.path.basename(sql_file_path)
            view_name = os.path.splitext(file_name)[0]

            sql_content = read_sql_content(sql_file_path)

            create_view_from_sql(cursor, sql_content, view_name)
            print(f"Created view: {view_name}")

        conn.commit()
        print("All views created successfully!")

    except Exception as e:
        conn.rollback()
        print(f"Error creating views: {e}")

    finally:
        cursor.close()
        conn.close()


if __name__ == "__main__":
    insert_into_views()
