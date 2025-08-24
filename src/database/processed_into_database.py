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


def insert_data_from_csv(csv_path):
    df = pd.read_csv(csv_path)

    column_mapping = {
        "Data": "report_date",
        "Interwencje": "interventions",
        "Zatrzymani na gorącym uczynku": "caught_in_the_act",
        "Zatrzymani poszukiwani": "wanted_persons_arrested",
        "Kierujący po spożyciu alkoholu": "dui_drivers",
        "Wypadki drogowe": "road_accidents",
        "Zabici w wypadkach": "accident_fatalities",
        "Ranni w wypadkach": "accident_injuries"
    }

    df.rename(columns=column_mapping, inplace=True)

    connection = connect_to_database()
    cursor = connection.cursor()

    for _, row in df.iterrows():
        cursor.execute(
            """
            INSERT INTO police_reports (
                report_date,
                interventions,
                caught_in_the_act,
                wanted_persons_arrested,
                dui_drivers,
                road_accidents,
                accident_fatalities,
                accident_injuries
            )
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
            """,
            (
                row["report_date"],
                row["interventions"],
                row["caught_in_the_act"],
                row["wanted_persons_arrested"],
                row["dui_drivers"],
                row["road_accidents"],
                row["accident_fatalities"],
                row["accident_injuries"]
            )
        )

    connection.commit()
    cursor.close()
    connection.close()


if __name__ == "__main__":
    csv_file = os.path.join(os.path.dirname(__file__),
                            "../../data/processed/police_reports.csv")
    insert_data_from_csv(csv_file)
    print("Data inserted successfully into police_reports table")
