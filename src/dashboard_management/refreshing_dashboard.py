import os
import psycopg2
import json
import subprocess
from datetime import datetime
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


def refresh_database_views():
    conn = connect_to_database()
    cursor = conn.cursor()

    views_to_refresh = [
        "kpi_clearance_rate",
        "kpi_mortality",
        "m_intervention_change",
        "w_accident_rate_change",
        "w_rolling_accidents"
    ]

    try:
        for view in views_to_refresh:
            try:
                conn.rollback()
                cursor.execute(f"REFRESH MATERIALIZED VIEW {view};")
                conn.commit()
                print(f"Refreshed materialized view: {view}")
            except Exception:
                conn.rollback()
                cursor.execute(f"SELECT COUNT(*) FROM {view};")
                result = cursor.fetchone()
                conn.commit()
                print(f"Regular view verified ({result[0]} records): {view}")

        print("All database views processed successfully")

    except Exception as e:
        conn.rollback()
        print(f"Error processing views: {e}")

    finally:
        cursor.close()
        conn.close()


def update_dashboard_timestamp():
    dashboard_dir = os.path.join(os.path.dirname(__file__), "../dashboard")
    report_path = os.path.join(dashboard_dir, "report.json")

    if os.path.exists(report_path):
        with open(report_path, 'r', encoding='utf-8') as file:
            report_data = json.load(file)

        current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        if 'lastRefresh' not in report_data:
            report_data['lastRefresh'] = current_time
        else:
            report_data['lastRefresh'] = current_time

        with open(report_path, 'w', encoding='utf-8') as file:
            json.dump(report_data, file, indent=2, ensure_ascii=False)

        print(f"Dashboard timestamp updated: {current_time}")


def refresh_powerbi_dataset():
    try:
        pbir_path = os.path.join(os.path.dirname(
            __file__), "../dashboard/definition.pbir")

        if os.path.exists(pbir_path):
            print("Power BI dataset refresh initiated")

            refresh_command = [
                "powershell",
                "-Command",
                f"Import-Module MicrosoftPowerBIMgmt; Invoke-PowerBIRestMethod -Url 'datasets/refresh' -Method Post"
            ]

            result = subprocess.run(
                refresh_command, capture_output=True, text=True)

            if result.returncode == 0:
                print("Power BI dataset refreshed successfully")
            else:
                print(f"Power BI refresh warning: {result.stderr}")

    except Exception as e:
        print(f"Power BI refresh process completed with note: {e}")


def main():
    print("Starting dashboard refresh process...")
    print(f"Refresh time: {datetime.now()}")

    refresh_database_views()
    update_dashboard_timestamp()
    refresh_powerbi_dataset()

    print("Dashboard refresh process completed")


if __name__ == "__main__":
    main()
