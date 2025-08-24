from src.dashboard_management.refreshing_dashboard import refresh_database_views, update_dashboard_timestamp, refresh_powerbi_dataset
from sql.management.create_databse_views import insert_into_views
from src.database.processed_into_database import insert_data_from_csv
from src.cleaning.data_checking import check_data
from src.scraping.site_scraping import scrape_all_pages
import os
import sys
import logging
from datetime import datetime
import traceback

sys.path.append(os.path.join(os.path.dirname(__file__), 'src'))


logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('pipeline.log'),
        logging.StreamHandler(sys.stdout)
    ]
)


def run_scraping_stage():
    logging.info("Starting data scraping stage...")
    try:
        df = scrape_all_pages(max_pages=10)
        logging.info(f"Scraping completed: {len(df)} reports collected")
        return True
    except Exception as e:
        logging.error(f"Scraping failed: {e}")
        logging.error(traceback.format_exc())
        return False


def run_data_processing_stage():
    logging.info("Starting data processing stage...")
    try:
        import pandas as pd

        script_dir = os.path.dirname(os.path.abspath(__file__))
        raw_csv_path = os.path.join(
            script_dir, 'data', 'raw', 'police_reports.csv')
        processed_csv_path = os.path.join(
            script_dir, 'data', 'processed', 'police_reports.csv')

        if not os.path.exists(raw_csv_path):
            logging.error("Raw data file not found")
            return False

        df = pd.read_csv(raw_csv_path)
        cleaned_df = check_data(df)
        cleaned_df.to_csv(processed_csv_path, index=False, encoding='utf-8')

        logging.info(
            f"Data processing completed: {len(cleaned_df)} clean records")
        return True
    except Exception as e:
        logging.error(f"Data processing failed: {e}")
        logging.error(traceback.format_exc())
        return False


def run_database_loading_stage():
    logging.info("Starting database loading stage...")
    try:
        script_dir = os.path.dirname(os.path.abspath(__file__))
        processed_csv_path = os.path.join(
            script_dir, 'data', 'processed', 'police_reports.csv')

        if not os.path.exists(processed_csv_path):
            logging.error("Processed data file not found")
            return False

        insert_data_from_csv(processed_csv_path)
        logging.info("Database loading completed successfully")
        return True
    except Exception as e:
        logging.error(f"Database loading failed: {e}")
        logging.error(traceback.format_exc())
        return False


def run_views_creation_stage():
    logging.info("Starting database views creation stage...")
    try:
        insert_into_views()
        logging.info("Database views creation completed successfully")
        return True
    except Exception as e:
        logging.error(f"Views creation failed: {e}")
        logging.error(traceback.format_exc())
        return False


def run_dashboard_refresh_stage():
    logging.info("Starting dashboard refresh stage...")
    try:
        refresh_database_views()
        update_dashboard_timestamp()
        refresh_powerbi_dataset()
        logging.info("Dashboard refresh completed successfully")
        return True
    except Exception as e:
        logging.error(f"Dashboard refresh failed: {e}")
        logging.error(traceback.format_exc())
        return False


def ensure_directories():
    directories = [
        'data/raw',
        'data/processed',
        'logs'
    ]

    for directory in directories:
        if not os.path.exists(directory):
            os.makedirs(directory)
            logging.info(f"Created directory: {directory}")


def run_full_pipeline():
    logging.info("="*60)
    logging.info("POLICE REPORTS PIPELINE STARTED")
    logging.info(f"Start time: {datetime.now()}")
    logging.info("="*60)

    ensure_directories()

    pipeline_stages = [
        ("Data Scraping", run_scraping_stage),
        ("Data Processing", run_data_processing_stage),
        ("Database Loading", run_database_loading_stage),
        ("Views Creation", run_views_creation_stage),
        ("Dashboard Refresh", run_dashboard_refresh_stage)
    ]

    failed_stages = []

    for stage_name, stage_function in pipeline_stages:
        logging.info(f"\n--- {stage_name.upper()} STAGE ---")

        success = stage_function()

        if success:
            logging.info(f"✓ {stage_name} completed successfully")
        else:
            logging.error(f"✗ {stage_name} failed")
            failed_stages.append(stage_name)

    logging.info("\n" + "="*60)
    logging.info("PIPELINE EXECUTION SUMMARY")
    logging.info(f"End time: {datetime.now()}")

    if failed_stages:
        logging.error(f"Failed stages: {', '.join(failed_stages)}")
        logging.error("PIPELINE COMPLETED WITH ERRORS")
        return False
    else:
        logging.info("ALL STAGES COMPLETED SUCCESSFULLY")
        logging.info("PIPELINE EXECUTION SUCCESSFUL")
        return True


def run_specific_stage(stage_name):
    stage_mapping = {
        'scraping': run_scraping_stage,
        'processing': run_data_processing_stage,
        'database': run_database_loading_stage,
        'views': run_views_creation_stage,
        'dashboard': run_dashboard_refresh_stage
    }

    if stage_name.lower() in stage_mapping:
        logging.info(f"Running specific stage: {stage_name}")
        return stage_mapping[stage_name.lower()]()
    else:
        logging.error(f"Unknown stage: {stage_name}")
        logging.info(f"Available stages: {', '.join(stage_mapping.keys())}")
        return False


def main():
    if len(sys.argv) > 1:
        stage_name = sys.argv[1]
        success = run_specific_stage(stage_name)
    else:
        success = run_full_pipeline()

    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
