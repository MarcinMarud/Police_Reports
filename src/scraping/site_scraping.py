import requests
from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime
import time

BASE_URL = "https://policja.pl/pol/form/1,Informacja-dzienna.html"
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
}


def fetch_daily_report(page_number):
    URL = f"{BASE_URL}?page={page_number}"
    print(f"Fetching URL: {URL}")

    try:
        response = requests.get(URL, headers=HEADERS)
        response.raise_for_status()

        print(f"Status code: {response.status_code}")
        print(f"Content length: {len(response.content)}")

        soup = BeautifulSoup(response.content, 'html.parser')
        return soup

    except requests.exceptions.RequestException as e:
        print(f"Failed to fetch data for page {page_number}: {e}")
        return None


def extract_page_data(soup):
    reports = []
    daily_info_sections = soup.find_all('div', class_='subAll')

    print(f"Found {len(daily_info_sections)} subAll sections")

    for section in daily_info_sections:
        try:
            box_div = section.find('div', class_='box')
            if box_div:
                table = box_div.find(
                    'table', class_='table-listing table-striped margin_b20')
                if table:
                    headers = []
                    thead = table.find('thead')
                    if thead:
                        header_cells = thead.find_all('th')
                        headers = [cell.text.strip() for cell in header_cells]
                        print(f"Found headers: {headers}")

                    tbody = table.find('tbody')
                    if tbody:
                        rows = tbody.find_all('tr')
                        for row in rows:
                            cells = row.find_all('td')
                            if cells:
                                row_data = {
                                    headers[i]: cell.text.strip()
                                    for i, cell in enumerate(cells)
                                    if i < len(headers)
                                }
                                reports.append(row_data)
                                print(
                                    f"Found row: {list(row_data.values())[:100]}")

        except AttributeError as e:
            print(f"Error extracting data: {e}")
            continue

    return reports


def scrape_all_pages(max_pages=10):
    all_reports = []

    for page in range(max_pages):
        print(f"Scraping page {page}...")
        soup = fetch_daily_report(page)

        if soup is None:
            print(f"Failed to fetch page {page}, moving to next page")
            continue

        page_reports = extract_page_data(soup)
        all_reports.extend(page_reports)

        time.sleep(2)

    df = pd.DataFrame(all_reports)

    output_path = "data/raw/police_reports.csv"
    df.to_csv(output_path, index=False, encoding='utf-8')

    return df


if __name__ == "__main__":
    df = scrape_all_pages()
    print(f"Scraped {len(df)} reports successfully!")
