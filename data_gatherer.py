import httpx
import os
from dotenv import load_dotenv
import datetime
from datetime import date, timedelta, datetime
import json
import time
import csv
# Load environment variables from .env file
load_dotenv()




def get_date_range(date_range_str: str) -> list[date]:
    start_date, end_date = date_range_str.split(":")
    start_date = datetime.strptime(start_date, "%Y-%m-%d").date()
    end_date = datetime.strptime(end_date, "%Y-%m-%d").date()
    return [start_date + timedelta(days=i) for i in range((end_date - start_date).days + 1)]


def get_checklists_for_date(date: date) -> list[dict]:
    
    headers = {}
    headers["X-eBirdApiToken"] = os.getenv("EBIRD_API_KEY")

    url = f"https://api.ebird.org/v2/product/lists/LK/{date.year}/{date.month}/{date.day}"
    
    response = httpx.get(url, headers=headers)
    response.raise_for_status()
    time.sleep(1)
    data = response.json()
    return data


def get_checklists_by_year_for_date_range_json(date_range: list[date]) -> list[dict]:
    for date in date_range:
        checklists = get_checklists_for_date(date)
        file_path = f"./data/checklists/checklists_{date.year}.json"
        # Create the file if it doesn't exist
        if not os.path.exists(file_path):
            os.makedirs(os.path.dirname(file_path), exist_ok=True)
        with open(file_path, "a") as f:
            json.dump(checklists, f)
        print(f"Saved checklists for {date}")


def get_checklists_by_year_for_date_range_tsv(date_range: list[date]) -> None:
    for date in date_range:
        checklists = get_checklists_for_date(date)
        file_path = f"./data/checklists/checklists_{date.year}.tsv"
        # Ensure directory exists
        if not os.path.exists(os.path.dirname(file_path)):
            os.makedirs(os.path.dirname(file_path), exist_ok=True)

        # Determine header fields: if file exists, read header; else compute from current batch
        if os.path.exists(file_path) and os.path.getsize(file_path) > 0:
            with open(file_path, "r", newline="") as f:
                first_line = f.readline().rstrip("\n\r")
                fieldnames = first_line.split("\t") if first_line else []
        else:
            keys_set = set()
            for item in checklists:
                if isinstance(item, dict):
                    keys_set.update(item.keys())
            fieldnames: list[str] = sorted(keys_set)

        write_header = not os.path.exists(file_path) or os.path.getsize(file_path) == 0
        with open(file_path, "a", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames, delimiter="\t", extrasaction="ignore")
            if write_header and fieldnames:
                writer.writeheader()
            for item in checklists:
                if isinstance(item, dict):
                    row = {key: item.get(key, "") for key in fieldnames}
                    writer.writerow(row)
        print(f"Saved checklists for {date}")


def main():
    print("Starting data gathering...")
    date_range_str = "2025-10-29:2025-11-01"
    date_range_list = get_date_range(date_range_str)
    print(f"Found {len(date_range_list)} dates to gather data for")


    get_checklists_by_year_for_date_range_tsv(date_range_list)
    # print("Data gathering complete")


if __name__ == "__main__":
    main()