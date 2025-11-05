import httpx
import asyncio
import os
from dotenv import load_dotenv
import datetime
from datetime import date, timedelta, datetime
import json
import time
import csv

from data_collector import collect_checklists_data, collect_checklists_data_async

from pandas import DataFrame
# Load environment variables from .env file
load_dotenv()


class APIClient:

    async def get_api_response(url: str):
        headers = {"X-eBirdApiToken": os.getenv("EBIRD_API_KEY")}
        async with httpx.AsyncClient() as client:
            response = await client.get(url, headers=headers)
            response.raise_for_status()
            await asyncio.sleep(1)
            data = response.json()
            print(f"Fetched reponse for url: {url}")
            return data

    async def write_json_to_tsv(json_data_list: list[dict], file_path :str) -> None:
        """Takes a list of json data and writes it into a given tsv file"""
        if not os.path.exists(os.path.dirname(file_path)):
            os.makedirs(os.path.dirname(file_path), exist_ok=True)
        
        if os.path.exists(file_path) and os.path.getsize(file_path) > 0:
            with open(file_path, "r", newline="") as f:
                first_line = f.readline().rstrip("\n\r")
                fieldnames = first_line.split("\t") if first_line else []
        else:
            keys_set = set()
            for item in json_data_list:
                if isinstance(item, dict):
                    keys_set.update(item.keys())
            fieldnames: list[str] = sorted(keys_set)

        write_header = not os.path.exists(file_path) or os.path.getsize(file_path) == 0
        with open(file_path, "a", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames, delimiter="\t", extrasaction="ignore")
            if write_header and fieldnames:
                writer.writeheader()
            for item in json_data_list:
                if isinstance(item, dict):
                    row = {key: item.get(key, "") for key in fieldnames}
                    writer.writerow(row)





def get_date_range(date_range_str: str) -> list[date]:
    """Create a date list for a date range given in the form YYYY-MM-DD:YYYY-MM-DD"""
    start_date, end_date = date_range_str.split(":")
    start_date = datetime.strptime(start_date, "%Y-%m-%d").date()
    end_date = datetime.strptime(end_date, "%Y-%m-%d").date()
    return [start_date + timedelta(days=i) for i in range((end_date - start_date).days + 1)]


######################

async def get_checklist_record(checklist_id:str):
    headers = {"X-eBirdApiToken": os.getenv("EBIRD_API_KEY")}
    url = f"https://api.ebird.org/v2/product/checklist/view/{checklist_id}"
    async with httpx.AsyncClient() as client:
        response = await client.get(url, headers=headers)
        response.raise_for_status()
        await asyncio.sleep(1)
        data = response.json()
        print(f"Fetched Checklist Record from eBird for checklist: {checklist_id}")
        return data

async def get_checklist_record_for_checklists(checklist_list : set()) -> None:
    semaphore = asyncio.Semaphore(20)

    async def fetch_one(checklist_id: str) -> tuple[str, dict]:
        async with semaphore:
            return checklist_id, await get_checklist_record(checklist_id)

    # Fetch with concurrency limited to 5 at a time
    result: list[tuple[str, dict]] = await asyncio.gather(*(fetch_one(cl) for cl in checklist_list))

    for checklist_id, record in result:
        file_path = f"data/checklist_records/checklist_records.tsv"
        if not os.path.exists(os.path.dirname(file_path)):
            os.makedirs(os.path.dirname(file_path), exist_ok=True)
        
        if os.path.exists(file_path) and os.path.getsize(file_path) > 0:
            with open(file_path, "r", newline="") as f:
                first_line = f.readline().rstrip("\n\r")
                fieldnames = first_line.split("\t") if first_line else []
        else:
            keys_set = set()
            if isinstance(record, dict):
                keys_set.update(record.keys())
            fieldnames: list[str] = sorted(keys_set)

        write_header = not os.path.exists(file_path) or os.path.getsize(file_path) == 0
        with open(file_path, "a", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames, delimiter="\t", extrasaction="ignore")
            if write_header and fieldnames:
                writer.writeheader()
            if isinstance(record, dict):
                row = {key: record.get(key, "") for key in fieldnames}
                writer.writerow(row)
        print(f"Saved record for {checklist_id}")


##################




async def get_checklists_for_date(target_date: date) -> list[dict]:
    headers = {"X-eBirdApiToken": os.getenv("EBIRD_API_KEY")}
    url = f"https://api.ebird.org/v2/product/lists/LK/{target_date.year}/{target_date.month}/{target_date.day}"
    async with httpx.AsyncClient() as client:
        response = await client.get(url, headers=headers)
        response.raise_for_status()
        await asyncio.sleep(1)
        data = response.json()
        print(f"Fetched Data from eBird for date: {target_date}")
        return data


async def get_checklists_by_year_for_date_range_tsv_async(date_range: list[date]) -> None:
    semaphore = asyncio.Semaphore(20)

    async def fetch_one(checklist_id: date) -> tuple[date, list[dict]]:
        async with semaphore:
            return date, await get_checklist_record(date)

    # Fetch with concurrency limited to 5 at a time
    results: list[tuple[date, list[dict]]] = await asyncio.gather(*(fetch_one(d) for d in date_range))

    # Write sequentially to avoid concurrent writes to the same year file
    for d, checklists in sorted(results, key=lambda x: x[0]):
        file_path = f"data/checklists/checklists_{d.year}.tsv"
        if not os.path.exists(os.path.dirname(file_path)):
            os.makedirs(os.path.dirname(file_path), exist_ok=True)

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
        print(f"Saved checklists for {d}")


def get_data_from_column(df: DataFrame, column:str) -> set():
    return set(df[column].tolist())


async def main_async():

    # Save Checklists Data from API
    if False:
        print("Starting data gathering (async)...")
        date_range_str = "2019-01-01:2025-11-01"
        date_range_list = get_date_range(date_range_str)
        print(f"Found {len(date_range_list)} dates to gather data for")
        await get_checklists_by_year_for_date_range_tsv_async(date_range_list)


    # Save Checklist Records Data from API
    if True:
        checklist_df = await collect_checklists_data_async()
        checklist_set = get_data_from_column(checklist_df, "subID")
        print(len(checklist_set))
        #checklist_set = {"S155667850", "S63647230", "S176770639"}
        await get_checklist_record_for_checklists(checklist_set)


    print("Main Complete")



if __name__ == "__main__":
    asyncio.run(main_async())