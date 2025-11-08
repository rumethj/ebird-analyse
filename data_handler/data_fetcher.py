import httpx
import asyncio
import os
from dotenv import load_dotenv
from datetime import date
import csv

from pandas import DataFrame
# Load environment variables from .env file
load_dotenv()

def ensure_tsv_header(file_path: str, record: dict) -> list[str]:
    os.makedirs(os.path.dirname(file_path), exist_ok=True)
    if os.path.exists(file_path) and os.path.getsize(file_path) > 0:
        with open(file_path, "r", newline="") as f:
            return f.readline().rstrip("\n\r").split("\t")
    else:
        fieldnames = sorted(record.keys())
        with open(file_path, "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames, delimiter="\t")
            writer.writeheader()
        return fieldnames



######################

async def make_ebird_api_request(url: str):
    headers = {"X-eBirdApiToken": os.getenv("EBIRD_API_KEY")}
    async with httpx.AsyncClient() as client:
        response = await client.get(url, headers=headers)
        response.raise_for_status()
        await asyncio.sleep(1)
        data = response.json()
        print(f"Fetched Response for url:{url}")
        return data


async def get_checklist_record_for_checklists(checklist_list : set()) -> None:
    semaphore = asyncio.Semaphore(20)

    async def fetch_one(checklist_id: str) -> tuple[str, dict]:
        async with semaphore:
            return checklist_id, await make_ebird_api_request(f"https://api.ebird.org/v2/product/checklist/view/{checklist_id}")

    # Fetch with concurrency limited to 5 at a time
    result: list[tuple[str, dict]] = await asyncio.gather(*(fetch_one(cl) for cl in checklist_list))

    for checklist_id, record in result:
        file_path = f"data/checklist_records/checklist_records.tsv"
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


async def get_checklists_by_year_for_date_range_tsv_async(date_range: list[date]) -> None:
    semaphore = asyncio.Semaphore(20)

    async def fetch_one(date: date) -> tuple[date, list[dict]]:
        async with semaphore:
            return date, await make_ebird_api_request(f"https://api.ebird.org/v2/product/lists/LK/{date.year}/{date.month}/{date.day}")

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

