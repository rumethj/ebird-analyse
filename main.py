from data_handler import get_checklists_by_year_for_date_range_tsv_async, collect_checklists_data_async, get_checklist_record_for_checklists
from util import get_date_range, get_data_from_column
import asyncio

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