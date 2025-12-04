# from data_handler import get_checklists_by_year_for_date_range_tsv_async, collect_checklists_data_async, get_checklist_record_for_checklists
from data_handler import eBirdDataHandler, WeatherDataHandler, UtilDataHandler
from util import get_date_range, get_data_from_column
from datetime import date
import asyncio
import pandas as pd
from pandas import DataFrame


async def get_loc_date_pairs(checklist_df: DataFrame) -> DataFrame():
    # for each unique locationID and date pair
    
    checklist_date_loc_id_pairs = checklist_df[["isoObsDate", "locId"]].drop_duplicates()

    # Convert ISO time to date object
    checklist_date_loc_id_pairs["isoObsDate"] = pd.to_datetime(checklist_date_loc_id_pairs["isoObsDate"])
    checklist_date_loc_id_pairs["date"] = checklist_date_loc_id_pairs["isoObsDate"].dt.date

    # Get unique dates and location IDs pairs
    unique_date_loc_id_pairs = checklist_date_loc_id_pairs[["date", "locId"]].drop_duplicates()
    print(f"Number of rows of loc_date pairs: {len(unique_date_loc_id_pairs)}")

    # write unique_date_loc_id_pairs to files to save state (4000 rows max per file)
    max_rows = 4500
    i = 0
    print(f"Number of files to create: {len(unique_date_loc_id_pairs) // max_rows + 1}")
    while True:
        i+=1
        chunk = unique_date_loc_id_pairs.iloc[(i-1)*max_rows:i*max_rows]
        if chunk.empty:
            break
        chunk.to_csv(UtilDataHandler()._get_absolute_path(f"data/weather_helper/date_loc/date_loc_{i}.tsv"), sep="\t", index=False)

    return unique_date_loc_id_pairs



async def main():

    print("Gathering eBird Data...")
    ebird_data_handler = eBirdDataHandler()

    # Set date range to get data for
    date_range_str = "2019-01-01:2025-11-01"
    date_range_list = get_date_range(date_range_str)
    print(f"Found {len(date_range_list)} dates to gather data for")

    # Fetch Checklists Data for date range
    await ebird_data_handler.fetch_checklists_by_year_for_date_range_tsv(date_range_list)

    # Fetch Checklists Records for each
    await ebird_data_handler.fetch_checklist_record_for_checklists() # Auto detects checklist

    await ebird_data_handler.fetch_loc_data_from_checklists()

    print("eBird Data Gathering Complete")


    print("Preparing to gather Weather Data...")
    # Create date_loc data files (divided into chunks for API limit management)
    await get_loc_date_pairs(await ebird_data_handler.get_checklists_data())

    # Create locId look up to for latitude and longitute
    location_data = await ebird_data_handler.get_location_data()
    location_data.drop_duplicates(inplace=True)
    # OPTIMIZATION: Convert location_data to a dictionary for O(1) lookup.
    loc_lookup = location_data.set_index("locId")[["latitude", "longitude"]].to_dict('index')

    print("Gathering Weather Data...")
    weather_data_handler = WeatherDataHandler()
    
    await weather_data_handler.fetch_weather_data_for_date_loc_pairs(loc_lookup) # automatically finds date_loc pairs in data/weather_helper/date_loc

    print("Weather Data Gathering Complete")



if __name__ == "__main__":
    asyncio.run(main())