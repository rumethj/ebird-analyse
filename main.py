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

    # write unique_date_loc_id_pairs to files
    unique_date_loc_id_pairs.to_csv(UtilDataHandler()._get_absolute_path(f"data/weather_helper/date_loc/date_loc.tsv"), sep="\t", index=False)

    return unique_date_loc_id_pairs


async def get_date_range_for_loc():
    # 1. Read the TSV file
    df = pd.read_csv(UtilDataHandler()._get_absolute_path(f"data/weather_helper/date_loc/date_loc.tsv"), sep="\t")

    # 2. Group by 'locId', find min and max of 'date', and reset index
    # Note: Since dates are YYYY-MM-DD, string sorting works exactly like date sorting.
    result = df.groupby('locId')['date'].agg(['min', 'max']).reset_index()

    # 3. Rename columns for clarity (optional)
    result.columns = ['locId', 'start_date', 'end_date']

    # 4. Write to a new TSV file
    result.to_csv(UtilDataHandler()._get_absolute_path(f"data/weather_helper/date_loc/date_range_loc.tsv"), sep="\t", index=False)


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

    await ebird_data_handler.fetch_observations_from_checklist_records()

    await ebird_data_handler.fetch_species_data_from_observations()

    await ebird_data_handler.fetch_loc_data_from_checklists()

    print("eBird Data Gathering Complete")

    print("Preparing to gather Weather Data...")
    # Create date_loc data files (divided into chunks for API limit management)
    date_loc_pairs = await get_loc_date_pairs(await ebird_data_handler.get_checklists_data())
    print("Number of date_loc pairs: ", len(date_loc_pairs))

    # Create location max date and min date
    await get_date_range_for_loc()


    # Create locId look up to for latitude and longitute
    location_data = await ebird_data_handler.get_location_data()
    location_data.drop_duplicates(inplace=True)
    print("Number of locations: ", len(location_data))
    # OPTIMIZATION: Convert location_data to a dictionary for O(1) lookup.
    loc_lookup = location_data.set_index("locId")[["latitude", "longitude"]].to_dict('index')


    print("Gathering Weather Data...")
    weather_data_handler = WeatherDataHandler()

    # await weather_data_handler._clean_weather_data_duplicates()
    
    await weather_data_handler.fetch_weather_data(loc_lookup) # automatically finds date_loc pairs in data/weather_helper/date_loc

    print("Weather Data Gathering Complete")


if __name__ == "__main__":
    asyncio.run(main())