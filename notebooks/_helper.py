from __future__ import annotations

import sys
from pathlib import Path
import os

# When this file is executed directly (e.g. `uv run notebooks/_helper.py`),
# Python sets sys.path[0] to the script directory (`notebooks/`), so sibling
# packages like `data_handler/` won't be importable. Add the project root.
PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

import pandas as pd
from pandas import DataFrame
from data_handler import ManualDataHandler, UtilDataHandler, WeatherDataHandler, eBirdDataHandler


def taxon_rollup(species_df: DataFrame) -> DataFrame:
    # find subspecies entries
    species_df.drop_duplicates(inplace=True)

    # Define which columns to update (All columns except 'speciesCode' and 'category', 'reportAs' for verifcation)
    cols_to_update = [col for col in species_df.columns if col not in ['speciesCode', 'category', 'reportAs']]

    for row_index, row in species_df.iterrows():
        if row['category'] == "issf":
            # Safety check: ensure reportAs is not empty/NaN
            if pd.isna(row['reportAs']):
                continue

            # find matching species for subspecies
            matches = species_df[species_df['speciesCode'] == row['reportAs']]
            # replace all values of species except for 'speciesCode' in this row
            if not matches.empty:
                species_data = matches.iloc[0]
                species_df.loc[row_index, cols_to_update] = species_data[cols_to_update].values

    return species_df


async def get_complete_dataset(force_collect: bool = False) -> DataFrame:
    # Big dataset path
    util_data_handler = UtilDataHandler()
    complete_data_path =  util_data_handler._get_absolute_path("data/complete/complete_dataset.tsv")

    # check if the dataset already exists
    os.makedirs(os.path.dirname(complete_data_path), exist_ok=True)
    if not force_collect:
        if os.path.exists(complete_data_path) and os.path.getsize(complete_data_path) > 0:
            print("Loading complete dataset from disk...")
            df = pd.read_csv(complete_data_path, sep='\t')
            return df

    ebird_data_handler = eBirdDataHandler()
    checklists_df = await ebird_data_handler.get_checklists_data()
    checklist_records_df = await ebird_data_handler.get_checklist_records_data()

    locations_df = await ebird_data_handler.get_location_data()
    locations_df = locations_df.drop_duplicates()
    observations_df = await ebird_data_handler.get_observations_data()

    manual_data_handler = ManualDataHandler()
    weather_code_df = await manual_data_handler.get_weather_code_data()


    # Combining datasets

    ## Combine Checklists and Checklist Records
    # numSpecies is inconsistent between checklists_df and checklist_records_df, so we exclude it from merge keys
    checklists_expanded_records = pd.merge(checklists_df, checklist_records_df, on=['subId','locId', 'userDisplayName'], how='left')
    print(f"Duplicated check 1: {checklists_expanded_records.duplicated().sum()}")

    checklists_expanded_locations = pd.merge(checklists_expanded_records, locations_df, on=['locId', 'subnational1Code'], how='left')#, validate="m:1")
    print(f"Duplicated check 2: {checklists_expanded_locations.duplicated().sum()}")

    # New additions
    checklist_expanded = pd.merge(checklists_expanded_locations, observations_df, on='subId', how='left')
    print(f"Duplicated check 3: {checklist_expanded.duplicated().sum()}")

    # Add Species Data
    species_df = await ebird_data_handler.get_species_data()
    # Species roll up
    species_df = taxon_rollup(species_df)

    checklist_expanded = pd.merge(checklist_expanded, species_df, on='speciesCode', how='left')
    print(f"Duplicated check 4: {checklist_expanded.duplicated().sum()}")
    
    
    priority_species_df = await manual_data_handler.get_priority_species_data()
    checklist_expanded = pd.merge(checklist_expanded, priority_species_df, on='speciesCode', how='left', suffixes=('', '_priority'))
    print(f"Duplicated check 5: {checklist_expanded.duplicated().sum()}")

    ## Combine Weather data

    ### Prepare super table
    checklist_expanded['isoObsDate'] = pd.to_datetime(checklist_expanded['isoObsDate'])
    ### Convert the Indian Standard Time(IST) to UTC
    checklist_expanded['isoObsDate'] = checklist_expanded['isoObsDate'].dt.tz_localize('Asia/Kolkata').dt.tz_convert('UTC')
    checklists_expanded_f = checklist_expanded.rename(columns={'isoObsDate': 'time_stamp_utc'})
    
    weather_data_handler = WeatherDataHandler()
    weather_df = await weather_data_handler.get_weather_data()
    weather_df = weather_df.drop_duplicates()

    ### Prepare weather table
    weather_df['date'] = pd.to_datetime(weather_df['date'])
    weather_df = weather_df.rename(columns={'date': 'time_stamp_utc'})

    # Ensure both sides use tz-aware UTC timestamps so merge_asof can compare safely.
    # Checklists are converted to UTC above; weather may be tz-naive depending on source.
    if getattr(weather_df['time_stamp_utc'].dt, 'tz', None) is None:
        weather_df['time_stamp_utc'] = weather_df['time_stamp_utc'].dt.tz_localize('UTC')
    else:
        weather_df['time_stamp_utc'] = weather_df['time_stamp_utc'].dt.tz_convert('UTC')

    ### Sort data frames (Required step)
    # merge_asof requires both sides to be sorted by the `on` key. When using `by`,
    # also include the grouping key in the sort order.
    checklists_expanded_f = checklists_expanded_f.sort_values(
        ['time_stamp_utc', 'locId'], kind='mergesort'
    )
    weather_df = weather_df.sort_values(['time_stamp_utc', 'locId'], kind='mergesort')

    ### Merge weather data for nearest time in checklist table for that location
    df = pd.merge_asof(
        checklists_expanded_f,
        weather_df,
        on='time_stamp_utc',
        by='locId',
        direction='nearest',
        tolerance=pd.Timedelta('30 min'),  # Only match if weather time is within 30 minutes of checklist data
        suffixes=('', '_from_weather'),  # In case of overlapping column names
    )

    ### Merge weather code
    df = pd.merge(df, weather_code_df, on='weather_code', how='left', suffixes=('', '_weather_code'))

    # Save the complete dataset to disk for future use
    df.to_csv(complete_data_path, sep='\t', index=False)

    return df
    
    
if __name__ == "__main__":
    import asyncio
    df = asyncio.run(get_complete_dataset(True))
    print(df.info())