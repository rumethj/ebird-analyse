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
    observations_df = await ebird_data_handler.get_observations_data()
    species_df = await ebird_data_handler.get_species_data()

    manual_data_handler = ManualDataHandler()
    priority_species_df = await manual_data_handler.get_priority_species_data()
    weather_code_df = await manual_data_handler.get_weather_code_data()

    weather_data_handler = WeatherDataHandler()
    weather_df = await weather_data_handler.get_weather_data()

    # Combining datasets

    ## Combine Checklists and Checklist Records
    checklists_expanded_records = pd.merge(checklists_df, checklist_records_df, on=['subId','locId', 'numSpecies', 'userDisplayName'], how='left')
    checklists_expanded_locations = pd.merge(checklists_expanded_records, locations_df, on=['locId', 'subnational1Code'], how='left')

    # New additions
    checklist_expanded = pd.merge(checklists_expanded_locations, observations_df, on='subId', how='left')
    checklist_expanded = pd.merge(checklist_expanded, species_df, on='speciesCode', how='left')
    checklist_expanded = pd.merge(checklist_expanded, priority_species_df, on='speciesCode', how='left', suffixes=('', '_priority'))

    # # Any species not present in priority_species_df should default to False
    # for col in ['isHighValue_LK', 'isEndemic_LK']:
    #     if col not in checklist_expanded.columns:
    #         checklist_expanded[col] = False
    #     else:
    #         # Use pandas' nullable boolean dtype to avoid FutureWarning about
    #         # silent downcasting during fillna on object dtype columns.
    #         checklist_expanded[col] = (
    #                                     checklist_expanded[col]
    #                                     .astype("boolean")  # <--- Convert to nullable boolean first
    #                                     .fillna(False)      # Now fillna works safely without warning
    #                                     .astype(bool))

    ## Combine Weather data

    ### Prepare super table
    checklist_expanded['isoObsDate'] = pd.to_datetime(checklist_expanded['isoObsDate'])
    ### Convert the Indian Standard Time(IST) to UTC
    checklist_expanded['isoObsDate'] = checklist_expanded['isoObsDate'].dt.tz_localize('Asia/Kolkata').dt.tz_convert('UTC')
    checklists_expanded_f = checklist_expanded.rename(columns={'isoObsDate': 'time_stamp_utc'})

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