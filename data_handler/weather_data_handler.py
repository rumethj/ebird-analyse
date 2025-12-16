"""Weather data handler for managing historical weather data.

This module provides functionality to:
- Read weather data from TSV files
- Write weather data to TSV files
- Organize weather data by location and date

The handler extends DataHandler to provide specialized functionality for
working with weather data stored in TSV format.
"""

from hashlib import file_digest
import os
import pandas as pd
from pandas import DataFrame
from dataclasses import dataclass, field
from typing import Optional, Dict, Any, List
from datetime import date, datetime
from .base import DataHandler, TSVConfig
import asyncio

import openmeteo_requests
import requests_cache
from retry_requests import retry



# Cloud Cover Total (cloudcover)
# Definition: The total percentage of the sky covered by clouds when looking up from the ground.
# Calculation: It is NOT a simple sum of Low + Mid + High.[2]
# Because clouds overlap (a low cloud can hide a high cloud), the total cover is calculated using a weighted formula.
# Example: If you have 100% low clouds and 100% high clouds, the Total is still 100% (not 200%), because you cannot see the high clouds through the low ones.


@dataclass
class WeatherDataHandler(DataHandler):
        
    file_lock: asyncio.Lock = field(default_factory=asyncio.Lock)

    def __post_init__(self):
        # Setup the Open-Meteo API client with cache and retry on error
        cache_session = requests_cache.CachedSession('.cache', expire_after = -1)
        retry_session = retry(cache_session, retries = 5, backoff_factor = 0.2)
        self.openmeteo = openmeteo_requests.Client(session = retry_session)

        self.date_loc_file = self._get_absolute_path("data/weather_helper/date_loc/date_range_loc.tsv")
        self.date_loc_status_file_path = self._get_absolute_path("data/weather_helper/date_loc_status/done.tsv")
        self.weather_data_path = self._get_absolute_path("data/weather/weather.tsv")


    async def get_weather_data(self) -> DataFrame:
        file_path = self.weather_data_path
        if not os.path.exists(file_path) or os.path.getsize(file_path) == 0:
            print(f"Weather data file not found: {file_path}")
            return pd.DataFrame()
        return pd.read_csv(file_path, sep=self.tsv_config.delimiter, na_values=self.tsv_config.na_values)
    

    async def _clean_weather_data_duplicates(self):
        weather_df = await self.get_weather_data()
        before_count = len(weather_df)
        weather_df = weather_df.drop_duplicates(subset=['date', 'locId'])
        after_count = len(weather_df)
        if after_count < before_count:
            print(f"Removed {before_count - after_count} duplicate weather records")
            # Save cleaned data back to file
            # weather_df.to_csv(self.weather_data_path, sep=self.tsv_config.delimiter, index=False)


    async def _make_weather_api_request(self, location_data: dict, start_date: date, end_date: date):
        # Ref: https://open-meteo.com/en/docs/historical-weather-api?latitude=6.3772655&longitude=80.1361152&start_date=2022-01-01&end_date=2025-11-01&hourly=temperature_2m,weather_code,rain,cloud_cover,apparent_temperature,wind_speed_10m&timezone=Asia%2FBangkok
        
        # Make sure all required weather variables are listed here
        # The order of variables in hourly or daily is important to assign them correctly below
        url = "https://archive-api.open-meteo.com/v1/archive"
        params = {
            "latitude": location_data["latitude"],
            "longitude": location_data["longitude"],
            "start_date": start_date.strftime("%Y-%m-%d"),
            "end_date": end_date.strftime("%Y-%m-%d"),
            "hourly": ["temperature_2m", "apparent_temperature", "rain", "weather_code", "cloud_cover", "cloud_cover_mid", "cloud_cover_high", "cloud_cover_low", "wind_speed_10m", "wind_speed_100m",  "wind_direction_100m", "wind_direction_10m"],
            "timezone": "GMT+5:30"
        }
        try:
            # OLD (Blocking)
            # responses = self.openmeteo.weather_api(url, params=params)

            # NEW (Non-blocking wrapper)
            # We offload the blocking call to a thread, making it awaitable
            responses = await asyncio.to_thread(self.openmeteo.weather_api, url, params=params)
        except Exception as e:
            print(f"Error occured making weather request: {e}")
            raise

        # Process first location. Add a for-loop for multiple locations or weather models
        response = responses[0]
        # print(f"Coordinates: {response.Latitude()}°N {response.Longitude()}°E")
        # print(f"Elevation: {response.Elevation()} m asl")
        # print(f"Timezone difference to GMT+0: {response.UtcOffsetSeconds()}s")

        # Process hourly data. The order of variables needs to be the same as requested.
        hourly = response.Hourly()
        hourly_temperature_2m = hourly.Variables(0).ValuesAsNumpy()
        hourly_apparent_temperature = hourly.Variables(1).ValuesAsNumpy()
        hourly_rain = hourly.Variables(2).ValuesAsNumpy()
        hourly_weather_code = hourly.Variables(3).ValuesAsNumpy()
        hourly_cloud_cover = hourly.Variables(4).ValuesAsNumpy()
        hourly_cloud_cover_mid = hourly.Variables(5).ValuesAsNumpy()
        hourly_cloud_cover_high = hourly.Variables(6).ValuesAsNumpy()
        hourly_cloud_cover_low = hourly.Variables(7).ValuesAsNumpy()
        hourly_wind_speed_10m = hourly.Variables(8).ValuesAsNumpy()
        hourly_wind_speed_100m = hourly.Variables(9).ValuesAsNumpy()
        hourly_wind_direction_100m = hourly.Variables(10).ValuesAsNumpy()
        hourly_wind_direction_10m = hourly.Variables(11).ValuesAsNumpy()

        hourly_data = {"date": pd.date_range(
            start = pd.to_datetime(hourly.Time(), unit = "s", utc = True),
            end =  pd.to_datetime(hourly.TimeEnd(), unit = "s", utc = True),
            freq = pd.Timedelta(seconds = hourly.Interval()),
            inclusive = "left"
        )}

        hourly_data["temperature_2m"] = hourly_temperature_2m
        hourly_data["apparent_temperature"] = hourly_apparent_temperature
        hourly_data["rain"] = hourly_rain
        hourly_data["weather_code"] = hourly_weather_code
        hourly_data["cloud_cover"] = hourly_cloud_cover
        hourly_data["cloud_cover_mid"] = hourly_cloud_cover_mid
        hourly_data["cloud_cover_high"] = hourly_cloud_cover_high
        hourly_data["cloud_cover_low"] = hourly_cloud_cover_low
        hourly_data["wind_speed_10m"] = hourly_wind_speed_10m
        hourly_data["wind_speed_100m"] = hourly_wind_speed_100m
        hourly_data["wind_direction_100m"] = hourly_wind_direction_100m
        hourly_data["wind_direction_10m"] = hourly_wind_direction_10m


        hourly_dataframe = pd.DataFrame(data = hourly_data)
        # print("\nHourly data\n", hourly_dataframe)

        hourly_dataframe["locId"] = location_data["locId"]
        
        return hourly_dataframe


    async def fetch_weather_data(self, loc_lookup: Dict[str, Dict[str, Any]]) -> None:
        # Get already completed requests
        try: 
            completed_date_loc_df = pd.read_csv(self.date_loc_status_file_path, sep="\t")
        except pd.errors.EmptyDataError:
            completed_date_loc_df = pd.DataFrame(columns=["locId", "start_date", "end_date"])


        # Get the full list of date-location pairs
        all_date_loc_df = pd.read_csv(self.date_loc_file, sep="\t")

        
        # Perform an outer join and add an indicator column
        merged = pd.merge(all_date_loc_df, completed_date_loc_df, how='outer', indicator=True)
        # Filter for rows that exist only in the 'left' DataFrame (all_date_loc_df)
        incomplete_date_loc_df = merged[merged['_merge'] == 'left_only'].drop(columns=['_merge'])

        if incomplete_date_loc_df.empty:
            return


        print("Number of incompletes date loc pairs: ", len(incomplete_date_loc_df))
        
        sem = asyncio.Semaphore(5)

        async def processing_worker(row):
            """ 
            This function handles the logic for a single row, 
            wrapped in the semaphore to limit concurrency.
            """
            async with sem:
                start_date = datetime.strptime(row["start_date"], "%Y-%m-%d").date()
                end_date = datetime.strptime(row["end_date"], "%Y-%m-%d").date()
                loc_id = row["locId"]
                
                # Fast lookup from our pre-made dictionary
                loc_info = loc_lookup.get(loc_id)
                
                if loc_info:
                    loc_data = {
                        "locId": loc_id,
                        "latitude": loc_info["latitude"],
                        "longitude": loc_info["longitude"]
                    }
                    # This is the actual network call
                    try:
                        hourly_dataframe = await self._make_weather_api_request(loc_data, start_date, end_date)
                        
                        # On success, write the data to the file and return the row to be marked as complete
                        async with self.file_lock:
                            os.makedirs(os.path.dirname(self.weather_data_path), exist_ok=True)
                            if not os.path.exists(self.weather_data_path) or os.path.getsize(self.weather_data_path) == 0:
                                hourly_dataframe.to_csv(self.weather_data_path, sep='\t', index=False)
                            else:
                                hourly_dataframe.to_csv(self.weather_data_path, sep='\t', index=False, mode='a', header=False)
                        # On success, return the identifiers to be marked as complete
                        return row
                        # return {"date": row["date"], "locId": loc_id}
                    except Exception as e:
                        print(f"!!! Task for {loc_id} Failed: {e}")
                        raise # Re-raise the exception to be caught in the loop

        # 2. Create a list of coroutine objects (tasks)
        # task_list = [asyncio.create_task(processing_worker(row)) for row in incomplete_date_loc_df.to_dict('records')]


        rows = incomplete_date_loc_df.to_dict('records')
        tasks = [] # We will store the task objects here to check results later

        try:
            async with asyncio.TaskGroup() as tg:
                # 2. Create the list INSIDE the group using 'tg'
                # This starts them and links them to the group's error handler
                tasks = [
                    tg.create_task(processing_worker(row))
                    for row in rows
                    if row['locId'] not in ["L51970428", "L40975620"] 
                ]
                
                # The code will block here automatically until all tasks are done
                # or until one fails (which triggers the group to cancel the others).
                
        except ExceptionGroup as e:
            print(f"One task failed! The others were cancelled: {e}")

        # 2. Find out EXACTLY what happened to each task
        for i, task in enumerate(tasks):
            if task.cancelled():
                # print(f"Row {i+1}: CANCELLED (Stopped because another task failed)")
                pass

            elif task.exception():
                # GET THE INTERNAL EXCEPTION HERE
                exc = task.exception()
                print(f"Row {i+1}: FAILED with error type: {type(exc).__name__}")
                print(f"       -> Message: {exc}")
                
            else:
                print(f"Row {i+1}: SUCCESS -> {task.result()}")


        successful_rows = []
        for t in tasks:
            # We only want tasks that finished successfully
            if t.done() and not t.cancelled() and not t.exception():
                successful_rows.append(t.result())


        if successful_rows:
            newly_completed_df = pd.DataFrame(successful_rows)
            # Append all newly completed tasks to the status file in one go
            if not os.path.exists(self.date_loc_status_file_path) or os.path.getsize(self.date_loc_status_file_path) == 0:
                newly_completed_df.to_csv(self.date_loc_status_file_path, sep='\t', index=False)
            else:
                newly_completed_df.to_csv(self.date_loc_status_file_path, sep='\t', index=False, mode='a', header=False)