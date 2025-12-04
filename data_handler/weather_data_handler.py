"""Weather data handler for managing historical weather data.

This module provides functionality to:
- Read weather data from TSV files
- Write weather data to TSV files
- Organize weather data by location and date

The handler extends DataHandler to provide specialized functionality for
working with weather data stored in TSV format.
"""

import os
import pandas as pd
from pandas import DataFrame
from dataclasses import dataclass, field
from typing import Optional, Dict, Any, List
from datetime import date
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

    def __post_init__(self):
        # Setup the Open-Meteo API client with cache and retry on error
        cache_session = requests_cache.CachedSession('.cache', expire_after = -1)
        retry_session = retry(cache_session, retries = 5, backoff_factor = 0.2)
        self.openmeteo = openmeteo_requests.Client(session = retry_session)

        self.date_loc_dir_path = self._get_absolute_path("data/weather_helper/date_loc")
        self.date_loc_status_file_path = self._get_absolute_path("data/weather_helper/date_loc_status/done.txt")
    
    
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
            "hourly": ["temperature_2m", "apparent_temperature", "rain", "weather_code", "cloud_cover", "cloud_cover_mid", "cloud_cover_high", "cloud_cover_low", "wind_speed_10m", "wind_speed_100m"],
            "timezone": "GMT+5:30"
        }
        responses = self.openmeteo.weather_api(url, params=params)

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

        hourly_dataframe = pd.DataFrame(data = hourly_data)
        # print("\nHourly data\n", hourly_dataframe)

        hourly_dataframe["locId"] = location_data["locId"]
        
        file_path = "data/weather/weather.tsv"
        file_path = self. _get_absolute_path(file_path)
        os.makedirs(os.path.dirname(file_path), exist_ok=True)

        if not os.path.exists(file_path) or os.path.getsize(file_path) == 0:
            hourly_dataframe.to_csv(file_path, sep='\t', index=False)
        else:
            hourly_dataframe.to_csv(file_path, sep='\t', index=False, mode='a', header=False)


    async def fetch_weather_data_for_date_loc_pairs(self, date_loc_pairs: DataFrame, loc_lookup: Dict[str, Dict[str, Any]]) -> None:
        # 1. Create the Semaphore
        sem = asyncio.Semaphore(20)

        async def processing_worker(row):
            """
            This function handles the logic for a single row, 
            wrapped in the semaphore to limit concurrency.
            """
            async with sem:
                date = row["date"]
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
                    await self.make_weather_api_request(loc_data, date, date)

        # 2. Create a list of coroutine objects (tasks) - Do not await here!
        tasks = []
        for index, row in date_loc_pairs.iterrows():
            tasks.append(processing_worker(row))

        # 3. Run them all concurrently
        await asyncio.gather(*tasks)
    
    
    async def fetch_weather_data(self, loc_lookup: Dict[str, Dict[str, Any]]) -> None:
        # List files in date_loc_path
        date_loc_files_list = os.listdir(self.date_loc_dir_path)

        # Get already completed requests
        completed = set()
        try:
            with open(self.date_loc_status_file_path, "r") as f:
                for line in f.readlines():
                    completed.add(line.strip())
        except FileNotFoundError:
            pass

        # Get the incomplete files that havent been processed
        incomplete_date_loc_pairs = [path for path in date_loc_files_list if path not in completed]

        # Pick one from the incomplete list per run
        date_loc_file = incomplete_date_loc_pairs[0]
        date_loc_df = pd.read_csv(date_loc_file, sep="\t")

        await self.fetch_weather_data_for_date_loc_pairs(date_loc_df, loc_lookup)

        completed.add(date_loc_file)
        
        # Update date_loc_status file
        with open(self.date_loc_status_file_path, "w") as f:
            f.write("\n".join(completed))