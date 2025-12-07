"""eBird data handler for collecting and managing bird observation data.

This module provides functionality to:
- Fetch checklists from the eBird API for specified date ranges
- Retrieve detailed checklist records for specific checklist IDs
- Organize and save data to TSV files for further analysis

The handler supports asynchronous operations for efficient data collection
and includes built-in rate limiting and file management capabilities.
"""


import os
import ast
import asyncio
import pandas as pd
from dataclasses import dataclass, field
from datetime import date
from dotenv import load_dotenv

from .base import APIConfig, DataHandler


# Load environment variables from .env file
load_dotenv()

@dataclass
class eBirdDataHandler(DataHandler):
    """eBird data handler for collecting and managing bird observation data.
    
    This class extends DataHandler to provide specialized functionality for
    interacting with the eBird API. It handles authentication, API requests,
    and data persistence for checklists and checklist records.
    
    Attributes:
        api_config: Configuration for eBird API authentication. Defaults to
            using the EBIRD_API_KEY environment variable with the X-eBirdApiToken
            header name.
    
    Example:
        >>> handler = eBirdDataHandler(project_root_dir="/path/to/project")
        >>> await handler.fetch_checklists_by_year_for_date_range_tsv(date_range)
    """
    
    api_config: APIConfig = field(default_factory=lambda: APIConfig(api_key_env="EBIRD_API_KEY", key_header_name="X-eBirdApiToken"))
    
    def __post_init__(self):
        """Initialize project root directory from environment if not provided."""
        if self.project_root_dir is None:
            self.project_root_dir = os.getenv('PROJECT_ROOT_DIR')
        if not self.project_root_dir:
            raise ValueError("Project root directory must be provided or set in PROJECT_ROOT_DIR env var")
        
        self.location_data_path: str = self._get_absolute_path("data/locations/locations.tsv")
        self.checklist_records_path: str = self._get_absolute_path("data/checklist_records/checklist_records.tsv")


    async def get_location_data(self) -> pd.DataFrame:
        file_path = self.location_data_path
        if not os.path.exists(file_path) or os.path.getsize(file_path) == 0:
            print(f"Location data file not found: {file_path}")
            return pd.DataFrame()
        return pd.read_csv(file_path, sep=self.tsv_config.delimiter, na_values=self.tsv_config.na_values)


    async def get_checklist_records_data(self) -> pd.DataFrame:
        file_path = self.checklist_records_path
        if not os.path:
            print(f"Checklist record data file not found: {file_path}")
            return pd.DataFrame()

        return pd.read_csv(file_path, sep=self.tsv_config.delimiter, na_values=self.tsv_config.na_values)
        

    async def get_checklists_data(self, checklist_data_path: str = "data/checklists") -> pd.DataFrame:
        """Asynchronously load and concatenate all TSV checklist files into a single DataFrame.
        
        Reads all TSV files from the specified directory in parallel and combines
        them into a single pandas DataFrame. Uses asyncio.to_thread to parallelize
        blocking pandas.read_csv calls without requiring additional dependencies.
        
        Args:
            checklist_data_path: Relative path to the directory containing TSV
                checklist files. Defaults to "data/checklists".
        
        Returns:
            A pandas DataFrame containing all checklist data from the TSV files.
            Returns an empty DataFrame if no TSV files are found.
        
        Raises:
            FileNotFoundError: If the specified directory does not exist or is empty.
        
        Example:
            >>> df = await handler.get_checklists_data("data/checklists")
            >>> print(df.head())
        """
        absolute_dir = self._get_absolute_path(checklist_data_path)
        if not os.path.isdir(absolute_dir):
            raise FileNotFoundError(f"Checklist directory not found: {absolute_dir}")
        if not os.listdir(absolute_dir):
            raise FileNotFoundError(f"Checklist directory empty: {absolute_dir}")

        tsv_files = [
            os.path.join(absolute_dir, file)
            for file in os.listdir(absolute_dir)
            if file.endswith('.tsv')
        ]

        if not tsv_files:
            return pd.DataFrame()

        # Read all TSVs concurrently using threads (I/O-bound)
        read_tasks = [
            asyncio.to_thread(
                pd.read_csv, 
                file_path, 
                sep=self.tsv_config.delimiter, 
                na_values=self.tsv_config.na_values
            ) 
            for file_path in tsv_files
        ]
        dataframes = await asyncio.gather(*read_tasks)

        return pd.concat(dataframes, ignore_index=True) if dataframes else pd.DataFrame()
    

    async def fetch_checklist_record_for_checklists(self, checklist_list: set = None) -> None:
        """Fetch detailed checklist records for a set of checklist IDs and save to TSV.
        
        Retrieves detailed observation records from the eBird API for each checklist ID
        in the provided set. Records are saved to a single TSV file with automatic
        header management. If no checklist list is provided, it will attempt to load
        checklist IDs from existing checklist data files.
        
        The method uses a semaphore to limit concurrent API requests to 20 at a time
        to respect rate limits and avoid overwhelming the API.
        
        Args:
            checklist_list: Optional set of checklist IDs (subID values) to fetch
                records for. If None, the method will attempt to load checklist IDs
                from the data/checklists directory. Defaults to None.
        
        Note:
            This method will skip fetching if the target directory already contains
            data to avoid redundant API calls.
        
        Example:
            >>> checklist_ids = {"S12345678", "S87654321"}
            >>> await handler.fetch_checklist_record_for_checklists(checklist_ids)
        """
        file_path = os.dirname(self.checklist_records_path)
        
        # Skip if directory already exists and has content
        if os.path.exists(file_path) and os.path.isdir(file_path) and os.listdir(file_path):
            print(f"Skipping fetch: {file_path} already has content")
            return

        if checklist_list is None:
            print("Checklists not provided, checking if they exists at data/checklists....")
            checklist_df = self.get_checklists_data(checklist_data_path = "data/checklists")
            checklist_list = set(checklist_df["subID"].tolist())
        
        semaphore = asyncio.Semaphore(20)

        async def fetch_one(checklist_id: str) -> tuple[str, dict]:
            async with semaphore:
                return checklist_id, await self._make_api_request(
                    f"https://api.ebird.org/v2/product/checklist/view/{checklist_id}"
                )

        # Fetch with concurrency limited to 20 at a time
        result: list[tuple[str, dict]] = await asyncio.gather(*(fetch_one(cl) for cl in checklist_list))

        for checklist_id, record in result:
            file_path = self.checklist_records_path

            fieldnames = self._get_tsv_fieldnames(file_path)
            if not fieldnames:
                fieldnames = self._get_fieldnames_from_records([record]) if isinstance(record, dict) else []

            write_header = self._should_write_header(file_path)
            self._write_tsv_record(file_path, record, fieldnames=fieldnames, write_header=write_header)
            print(f"Saved record for {checklist_id}")
    

    async def fetch_checklists_by_year_for_date_range_tsv(self, date_range: list[date]) -> None:
        """Fetch checklists for a date range and save to TSV files organized by year.
        
        Retrieves checklist data from the eBird API for each date in the provided
        date range. Checklists are fetched for Sri Lanka (country code: LK) and
        saved to separate TSV files organized by year (e.g., checklists_2023.tsv).
        
        The method uses a semaphore to limit concurrent API requests to 20 at a time
        to respect rate limits. Results are written sequentially to avoid concurrent
        file access issues when multiple dates belong to the same year.
        
        Args:
            date_range: List of date objects representing the dates for which to
                fetch checklists. Each date will be queried against the eBird API
                for Sri Lanka (LK) checklists.
        
        Note:
            This method will skip fetching if the target directory already contains
            data to avoid redundant API calls. Files are organized by year, so
            multiple dates from the same year will be appended to the same file.
        
        Example:
            >>> from datetime import date
            >>> dates = [date(2023, 1, 1), date(2023, 1, 2), date(2023, 1, 3)]
            >>> await handler.fetch_checklists_by_year_for_date_range_tsv(dates)
        """

        file_path = f"data/checklists/"
        file_path = self._get_absolute_path(file_path)
        if os.path.exists(file_path) and os.path.isdir(file_path) and os.listdir(file_path):
            print(f"Skipping fetching checklists. Already fetched")
            return

        semaphore = asyncio.Semaphore(20)

        async def fetch_one(d: date) -> tuple[date, list[dict]]:
            async with semaphore:
                return d, await self._make_api_request(
                    f"https://api.ebird.org/v2/product/lists/LK/{d.year}/{d.month}/{d.day}"
                )

        # Fetch with concurrency limited to 20 at a time
        results: list[tuple[date, list[dict]]] = await asyncio.gather(*(fetch_one(d) for d in date_range))

        # Write sequentially to avoid concurrent writes to the same year file
        for d, checklists in sorted(results, key=lambda x: x[0]):
            file_path = f"data/checklists/checklists_{d.year}.tsv"
            file_path = self._get_absolute_path(file_path)
            
            fieldnames = self._get_tsv_fieldnames(file_path)
            if not fieldnames:
                fieldnames = self._get_fieldnames_from_records(checklists)
            
            write_header = self._should_write_header(file_path)
            self._write_tsv_records(file_path, checklists, fieldnames=fieldnames, write_header=write_header)
            print(f"Saved checklists for {d}")


    async def fetch_loc_data_from_checklists(self):
        file_path = self.location_data_path
        
        # Skip if file already exists and has content
        if os.path.exists(file_path) and os.path.getsize(file_path) > 0:
            print(f"Skipping fetch: {file_path} already has content")
            return
        
        checklist_df = await self.get_checklists_data(checklist_data_path = "data/checklists")
        locations_list = set(checklist_df["loc"].tolist())
        
        # Parse unique location dictionaries from the location strings
        location_records = []
        for loc_str in locations_list:
            try:
                # Handle different formats: dict object, string representation of dict
                if isinstance(loc_str, dict):
                    loc_dict = loc_str
                elif isinstance(loc_str, str):
                    loc_str_clean = loc_str.strip()
                    # Strip outer quotes if present (from TSV quoting)
                    if (loc_str_clean.startswith('"') and loc_str_clean.endswith('"')) or \
                       (loc_str_clean.startswith("'") and loc_str_clean.endswith("'")):
                        loc_str_clean = loc_str_clean[1:-1]
                    
                    # Parse the string representation of the dictionary
                    loc_dict = ast.literal_eval(loc_str_clean)
                else:
                    raise ValueError(f"Unexpected object type in Location. Should be dict or str: {loc_str}")
                
                if isinstance(loc_dict, dict) and 'locId' in loc_dict:
                    location_records.append(loc_dict)
            except (ValueError, SyntaxError) as e:
                raise RuntimeError(f"Warning: Could not parse location data: {e}")

        # Each item will be in the form {'locId': 'L10398196', 'name': 'LK-Southern Province-Sky garden hotel (5.9460,80.4571)', 'latitude': 5.946044, 'longitude': 80.457065, 'countryCode': 'LK', 'countryName': 'Sri Lanka', 'subnational1Name': 'Matara', 'subnational1Code': 'LK-32', 'subnational2Code': 'LK-32-015', 'subnational2Name': 'Weligama', 'isHotspot': False, 'locName': 'LK-Southern Province-Sky garden hotel (5.9460,80.4571)', 'lat': 5.946044, 'lng': 80.457065, 'hierarchicalName': 'LK-Southern Province-Sky garden hotel (5.9460,80.4571), Weligama, Matara, LK', 'locID': 'L10398196'}
        # Write to TSV
        if not location_records:
            print("No location data found in checklists")
            return

        fieldnames = self._get_tsv_fieldnames(file_path)
        if not fieldnames:
            fieldnames = self._get_fieldnames_from_records(location_records)

        for record in location_records:
            write_header = self._should_write_header(file_path)
            self._write_tsv_record(file_path, record, fieldnames=fieldnames, write_header=write_header)
            print(f"Saved location data for {record.get('locId', 'unknown')}")