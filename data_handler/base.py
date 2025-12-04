import os
import asyncio
import csv
import httpx
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Optional
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()


@dataclass
class APIConfig:
    """Configuration for API requests."""
    api_key_env: str
    key_header_name: str
    api_key: Optional[str] = None
    delay: float = 1.0
    
    def __post_init__(self):
        """Set API key from environment if not provided."""
        if self.api_key is None:
            self.api_key = os.getenv(self.api_key_env)
    
    def get_headers(self) -> dict:
        """Get request headers with API key."""
        if not self.api_key:
            raise ValueError("API key not found in environment variables or config")
        return {self.key_header_name: self.api_key}


@dataclass
class TSVConfig:
    """Configuration for TSV file operations."""
    delimiter: str = "\t"
    extrasaction: str = "ignore"  # "ignore" or "raise"
    na_values: list = field(default_factory=list)


@dataclass
class DataHandler(ABC):
    """eBird data collector for handling file I/O, API requests, and data collection."""
    
    project_root_dir: Optional[str] = None
    tsv_config: TSVConfig = field(default_factory=TSVConfig)
    project_root_dir:str = field(default_factory=lambda: os.getenv('PROJECT_ROOT_DIR'))

    
    def _find_project_root(self, project_dir: Optional[str] = None) -> str:
        """
        Walks up the directory tree from the current working directory until it finds the directory named project_dir.
        Returns the absolute path of the project root directory.
        Raises FileNotFoundError if not found.
        
        Args:
            project_dir: Directory name to search for. If None, uses self.project_root_dir.
            
        Returns:
            Absolute path of the project root directory.
        """
        search_dir = project_dir or self.project_root_dir
        current_path = os.path.abspath(os.getcwd())
        while True:
            if os.path.basename(current_path) == search_dir:
                return current_path
            parent_path = os.path.dirname(current_path)
            if parent_path == current_path:  # Reached root of filesystem
                break
            current_path = parent_path
        raise FileNotFoundError(f"Project root directory '{search_dir}' not found from current directory upward.")
    
    def _get_absolute_path(self, relative_path: str) -> str:
        """Get absolute path by joining project root with relative path.
        
        Args:
            relative_path: Path relative to project root.
            
        Returns:
            Absolute path.
        """
        project_root = self._find_project_root()
        return os.path.join(project_root, relative_path)
    
    def _ensure_tsv_header(self, file_path: str, record: dict) -> list[str]:
        """Ensure TSV file exists with proper header. Creates file and header if needed.
        
        Args:
            file_path: Path to TSV file (can be relative or absolute).
            record: Dictionary record to determine fieldnames from.
            
        Returns:
            List of fieldnames (header columns).
        """
        # Convert to absolute path if relative
        if not os.path.isabs(file_path):
            file_path = self._get_absolute_path(file_path)
        
        os.makedirs(os.path.dirname(file_path), exist_ok=True)
        if os.path.exists(file_path) and os.path.getsize(file_path) > 0:
            with open(file_path, "r", newline="") as f:
                return f.readline().rstrip("\n\r").split(self.tsv_config.delimiter)
        else:
            fieldnames = sorted(record.keys())
            with open(file_path, "w", newline="") as f:
                writer = csv.DictWriter(f, fieldnames=fieldnames, delimiter=self.tsv_config.delimiter)
                writer.writeheader()
            return fieldnames
    
    def _get_tsv_fieldnames(self, file_path: str) -> list[str]:
        """Get fieldnames from existing TSV file or return empty list.
        
        Args:
            file_path: Path to TSV file (can be relative or absolute).
            
        Returns:
            List of fieldnames, or empty list if file doesn't exist or is empty.
        """
        # Convert to absolute path if relative
        if not os.path.isabs(file_path):
            file_path = self._get_absolute_path(file_path)
        
        if os.path.exists(file_path) and os.path.getsize(file_path) > 0:
            with open(file_path, "r", newline="") as f:
                first_line = f.readline().rstrip("\n\r")
                return first_line.split(self.tsv_config.delimiter) if first_line else []
        return []
    
    def _get_fieldnames_from_records(self, records: list[dict]) -> list[str]:
        """Extract and sort fieldnames from a list of record dictionaries.
        
        Args:
            records: List of dictionaries to extract keys from.
            
        Returns:
            Sorted list of unique fieldnames.
        """
        keys_set = set()
        for item in records:
            if isinstance(item, dict):
                keys_set.update(item.keys())
        return sorted(keys_set)
    
    def _write_tsv_record(self, file_path: str, record: dict, fieldnames: Optional[list[str]] = None, 
                        write_header: bool = False) -> None:
        """Write a single record to TSV file.
        
        Args:
            file_path: Path to TSV file (can be relative or absolute).
            record: Dictionary record to write.
            fieldnames: Optional list of fieldnames. If None, uses record keys.
            write_header: Whether to write header row.
        """
        # Convert to absolute path if relative
        if not os.path.isabs(file_path):
            file_path = self._get_absolute_path(file_path)
        
        os.makedirs(os.path.dirname(file_path), exist_ok=True)
        
        if fieldnames is None:
            fieldnames = sorted(record.keys()) if isinstance(record, dict) else []
        
        with open(file_path, "a", newline="") as f:
            writer = csv.DictWriter(
                f, 
                fieldnames=fieldnames, 
                delimiter=self.tsv_config.delimiter, 
                extrasaction=self.tsv_config.extrasaction
            )
            if write_header and fieldnames:
                writer.writeheader()
            if isinstance(record, dict):
                row = {key: record.get(key, "") for key in fieldnames}
                writer.writerow(row)
    
    def _write_tsv_records(self, file_path: str, records: list[dict], fieldnames: Optional[list[str]] = None,
                         write_header: bool = False) -> None:
        """Write multiple records to TSV file.
        
        Args:
            file_path: Path to TSV file (can be relative or absolute).
            records: List of dictionary records to write.
            fieldnames: Optional list of fieldnames. If None, extracts from records.
            write_header: Whether to write header row.
        """
        # Convert to absolute path if relative
        if not os.path.isabs(file_path):
            file_path = self._get_absolute_path(file_path)
        
        os.makedirs(os.path.dirname(file_path), exist_ok=True)
        
        if fieldnames is None:
            fieldnames = self._get_fieldnames_from_records(records)
        
        with open(file_path, "a", newline="") as f:
            writer = csv.DictWriter(
                f, 
                fieldnames=fieldnames, 
                delimiter=self.tsv_config.delimiter, 
                extrasaction=self.tsv_config.extrasaction
            )
            if write_header and fieldnames:
                writer.writeheader()
            for item in records:
                if isinstance(item, dict):
                    row = {key: item.get(key, "") for key in fieldnames}
                    writer.writerow(row)
    
    async def _make_api_request(self, url: str, headers: Optional[dict] = None, 
                               delay: Optional[float] = None) -> dict:
        """Make an async HTTP API request.
        
        Args:
            url: API endpoint URL.
            headers: Optional request headers. If None, uses default API config headers.
            delay: Delay in seconds after request (for rate limiting). If None, uses api_config.delay.
            
        Returns:
            JSON response as dictionary.
        """
        if headers is None:
            headers = self.api_config.get_headers()
        
        request_delay = delay if delay is not None else self.api_config.delay
        
        async with httpx.AsyncClient() as client:
            response = await client.get(url, headers=headers)
            response.raise_for_status()
            await asyncio.sleep(request_delay)
            data = response.json()
            print(f"Fetched Response for url:{url}")
            return data

    
    def _should_write_header(self, file_path: str) -> bool:
        """Check if header should be written (file doesn't exist or is empty).
        
        Args:
            file_path: Path to TSV file (can be relative or absolute).
            
        Returns:
            True if header should be written, False otherwise.
        """
        # Convert to absolute path if relative
        if not os.path.isabs(file_path):
            file_path = self._get_absolute_path(file_path)
        
        return not os.path.exists(file_path) or os.path.getsize(file_path) == 0


class UtilDataHandler(DataHandler):
    pass