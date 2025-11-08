from pandas import DataFrame
from datetime import date, datetime, timedelta

def get_data_from_column(df: DataFrame, column:str) -> set():
    return set(df[column].tolist())


def get_date_range(date_range_str: str) -> list[date]:
    """Create a date list for a date range given in the form YYYY-MM-DD:YYYY-MM-DD"""
    start_date, end_date = date_range_str.split(":")
    start_date = datetime.strptime(start_date, "%Y-%m-%d").date()
    end_date = datetime.strptime(end_date, "%Y-%m-%d").date()
    return [start_date + timedelta(days=i) for i in range((end_date - start_date).days + 1)]
