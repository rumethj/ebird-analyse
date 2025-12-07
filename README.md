# Ebird Data Analysis

## Description
This project involves analyzing eBird observation data along with corresponding weather information. The data is collected from the eBird API and a weather API, and is stored in TSV files. The goal is to explore relationships between bird sightings and weather patterns.

## Data Schema

### `weather.tsv`
This table contains hourly weather data for various locations.

| Column | Type | Description |
|---|---|---|
| `date` | `DATETIME` | The timestamp in UTC for the weather data record. |
| `temperature_2m` | `FLOAT` | Temperature in Celsius at 2 meters above ground. |
| `apparent_temperature` | `FLOAT` | The perceived temperature. |
| `rain` | `FLOAT` | Rain precipitation in millimeters. |
| `weather_code` | `INTEGER` | A code representing the weather conditions (e.g., clear, cloudy). |
| `cloud_cover` | `FLOAT` | Total cloud cover percentage. |
| `cloud_cover_mid` | `FLOAT` | Mid-level cloud cover percentage. |
| `cloud_cover_high` | `FLOAT` | High-level cloud cover percentage. |
| `cloud_cover_low` | `FLOAT` | Low-level cloud cover percentage. |
| `wind_speed_10m` | `FLOAT` | Wind speed at 10 meters above ground. |
| `wind_speed_100m` | `FLOAT` | Wind speed at 100 meters above ground. |
| `wind_direction_10m`| `FLOAT` | Wind direction at 10 meters above ground. | 
| `wind_direction_100m`| `FLOAT` | Wind direction at 100 meters above ground. | 
| `locId` | `STRING` | The unique identifier for the location, foreign key to `checklists.tsv`. |

### `locations.tsv`
This table contains detailed information for each bird observation checklist.

| Column | Type | Description |
|---|---|---|
| `allObsReported` | `BOOLEAN` | Indicates if all observations for the checklist were reported. |
| `checklistId` | `STRING` | The unique identifier for the checklist. |
| `creationDt` | `DATETIME` | The creation date and time of the checklist. |
| `deleteTrack` | `BOOLEAN` | Indicates if the track was deleted. |
| `durationHrs` | `FLOAT` | The duration of the observation in hours. |
| `effortDistanceEnteredUnit` | `STRING` | The unit of measurement for the distance traveled (e.g., 'km', 'mi'). |
| `effortDistanceKm` | `FLOAT` | The distance traveled in kilometers. |
| `lastEditedDt` | `DATETIME` | The date and time the checklist was last edited. |
| `locId` | `STRING` | The unique identifier for the location. |
| `numObservers` | `INTEGER` | The number of observers for the checklist. |
| `numSpecies` | `INTEGER` | The total number of species observed. |
| `obs` | `JSON` | A JSON array of observation records for the checklist. |
| `obsDt` | `DATETIME` | The date and time of the observation. |
| `obsTimeValid` | `BOOLEAN` | Indicates if the observation time is valid. |
| `projId` | `STRING` | The project ID associated with the checklist (e.g., 'EBIRD'). |
| `projectIds` | `JSON` | A JSON array of project IDs. |
| `protocolId` | `STRING` | The protocol ID for the observation (e.g., 'P22' for traveling). |
| `subAux` | `JSON` | A JSON array of auxiliary submission information. |
| `subAuxAi` | `JSON` | A JSON array of AI-related auxiliary submission information. |
| `subId` | `STRING` | The unique submission identifier. |
| `submissionMethodCode` | `STRING` | The code for the submission method (e.g., 'EBIRD_android'). |
| `submissionMethodVersion` | `STRING` | The version of the submission method. |
| `submissionMethodVersionDisp` | `STRING` | The display version of the submission method. |
| `subnational1Code` | `STRING` | The code for the subnational region (e.g., province or state). |
| `userDisplayName` | `STRING` | The display name of the user who submitted the checklist. |

### `checklists.tsv`
This table contains a summary of each checklist, including location details.

| Column | Type | Description |
|---|---|---|
| `isoObsDate` | `DATETIME` | The observation date and time in ISO format. |
| `loc` | `JSON` | A JSON object containing detailed location information. |
| `locId` | `STRING` | The unique identifier for the location. **Primary Key**. |
| `numSpecies` | `INTEGER` | The number of species reported in the checklist. |
| `obsDt` | `STRING` | The observation date in 'D MMM YYYY' format. |
| `obsTime` | `STRING` | The observation time. |
| `subID` | `STRING` | The submission identifier. |
| `subId` | `STRING` | The submission identifier (duplicate of `subID`). |
| `userDisplayName` | `STRING` | The display name of the user. |

### `checklist_records.tsv`
This table appears to contain the same data as `locations.tsv`, with detailed information for each checklist.

| Column | Type | Description |
|---|---|---|
| `allObsReported` | `BOOLEAN` | Indicates if all observations for the checklist were reported. |
| `checklistId` | `STRING` | The unique identifier for the checklist. |
| `creationDt` | `DATETIME` | The creation date and time of the checklist. |
| `deleteTrack` | `BOOLEAN` | Indicates if the track was deleted. |
| `durationHrs` | `FLOAT` | The duration of the observation in hours. |
| `effortDistanceEnteredUnit` | `STRING` | The unit of measurement for the distance traveled (e.g., 'km', 'mi'). |
| `effortDistanceKm` | `FLOAT` | The distance traveled in kilometers. |
| `lastEditedDt` | `DATETIME` | The date and time the checklist was last edited. |
| `locId` | `STRING` | The unique identifier for the location. |
| `numObservers` | `INTEGER` | The number of observers for the checklist. |
| `numSpecies` | `INTEGER` | The total number of species observed. |
| `obs` | `JSON` | A JSON array of observation records for the checklist. |
| `obsDt` | `DATETIME` | The date and time of the observation. |
| `obsTimeValid` | `BOOLEAN` | Indicates if the observation time is valid. |
| `projId` | `STRING` | The project ID associated with the checklist (e.g., 'EBIRD'). |
| `projectIds` | `JSON` | A JSON array of project IDs. |
| `protocolId` | `STRING` | The protocol ID for the observation (e.g., 'P22' for traveling). |
| `subAux` | `JSON` | A JSON array of auxiliary submission information. |
| `subAuxAi` | `JSON` | A JSON array of AI-related auxiliary submission information. |
| `subId` | `STRING` | The unique submission identifier. |
| `submissionMethodCode` | `STRING` | The code for the submission method (e.g., 'EBIRD_android'). |
| `submissionMethodVersion` | `STRING` | The version of the submission method. |
| `submissionMethodVersionDisp` | `STRING` | The display version of the submission method. |
| `subnational1Code` | `STRING` | The code for the subnational region (e.g., province or state). |
| `userDisplayName` | `STRING` | The display name of the user who submitted the checklist. |

## Usage Instructions

1. Set Up Env
python3.11 -m venv .venv
source .venv/bin/activate

Create a .env file in the root and add your eBird api key:
'EBIRD_API_KEY=your_key_here'

2. Get Data
Run the get data which will create the "data" directory and append all the data to.