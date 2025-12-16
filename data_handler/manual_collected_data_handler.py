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
class ManualDataHandler(DataHandler):
    async def get_weather_code_data(self):
        self.weather_code_data = [
            {"weather_code": 0, "weather_code_description": "Clear Sky"},
            {"weather_code": 1, "weather_code_description": "Mainly Clear"},
            {"weather_code": 2, "weather_code_description": "Partly Cloudy"},
            {"weather_code": 3, "weather_code_description": "Overcast"},
            {"weather_code": 45, "weather_code_description": "Fog"},
            {"weather_code": 48, "weather_code_description": "Fog"},
            {"weather_code": 51, "weather_code_description": "Drizzle: Light Intensity"},
            {"weather_code": 53, "weather_code_description": "Drizzle: Moderate Intensity"},
            {"weather_code": 55, "weather_code_description": "Drizzle: Dense Intensity"},
            {"weather_code": 56, "weather_code_description": "Freezing Drizzle: Light Intensity"},
            {"weather_code": 57, "weather_code_description": "Freezing Drizzle: Dense Intensity"},
            {"weather_code": 61, "weather_code_description": "Rain: Slight Intensity"},
            {"weather_code": 63, "weather_code_description": "Rain: Moderate Intensity"},
            {"weather_code": 65, "weather_code_description": "Rain: Heavy Intensity"},
            {"weather_code": 66, "weather_code_description": "Freezing Rain: Light Intensity"},
            {"weather_code": 67, "weather_code_description": "Freezing Rain: Heavy Intensity"},
            {"weather_code": 71, "weather_code_description": "Snow Fall: Slight Intensity"},
            {"weather_code": 73, "weather_code_description": "Snow Fall: Moderate Intensity"},
            {"weather_code": 75, "weather_code_description": "Snow Fall: Heavy Intensity"},
            {"weather_code": 77, "weather_code_description": "Snow Grains"},
            {"weather_code": 80, "weather_code_description": "Rain Showers: Slight"},
            {"weather_code": 81, "weather_code_description": "Rain Showers: Moderate"},
            {"weather_code": 82, "weather_code_description": "Rain Showers: Violent"},
            {"weather_code": 85, "weather_code_description": "Snow Showers: Slight"},
            {"weather_code": 86, "weather_code_description": "Snow Showers: Heavy"},
            {"weather_code": 95, "weather_code_description": "Thunderstorm: Slight or Moderate"},
            {"weather_code": 96, "weather_code_description": "Thunderstorm with Slight Hail"},
            {"weather_code": 99, "weather_code_description": "Thunderstorm with Heavy Hail"},
        ]

        return pd.DataFrame(self.weather_code_data)

        
    async def get_priority_species_data(self):
        self.priority_species_data = [
            {"speciesCode": "ceyjun1", "isHighValue_LK": True, "isEndemic_LK": True}, #1 Sri Lanka Junglefowl.            ceyjun1         endemic
            {"speciesCode": "ceymag1", "isHighValue_LK": True, "isEndemic_LK": True}, #2 Sri Lanka Blue Magpie            ceymag1         endemic
            {"speciesCode": "cehpar1", "isHighValue_LK": True, "isEndemic_LK": True}, #3 Sri Lanka Hanging Parrot         cehpar1         endemic
            {"speciesCode": "ceywop1", "isHighValue_LK": True, "isEndemic_LK": True}, #4 Sri Lanka Wood Pigeon            ceywop1         endemic
            {"speciesCode": "pogpig1", "isHighValue_LK": True, "isEndemic_LK": True}, #5 Sri Lanka Green Pigeon           pogpig1         endemic
            {"speciesCode": "ceghor1", "isHighValue_LK": True, "isEndemic_LK": True}, #6 Sri Lanka Grey Hornbill          ceghor1         endemic
            {"speciesCode": "ceymyn1", "isHighValue_LK": True, "isEndemic_LK": True}, #7 Sri Lanka Hill Myna              ceymyn1         endemic
            {"speciesCode": "bkrfla2", "isHighValue_LK": True, "isEndemic_LK": True}, #8 Red-backed Flameback             bkrfla2         endemic
            {"speciesCode": "crbfla1", "isHighValue_LK": True, "isEndemic_LK": True}, #9 Crimson-backed Flameback         crbfla1         endemic
            {"speciesCode": "yeebul1", "isHighValue_LK": True, "isEndemic_LK": True}, #10 Yellow-eared Bulbul             yeebul1         endemic
            {"speciesCode": "bkcbul2", "isHighValue_LK": True, "isEndemic_LK": True}, #11 Black-capped Bulbul             bkcbul2         endemic
            {"speciesCode": "ceywhe1", "isHighValue_LK": True, "isEndemic_LK": True}, #12 Sri Lanka White-eye             ceywhe1         endemic
            {"speciesCode": "sersco1", "isHighValue_LK": True, "isEndemic_LK": True}, #13 Serendib Scops Owl              sersco1         endemic
            {"speciesCode": "chbowl1", "isHighValue_LK": True, "isEndemic_LK": True}, #14 Chestnut-backed Owlet           chbowl1         endemic
            {"speciesCode": "whrsha5", "isHighValue_LK": True, "isEndemic_LK": True}, #16 Sri Lanka Shama                 whrsha5         endemic
            {"speciesCode": "laypar1", "isHighValue_LK": True, "isEndemic_LK": True}, #18 Sri Lanka Layard's              laypar1         endemic
            {"speciesCode": "ceyspu1", "isHighValue_LK": True, "isEndemic_LK": True}, #19 Sri Lanka Spurfowl              ceyspu1         endemic
            {"speciesCode": "grbcou2", "isHighValue_LK": True, "isEndemic_LK": True}, #20 Green-billed Coucal             grbcou2         endemic
            {"speciesCode": "whfsta2", "isHighValue_LK": True, "isEndemic_LK": True}, #21 White-faced Starling.           whfsta2         endemic
            {"speciesCode": "ceywht1", "isHighValue_LK": True, "isEndemic_LK": True}, #22 Sri Lanka Whistling Thrush      ceywht1         endemic
            {"speciesCode": "dubfly3", "isHighValue_LK": True, "isEndemic_LK": True}, #23 Sri Lanka Dull-blue Flycatcher  dubfly3         endemic
            {"speciesCode": "srlswa1", "isHighValue_LK": True, "isEndemic_LK": True}, #24 Sri Lanka Swallow               srlswa1         endemic
            {"speciesCode": "spwthr1", "isHighValue_LK": True, "isEndemic_LK": True}, #25 Spot-winged Thrush              spwthr1         endemic
            {"speciesCode": "scathr5", "isHighValue_LK": True, "isEndemic_LK": True}, #26 Sri Lanka Thrush                scathr5         endemic
            {"speciesCode": "srlwoo1", "isHighValue_LK": True, "isEndemic_LK": True}, #27 Sri Lanka Woodshrike            srlwoo1         endemic
            {"speciesCode": "ceybuw1", "isHighValue_LK": True, "isEndemic_LK": True}, #28 Sri Lanka Bushwarbler           ceybuw1         endemic
            {"speciesCode": "orbbab1", "isHighValue_LK": True, "isEndemic_LK": True}, #29 Sri Lanka Orange-billed Babbler orbbab1         endemic
            {"speciesCode": "refmal1", "isHighValue_LK": True, "isEndemic_LK": True}, #30 Red-faced Malkoha               refmal1         endemic
            {"speciesCode": "crfbar3", "isHighValue_LK": True, "isEndemic_LK": True}, #32 Crimson-fronted Barbet          crfbar3         endemic
            {"speciesCode": "yefbar1", "isHighValue_LK": True, "isEndemic_LK": True}, #33 Yellow-fronted Barbet           yefbar1         endemic
            {"speciesCode": "srldro1", "isHighValue_LK": True, "isEndemic_LK": True}, #34 Sri Lanka Drongo                srldro1         endemic
            {"speciesCode": "ashlau1", "isHighValue_LK": True, "isEndemic_LK": True}, #35 Ashy-headed laughingthrush      ashlau1         endemic
            {"speciesCode": "whtflo1", "isHighValue_LK": True, "isEndemic_LK": True}, #36 Legge's flowerpecker.           whtflo1         endemic
            {"speciesCode": "srlscb1", "isHighValue_LK": True, "isEndemic_LK": True}, #37 Sri Lanka scimitar babbler      srlscb1         endemic
            {"speciesCode": "ceyfro1", "isHighValue_LK": True, "isEndemic_LK": False}, #17 Sri Lanka Frogmouth             ceyfro1
            {"speciesCode": "blfmal1", "isHighValue_LK": True, "isEndemic_LK": False}, #31 Blue-faced Malkoha              blfmal1
            {"speciesCode": "srlbao1", "isHighValue_LK": True, "isEndemic_LK": False}, #15 Sri Lanka Bay-owl               srlbao1
            {"speciesCode": "maltro1", "isHighValue_LK": True, "isEndemic_LK": False}, #38 Malabar Trogon                  maltro1
            {"speciesCode": "lesadj1", "isHighValue_LK": True, "isEndemic_LK": False}, #39 Lesser adjutant                 lesadj1
            {"speciesCode": "piethr1", "isHighValue_LK": True, "isEndemic_LK": False}, #40 Pied Thrush
            {"speciesCode": "kasfly1", "isHighValue_LK": True, "isEndemic_LK": False}, #41 Kashmir Flycatcher
            {"speciesCode": "sirmal1", "isHighValue_LK": True, "isEndemic_LK": False}, #42 Sirkeer Malkoha
            {"speciesCode": "dafbab1", "isHighValue_LK": True, "isEndemic_LK": False}, #43 Dark-fronted Babbler
            {"speciesCode": "plhpar1", "isHighValue_LK": True, "isEndemic_LK": False}, #44 Plum-headed Parakeet
        ]

        return pd.DataFrame(self.priority_species_data)