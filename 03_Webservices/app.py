# pip install
import os
import requests
#from dotenv import load_dotenv
import pprint

from pathlib import Path

os.chdir(Path(__file__).parent)

LAT = "52.52"
LON = "13.404"

# read the .env file
load_dotenv()

API_KEY = os.getenv("API_KEY")

# optionales Parameter
UNITS = "metric"
LANG = "de"

URL = f"https://api.openweathermap.org/data/2.5/weather?lat={LAT}&lon={LON}&appid={API_KEY}&units={UNITS}&lang={LANG}"

response = requests.get(URL)

print(response)

print(response.status_code)

data = response.json()

pprint.pprint(data)

print(data['main']['temp'])
print(data['sys']['country'])
print(data['weather'][0]['description'])