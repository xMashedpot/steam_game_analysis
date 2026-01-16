import requests
from dotenv import load_dotenv
import os
import csv

# Load private info
load_dotenv()

API_KEY = os.getenv("API_KEY")
GAME_LIST_CSV_PATH = os.getenv("GAME_LIST_CSV_PATH")

# create the address for api call depending on api_key and last app checked
def make_address(api_key, last_appid):
    return f"https://api.steampowered.com/IStoreService/GetAppList/v1/?key={api_key}&include_dlc=false&include_software=false&include_videos=false&include_hardware=false&last_appid={last_appid}&max_results=50000"

# calling the api
def fetch_apps(address):
    response = requests.get(address)
    response.raise_for_status()
    return response.json()

# saving data in variables
def parse_response(data):
    response = data.get("response", {})
    
    apps = response.get("apps", [])
    have_more_results = response.get("have_more_results", False)
    last_appid = response.get("last_appid")

    return apps, have_more_results, last_appid

# getting game data
def extract_app_rows(apps):
    rows = []

    for app in apps:
        appid = app.get("appid")
        name = app.get("name", "")

        if appid is not None:
            rows.append((appid, name))

    return rows


# main api call
have_more_results = True
last_appid = "0"
all_apps = []

while have_more_results:
    address = make_address(API_KEY, last_appid)
    data = fetch_apps(address)
    apps, have_more_results, last_appid = parse_response(data)
    rows = extract_app_rows(apps)
    
    all_apps.extend(rows)
    
    if not apps:
        break

# Check Duplicates
unique_apps = list({appid: name for appid, name in all_apps}.items())
unique_apps.sort(key=lambda x: x[0])

# Write csv file
with open(GAME_LIST_CSV_PATH, "w", newline="", encoding="utf-8") as f:
    writer = csv.writer(f)
    writer.writerow(["appid", "name"])
    writer.writerows(unique_apps)





# old test
"""
test = requests.get(make_address(last_appid))
data = test.json()
print(json.dumps(data, indent=2)[:2000])
"""