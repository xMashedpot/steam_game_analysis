import requests
import os
import csv
from datetime import date
from pathlib import Path
import time

################################################################################

# CONSTANTS

APP_REVIEWS_URL = "https://store.steampowered.com/appreviews"

# Order of fields for app reviews
APP_FIELDS = [
    "appid",
    "num_reviews",
    "review_score",
    "total_positive",
    "total_negative",
    "total_reviews",
]

################################################################################


################################################################################

# FETCHING REVIEWS
    
def fetch_reviews(appid):
    response = requests.get(
        f"{APP_REVIEWS_URL}/{appid}",
        params={"json": 1},
        timeout=30,
    )
    response.raise_for_status()
    return response.json()

################################################################################


################################################################################

# PARSING REVIEWS

def parse_reviews(appid, data):
    if data.get("success") != 1:
        return None
    
    app_data = data.get("query_summary", {})

    app_row = {
        "appid": appid,
        "num_reviews": app_data.get("num_reviews"),
        "review_score": app_data.get("review_score"),
        "total_positive": app_data.get("total_positive"),
        "total_negative": app_data.get("total_negative"),
        "total_reviews": app_data.get("total_reviews"),
    }
    
    return app_row
        
################################################################################


################################################################################

# WRITING CSV

def write_app_row(app_row, run_dir):
    filepath = run_dir / "app_reviews.csv"
    fieldnames = APP_FIELDS

    file_exists = filepath.exists()
    with open(filepath, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        if not file_exists:
            writer.writeheader()
        writer.writerow(app_row)

################################################################################


################################################################################

# MANAGE PROCESSED APPIDS

def load_processed_appids(filename):
    try:
        with open(filename, "r") as f:
            return set(int(line.strip()) for line in f if line.strip())
    except FileNotFoundError:
        return set()

def mark_appid_processed(appid, filename):
    with open(filename, "a") as f:
        f.write(f"{appid}\n")
        
################################################################################


################################################################################

# READ APPID CSV

def read_appid_csv(filename):
    filename = Path(filename)
    appids = []

    with open(filename, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            try:
                appid = int(row["appid"])
                appids.append(appid)
            except (KeyError, ValueError):
                continue

    return appids

################################################################################


################################################################################

# MAIN LOOP

# Setting up
RUN_DATE = date.today().strftime("%Y-%m")
RUN_DIR = Path("data") / RUN_DATE
RUN_DIR.mkdir(parents=True, exist_ok=True)

reviews_csv = RUN_DIR / "app_reviews.csv"

appids = read_appid_csv("steam_game_list.csv")

processed_file = RUN_DIR / "processed_reviews.txt"
processed_appids = load_processed_appids(processed_file)

# Main loop
for appid in appids:
    if appid in processed_appids:
        continue

    try:
        data = fetch_reviews(appid)
        result = parse_reviews(appid, data)

        if not result:
            mark_appid_processed(appid, processed_file)
            continue

        app_row = result

        write_app_row(app_row, RUN_DIR)

        mark_appid_processed(appid, processed_file)

    except Exception as e:
        print(f"Failed app {appid}: {e}")
        
    time.sleep(1.5)
    
################################################################################