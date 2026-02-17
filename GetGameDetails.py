import requests
import os
import csv
from datetime import date
from pathlib import Path
import time


################################################################################

# CONSTANTS

# API URL
APP_DETAILS_URL = "https://store.steampowered.com/api/appdetails"

# Order of fields for app details
APP_FIELDS = [
    "appid",
    "name",
    "type",
    "is_free",
    "coming_soon",
    "release_date",
    "price",
    "recommendations",
    "developers",
    "publishers",
]

################################################################################


################################################################################

# FETCHING DETAILS

def build_details_param(appid):
    return {
        "appids": appid,
        "cc": "us",
        "l": "en",
    }
    
def fetch_details(appid):
    response = requests.get(
        APP_DETAILS_URL,
        params=build_details_param(appid),
        timeout=30,
    )
    response.raise_for_status()
    return response.json()

################################################################################


################################################################################

# PARSING DETAILS

def extract_categories(details):
    categories = details.get("categories", [])
    return [
        (category.get("id"), category.get("description"))
        for category in categories
        if category.get("id") and category.get("description")
    ]

def extract_genres(details):
    genres = details.get("genres", [])
    return [
        (genre.get("id"), genre.get("description"))
        for genre in genres
        if genre.get("id") and genre.get("description")
    ]

def join_list(values):
    if not values:
        return ""
    return ";".join(v.strip() for v in values)

def parse_details(appid, data):
    app_data = data.get(str(appid), {})
    
    if not app_data.get("success"):
        return None
    
    details = app_data.get("data", {})

    app_row = {
        "appid": appid,
        "type": details.get("type"),
        "name": details.get("name"),
        "is_free": details.get("is_free"),
        "developers": join_list(details.get("developers")),
        "publishers": join_list(details.get("publishers")),
        "price": details.get("price_overview", {}).get("initial"),
        "recommendations": details.get("recommendations", {}).get("total"),
        "release_date": details.get("release_date", {}).get("date"),
        "coming_soon": details.get("release_date", {}).get("coming_soon"),
    }
    
    categories = extract_categories(details)
    genres = extract_genres(details)
    
    return app_row, categories, genres

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

# WRITE CSV

def load_existing_ids(filename, id_column):
    filename = Path(filename)
    if not filename.exists():
        return set()

    existing_ids = set()
    with open(filename, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            try:
                existing_ids.add(int(row[id_column]))
            except (KeyError, ValueError):
                continue
    return existing_ids

def write_app_row(app_row, run_dir):
    filepath = run_dir / "app_details.csv"
    fieldnames = APP_FIELDS

    file_exists = filepath.exists()
    with open(filepath, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        if not file_exists:
            writer.writeheader()
        writer.writerow(app_row)


def write_relationship_rows(rows, run_dir, filename, headers):
    filepath = run_dir / filename
    file_exists = filepath.exists()
    with open(filepath, "a", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        if not file_exists:
            writer.writerow(headers)
        writer.writerows(rows)


def write_mapping(appid, app_cats, app_gens, categories, genres, run_dir):
    new_cats = []
    new_app_cats = []
    for cat_id, desc in app_cats:
        if cat_id not in categories:
            categories[cat_id] = desc
            new_cats.append((cat_id, desc))
        new_app_cats.append((appid, cat_id))

    if new_cats:
        write_relationship_rows(new_cats, run_dir, "categories.csv", ["category_id", "description"])
    if new_app_cats:
        write_relationship_rows(new_app_cats, run_dir, "app_categories.csv", ["appid", "category_id"])

    new_gens = []
    new_app_gens = []
    for gen_id, desc in app_gens:
        if gen_id not in genres:
            genres[gen_id] = desc
            new_gens.append((gen_id, desc))
        new_app_gens.append((appid, gen_id))

    if new_gens:
        write_relationship_rows(new_gens, run_dir, "genres.csv", ["genre_id", "description"])
    if new_app_gens:
        write_relationship_rows(new_app_gens, run_dir, "app_genres.csv", ["appid", "genre_id"])


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

categories_csv = RUN_DIR / "categories.csv"
genres_csv = RUN_DIR / "genres.csv"

existing_category_ids = load_existing_ids(categories_csv, "category_id")
existing_genre_ids = load_existing_ids(genres_csv, "genre_id")

categories = {cid: "" for cid in existing_category_ids}
genres = {gid: "" for gid in existing_genre_ids}

appids = read_appid_csv("steam_game_list.csv")

processed_file = RUN_DIR / "processed_details.txt"
processed_appids = load_processed_appids(processed_file)

# Main loop
for appid in appids:
    if appid in processed_appids:
        continue

    try:
        data = fetch_details(appid)
        result = parse_details(appid, data)

        if not result:
            mark_appid_processed(appid, processed_file)
            continue

        app_row, app_cats, app_gens = result

        write_app_row(app_row, RUN_DIR)

        write_mapping(appid, app_cats, app_gens, categories, genres, RUN_DIR)

        mark_appid_processed(appid, processed_file)

    except Exception as e:
        print(f"Failed app {appid}: {e}")
        
    time.sleep(1.5)

################################################################################