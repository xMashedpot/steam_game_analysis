import requests
import pandas as pd
from bs4 import BeautifulSoup
from pathlib import Path
from datetime import datetime
from datetime import date
import time
import csv
import os

################################################################################

# CONSTANTS

STEAM_CHARTS_URL = "https://steamcharts.com/app"

################################################################################


################################################################################

# FETCHING HTML

def fetch_players(appid):
    response = requests.get(
        f"{STEAM_CHARTS_URL}/{appid}",
        timeout=30,
    )
    response.raise_for_status()
    return response.text

################################################################################


################################################################################

# PARSING HTML

def parse_players(html):
    soup = BeautifulSoup(html, "html.parser")
    table = soup.find("table", class_="common-table")
    
    if table is None:
        raise ValueError("Table not found")

    tbody = table.find("tbody")
    return tbody.find_all("tr")

def extract_monthly_rows(rows):
    data = []
    current_month = datetime.today().strftime("%Y-%m")

    for row in rows:
        cols = [c.get_text(strip=True) for c in row.find_all("td")]

        if len(cols) < 5:
            continue
        
        month_text = cols[0]
        if month_text.lower() == "last 30 days":
            month = current_month
        else:
            dt = datetime.strptime(month_text, "%B %Y")
            month = dt.strftime("%Y-%m")

        data.append({
            "month": month,
            "avg_players": cols[1],
            "gain": cols[2],
            "percent_gain": cols[3],
            "peak_players": cols[4],
        })

    return data

def build_dataframe(data):
    return pd.DataFrame(data)

def clean_numeric_column(series):
    return pd.to_numeric(
        series
        .str.replace(",", "", regex=False)
        .str.replace("+", "", regex=False)
        .str.replace("%", "", regex=False)
        .replace("-", ""),
        errors="coerce"
    )

def clean_player_data(df):
    numeric_cols = [
        "avg_players",
        "gain",
        "percent_gain",
        "peak_players",
    ]

    for col in numeric_cols:
        df[col] = clean_numeric_column(df[col])

    return df

def move_appid_first(df):
    cols = ["appid"] + [c for c in df.columns if c != "appid"]
    return df[cols]

def scrape_monthly_player_counts(appid):
    html = fetch_players(appid)
    rows = parse_players(html)
    data = extract_monthly_rows(rows)
    df = build_dataframe(data)

    df["appid"] = appid

    df = clean_player_data(df)
    df = move_appid_first(df)
    
    return df

################################################################################


################################################################################

# WRITE TO CSV

def append_df_to_csv(df, csv_path):
    write_header = not os.path.exists(csv_path)

    df.to_csv(
        csv_path,
        mode="a",
        index=False,
        header=write_header
    )

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

# MAIN LOOP

RUN_DATE = date.today().strftime("%Y-%m")
RUN_DIR = Path("data") / RUN_DATE
RUN_DIR.mkdir(parents=True, exist_ok=True)

appids = read_appid_csv("steam_game_list.csv")

players_csv = RUN_DIR / "app_players.csv"

processed_file = RUN_DIR / "processed_players.txt"
processed_appids = load_processed_appids(processed_file)

for appid in appids:
    if appid in processed_appids:
        continue
    
    try:
        data = scrape_monthly_player_counts(appid)
        
        if data.empty:
            mark_appid_processed(appid, processed_file)
            continue
        
        append_df_to_csv(data, players_csv)
        
        mark_appid_processed(appid, processed_file)

    except Exception as e:
        print(f"Failed app {appid}: {e}")
        
    time.sleep(1.5)

################################################################################