import os
import datetime
import csv
import re
import pandas as pd
from urllib.request import urlopen
from bs4 import BeautifulSoup
from datetime import timedelta
from scipy.stats import binomtest, ttest_ind


records = []

trip_df = pd.read_csv("trimet_relpos_2022-12-07.csv")

file_path = "trimet_stopevents_2022-12-07.html"

print("Exists:", os.path.exists(file_path))
print("Size in bytes:", os.path.getsize(file_path) if os.path.exists(file_path) else "File not found")

with open("trimet_stopevents_2022-12-07.html", encoding="utf-8") as f:
    html = f.read()
print("File length:", len(html))

soup = BeautifulSoup(html, "lxml")

tables = soup.find_all("table")
headers = soup.find_all("h2")
print("Found", len(headers), "h2 headers")
print("Found", len(tables), "tables")
for h2, table in zip(headers, tables):
    match = re.search(r"PDX_TRIP (-?\d+)", h2.text)
    if not match:
        continue

    trip_id = int(match.group(1))
    rows = table.find_all("tr")[1:]

    for row in rows:
        cols = row.find_all("td")
        if len(cols) < 15:
            continue
            
        try:
            vehicle_number = int(cols[0].text.strip())
            arrive_time = int(cols[8].text.strip())
            location_id = cols[10].text.strip()
            ons = int(cols[13].text.strip())
            offs = int(cols[14].text.strip())
            tstamp = datetime.datetime(2022, 12, 7) + timedelta(seconds=arrive_time)

            records.append({
                "trip_id": int(trip_id),
                "vehicle_number": int(vehicle_number),
                "tstamp": tstamp,
                "location_id": location_id,
                "ons": ons,
                "offs": offs
            })
        except Exception as e:
            print(f"Error: {e}")
            continue

stops_df = pd.DataFrame(records)
# print("Rows:", len(stops_df))

# print("Total stop events:", len(stops_df))

# unique_vehicles = stops_df["vehicle_number"].nunique()
# print("Unique vehicles:", unique_vehicles)

# unique_locations = stops_df["location_id"].nunique()
# print("Unique stop locations:", unique_locations)

# min_tstamp = stops_df["tstamp"].min()
# max_tstamp = stops_df["tstamp"].max()
# print("Timestamp range:")
# print("\tMin:", min_tstamp)
# print("\tMax:", max_tstamp)

# boarding_events = stops_df[stops_df["ons"] >= 1]
# num_boarding = len(boarding_events)
# percent_boarding = (num_boarding / len(stops_df)) * 100

# print("Stop events with at least one boarding:", num_boarding)
# print(f"Percentage of stop events with boardings: {percent_boarding:.2f}%")

# # For location 6913
# location_id = "6913"
# location_df = stops_df[stops_df["location_id"] == location_id]

# print("\n\n\nLocation", location_id)
# print("Number of stops at this location:", len(location_df))
# print("Number of different buses that stopped here:", location_df["vehicle_number"].nunique())

# boarding_at_location = (location_df["ons"] >= 1).sum()
# percent_boarding_location = (boarding_at_location / len(location_df)) * 100 if len(location_df) else 0

# print("Percentage of stops at this location with boardings: {:.2f}%".format(percent_boarding_location))


# # For vehicle 4062
# vehicle_id = 4062
# vehicle_df = stops_df[stops_df["vehicle_number"] == vehicle_id]

# print("\nVehicle", vehicle_id)
# print("Number of stops made by this vehicle:", len(vehicle_df))
# print("Total passengers boarded by this vehicle:", vehicle_df["ons"].sum())
# print("Total passengers deboarded from this vehicle:", vehicle_df["offs"].sum())

# boarding_events_vehicle = (vehicle_df["ons"] >= 1).sum()
# percent_boarding_vehicle = (boarding_events_vehicle / len(vehicle_df)) * 100 if len(vehicle_df) else 0

# print("Percentage of this vehicle’s stop events with boardings: {:.2f}%".format(percent_boarding_vehicle))

#Sys-wide probability of a boarding event
system_p = (stops_df["ons"] >= 1).mean()

alpha = 0.05 #Threashold for significance

biased_vehicles = []

for vehicle_id in stops_df["vehicle_number"].unique():
    vehicle_df = stops_df[stops_df["vehicle_number"] == vehicle_id]
    n = len(vehicle_df)
    k = (vehicle_df["ons"] >= 1).sum()

    p_value = binomtest(k=k, n=n, p=system_p).pvalue

    if p_value < alpha:
        biased_vehicles.append((vehicle_id, n, k, p_value))

print("System-wide boarding probability:", round(system_p, 4))
print("\nVehicles with potential boarding bias (p < 0.05):")
for vid, n, k, pval in sorted(biased_vehicles, key=lambda x: x[3]):
    print(f"Vehicle {vid} | Stops: {n} | Boardings: {k} | p-value: {pval:.6f}")

relpos_df = pd.read_csv("trimet_relpos_2022-12-07.csv")
all_relpos = relpos_df["RELPOS"].dropna().values

alpha = 0.005
biased_vehicles = []

for vehicle_id in relpos_df["VEHICLE_NUMBER"].unique():
    vehicle_relpos = relpos_df[relpos_df["VEHICLE_NUMBER"] == vehicle_id]["RELPOS"].dropna().values
    if len(vehicle_relpos) < 2:
        continue

    mask = relpos_df["VEHICLE_NUMBER"] != vehicle_id
    other_relpos = relpos_df[mask]["RELPOS"].dropna().values
    if len(other_relpos) < 2:
        continue

    p_value = ttest_ind(vehicle_relpos, other_relpos).pvalue

    if p_value < alpha:
        biased_vehicles.append((vehicle_id, len(vehicle_relpos), vehicle_relpos.mean(), p_value))

print("Vehicles with GPS bias (p < 0.005):")
for vid, n, avg, pval in biased_vehicles:
    print(f"Vehicle {vid} | Samples: {n} | Avg RELPOS: {avg:.4f} | p-value: {pval:.6f}")


