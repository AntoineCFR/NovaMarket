# -*- coding: utf-8 -*-
"""Spoke — data generator. Deterministic: same seed, same bytes.

Do not read this file before you have finished the project. It gives away
every defect the brief expects you to discover by analysis.
"""
import csv, json, random, os, io
from datetime import datetime, timedelta, date

SEED = 20260904
BASE = os.path.join(os.path.dirname(__file__), "..", "data")
DAYS = [date(2026, 6, 1) + timedelta(days=i) for i in range(7)]

DISTRICTS = ["Riverside", "Old Town", "Harbour", "Northgate", "University", "Meadows"]
PLATFORMS = ["ios", "android", "web"]
VERSIONS  = ["4.2.0", "4.2.1", "4.3.0", "3.9.7"]
ISSUES    = ["brake_noise", "flat_tyre", "seat_loose", "chain_slip", "battery_low"]

rnd = random.Random(SEED)

# ---------------------------------------------------------------- stations
stations = []
for i in range(1, 61):
    sid = f"ST{i:03d}"
    district = DISTRICTS[(i - 1) % len(DISTRICTS)]
    name = f"{district} {rnd.choice(['Square','Bridge','Park','Station','Market','Quay','Gate','Court'])}"
    stations.append({
        "station_id": sid,
        "station_name": name,
        "district": district,
        "capacity": rnd.choice([12, 16, 20, 24, 28]),
        "latitude": round(50.05 + rnd.uniform(-0.06, 0.06), 6),
        "longitude": round(14.42 + rnd.uniform(-0.09, 0.09), 6),
        "commissioned_on": (date(2023, 1, 1) + timedelta(days=rnd.randint(0, 1100))).isoformat(),
        "status": "active",
    })

# --- defects, deliberate and countable
# a) three names carry stray whitespace (spreadsheet export)
for i in (3, 21, 44):
    stations[i]["station_name"] = "  " + stations[i]["station_name"] + " "
# b) two capacities were never registered
for i in (9, 37):
    stations[i]["capacity"] = ""
# c) district spelling drifts across the file
for i in (5, 18, 30, 51):
    stations[i]["district"] = stations[i]["district"].upper()
for i in (12, 26):
    stations[i]["district"] = stations[i]["district"].lower()
# d) two stations were decommissioned mid-period
for i in (14, 47):
    stations[i]["status"] = "decommissioned"
# e) one station appears twice — merged operator records
stations.append(dict(stations[7]))

os.makedirs(os.path.join(BASE, "stations"), exist_ok=True)
with open(os.path.join(BASE, "stations", "stations.csv"), "w", encoding="utf-8", newline="") as f:
    w = csv.DictWriter(f, fieldnames=list(stations[0].keys()), delimiter=",",
                       quoting=csv.QUOTE_MINIMAL, lineterminator="\n")
    w.writeheader(); w.writerows(stations)

KNOWN = [s["station_id"] for s in stations if s["status"] == "active"]
GHOST = ["ST071", "ST072"]          # opened mid-period, absent from the reference

# ------------------------------------------------------------------- rides
def hour_weight(h):
    return {7:5,8:9,9:6,12:4,13:4,16:5,17:10,18:8,19:5}.get(h, 1)

HOURS = [h for h in range(5, 24) for _ in range(hour_weight(h))]
ride_no = 0
totals = {"emitted": 0, "duplicates": 0, "no_end": 0, "staff": 0,
          "ghost": 0, "bad_duration": 0}

os.makedirs(os.path.join(BASE, "rides"), exist_ok=True)
for day in DAYS:
    lines = []
    n = rnd.randint(620, 780)
    for _ in range(n):
        ride_no += 1
        rid = f"R{ride_no:07d}"
        start_pool = KNOWN + (GHOST if rnd.random() < 0.02 else [])
        a = rnd.choice(start_pool)
        b = rnd.choice(start_pool)
        h = rnd.choice(HOURS)
        started = datetime(day.year, day.month, day.day, h,
                           rnd.randint(0, 59), rnd.randint(0, 59))
        real = rnd.randint(180, 4200)
        ended = started + timedelta(seconds=real)
        user = "staff" if rnd.random() < 0.03 else rnd.choice(["member", "casual", "member"])
        rec = {
            "ride_id": rid,
            "bike_id": f"BK{rnd.randint(1, 900):04d}",
            "user_type": user,
            "started_at": started.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "ended_at": ended.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "start_station_id": a,
            "end_station_id": b,
            "duration_seconds": real,
            "device": {"platform": rnd.choice(PLATFORMS), "app_version": rnd.choice(VERSIONS)},
            "issues": rnd.sample(ISSUES, rnd.choice([0, 0, 0, 0, 1, 1, 2])),
        }
        if a in GHOST or b in GHOST: totals["ghost"] += 1
        if user == "staff":          totals["staff"] += 1
        # f) the bike was left outside a dock: no end station, no end time
        if rnd.random() < 0.018:
            rec["end_station_id"] = None; rec["ended_at"] = None
            totals["no_end"] += 1
        # g) the app computes duration itself, and sometimes gets it wrong
        elif rnd.random() < 0.04:
            rec["duration_seconds"] = real + rnd.choice([-real, 3600, -120, 86400])
            totals["bad_duration"] += 1
        lines.append(rec); totals["emitted"] += 1
        # h) the app retries on a dropped connection and sends the ride twice
        if rnd.random() < 0.015:
            lines.append(dict(rec)); totals["duplicates"] += 1

    rnd.shuffle(lines)
    path = os.path.join(BASE, "rides", f"rides_{day.isoformat()}.jsonl")
    with open(path, "w", encoding="utf-8", newline="") as f:
        for r in lines:
            f.write(json.dumps(r, ensure_ascii=False, separators=(",", ":")) + "\n")

print(f"stations : {len(stations)} lignes écrites (dont 1 doublon)")
print(f"rides    : {totals['emitted']} courses distinctes sur {len(DAYS)} jours")
for k, v in totals.items():
    if k != "emitted": print(f"   {k:14s} {v}")
