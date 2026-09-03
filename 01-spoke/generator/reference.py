# -*- coding: utf-8 -*-
"""Spoke — reference implementation in plain Python.

Computes every figure the grader checks, straight from the delivered files.
Never assert an expected value from memory: run this.
"""
import csv, json, glob, os, sys, io
from collections import Counter, defaultdict
from datetime import datetime
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")

BASE = os.path.join(os.path.dirname(__file__), "..", "data")
TS = "%Y-%m-%dT%H:%M:%SZ"

# ------------------------------------------------------------- stations
raw_stations = list(csv.DictReader(open(os.path.join(BASE, "stations", "stations.csv"),
                                        encoding="utf-8")))
seen, stations = set(), {}
for s in raw_stations:
    sid = s["station_id"].strip()
    if sid in seen:  continue
    seen.add(sid)
    stations[sid] = {
        "station_name": " ".join(s["station_name"].split()),
        "district": s["district"].strip().title(),
        "capacity": int(s["capacity"]) if s["capacity"].strip() else None,
        "status": s["status"].strip(),
    }

# ----------------------------------------------------------------- rides
rides, dup = {}, 0
for path in sorted(glob.glob(os.path.join(BASE, "rides", "*.jsonl"))):
    for line in open(path, encoding="utf-8"):
        r = json.loads(line)
        if r["ride_id"] in rides: dup += 1; continue
        rides[r["ride_id"]] = r

no_end   = sum(1 for r in rides.values() if not r["end_station_id"])
staff    = sum(1 for r in rides.values() if r["user_type"] == "staff")
unknown  = sum(1 for r in rides.values()
               if r["start_station_id"] not in stations
               or (r["end_station_id"] and r["end_station_id"] not in stations))

def real_seconds(r):
    if not r["ended_at"]: return None
    return int((datetime.strptime(r["ended_at"], TS) -
                datetime.strptime(r["started_at"], TS)).total_seconds())

unreliable = sum(1 for r in rides.values()
                 if (s := real_seconds(r)) is not None
                 and abs(s - r["duration_seconds"]) > 60)

# ------------------------------------------------- usage : hors staff, station connue
usage = defaultdict(lambda: [0, 0])
hourly = Counter(); minutes = defaultdict(list)
for r in rides.values():
    if r["user_type"] == "staff": continue
    st = datetime.strptime(r["started_at"], TS)
    a, b = r["start_station_id"], r["end_station_id"]
    if a in stations:
        usage[(st.date().isoformat(), a)][0] += 1
        hourly[st.hour] += 1
        if (s := real_seconds(r)) is not None: minutes[st.hour].append(s / 60)
    if b and b in stations:
        usage[(datetime.strptime(r["ended_at"], TS).date().isoformat(), b)][1] += 1

print(f"{'FICHIERS LIVRÉS':<38}")
print(f"  stations.csv, lignes                {len(raw_stations)}")
print(f"  stations distinctes                 {len(stations)}")
print(f"  fichiers de courses                 {len(glob.glob(os.path.join(BASE,'rides','*.jsonl')))}")
print(f"  lignes de courses émises            {len(rides)+dup}")
print(f"\n{'CONTRAT DE QUALITÉ':<38}")
print(f"  duplicate_ride                      {dup}")
print(f"  missing_end_station                 {no_end}")
print(f"  unknown_station                     {unknown}")
print(f"  unreliable_duration                 {unreliable}")
print(f"\n{'COURSES':<38}")
print(f"  distinctes                          {len(rides)}")
print(f"  dont maintenance (staff)            {staff}")
print(f"  retenues pour l'usage               {len(rides)-staff}")
print(f"\n{'GOLD':<38}")
print(f"  daily_station_usage, lignes         {len(usage)}")
print(f"  hourly_demand, lignes               {len(hourly)}")
print(f"  data_quality_report, lignes         4")
print(f"  total rides_started                 {sum(v[0] for v in usage.values())}")
print(f"  total rides_ended                   {sum(v[1] for v in usage.values())}")
print(f"\n  heure de pointe                     {max(hourly, key=hourly.get)} h "
      f"({hourly[max(hourly, key=hourly.get)]} départs)")
top = sorted(usage.items(), key=lambda kv: -kv[1][0])[0]
print(f"  station la plus active (un jour)     {top[0][1]} le {top[0][0]} "
      f"({top[1][0]} départs)")
