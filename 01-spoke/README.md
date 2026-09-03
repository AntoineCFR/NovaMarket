# Spoke — Rebalancing the fleet

**Client brief · Data engineering engagement · June 2026**

---

## Who we are

Spoke runs the docked bike-sharing network in the city. Sixty stations, around nine
hundred bikes, and a mobile app our riders use to unlock and return them. We have been
operating for three years and we are, by most measures, doing well.

We have one problem, and it is expensive.

## The problem

Every morning our vans go out to rebalance the fleet — moving bikes from stations that
have filled up overnight to stations that have run dry. Today those routes are decided by
two people who have been doing this long enough to have a feel for it. They are usually
right. But they are only two people, they take holidays, and neither of them can explain
their reasoning to anyone else.

We want to stop relying on instinct. Before we can automate anything, we need to *see*
what actually happens on the network — reliably, every day, in numbers we trust.

That is what we are asking you to build.

## What we will hand over

Two feeds. Both are already flowing; nothing needs to be requested from anyone.

### The station reference

A single file, exported from our asset management system by the operations team. One row
per station: identifier, name, district, dock capacity, coordinates, the date it was
commissioned, and whether it is still in service.

Three things you should know about this file, because we would rather tell you now than
have you discover them:

- **Our operations team merged two asset systems last year.** We are fairly confident at
  least one station ended up recorded twice. Nobody has ever cleaned it up.
- **A handful of capacity figures were never registered.** Contractors installed those
  docks and the paperwork never came back.
- **District names have been typed by hand for three years**, by different people. They
  are not consistent, and the marketing team refers to districts by name in every report
  they publish.

### The ride feed

One file per day, produced by the app backend at midnight. One record per completed ride:
who rode, which bike, when it started and ended, where it started and ended, how long it
took, plus some technical detail about the phone and any faults the rider reported.

Four things you should know:

- **The app retries when a connection drops mid-upload.** Our backend team tells us this
  means some rides reach you more than once, identically.
- **When a rider fails to dock the bike properly, the ride never closes.** We still send
  you the record, but it has no end station and no end time. It is a real ride — someone
  took a bike out — but we never learned where it finished.
- **The duration figure is computed on the phone, not by us.** Our engineering lead is
  blunt about it: when the handset loses signal, that number can be wildly wrong. Do not
  build anything on it that you would not want to defend in a meeting.
- **Two stations opened at the beginning of June** and are not in the reference file yet.
  Rides at those stations are perfectly real.

One more thing, and it matters for what we are asking. **Our maintenance staff use the
same app** to move bikes around. Those trips are not customer demand — counting them as
usage is exactly the mistake we are trying to stop making.

---

## What we need to know

Three questions. They are the questions our rebalancing team asks every morning, and they
have never had a real answer.

**Where does the fleet drift?** For each station and each day, how many rides started
there and how many ended there. A station that consistently loses more bikes than it
receives is a station our vans need to visit.

**When does demand happen?** Across the whole period, how many rides start in each hour
of the day, and how long the average ride lasts at that hour. We want to know whether the
evening peak is one wave or two, and whether evening rides are longer than morning ones.

**How much of this can we not use?** We are aware the feeds are imperfect. We would like
that stated plainly, in numbers, rather than discovered later by someone building a
dashboard on top of your work. Tell us how many records we lost and why.

---

## What our tools will consume

Our BI team will read these three tables and nothing else. Everything upstream of them is
yours to design as you see fit — we have no opinion on how you get there, and we will not
ask.

Put them in a catalogue named `spoke`, in a schema named `gold`.

### `spoke.gold.daily_station_usage`

One row per station per day, for stations that appear in the reference.

| Column | Type | Meaning |
|---|---|---|
| `ride_date` | date | The day |
| `station_id` | string | |
| `station_name` | string | As it should be displayed |
| `district` | string | As it should be displayed |
| `rides_started` | bigint | Rides that began at this station on this day |
| `rides_ended` | bigint | Rides that ended at this station on this day |
| `net_flow` | bigint | `rides_ended` − `rides_started` |

### `spoke.gold.hourly_demand`

One row per hour of the day in which anything happened.

| Column | Type | Meaning |
|---|---|---|
| `hour_of_day` | int | 0 to 23 |
| `rides_started` | bigint | Across the whole period |
| `avg_ride_minutes` | double | Average duration of rides starting in that hour |

### `spoke.gold.data_quality_report`

One row per kind of problem, with how many records it affected.

| Column | Type | Meaning |
|---|---|---|
| `issue` | string | One of the four values below |
| `record_count` | bigint | |

| `issue` | What it counts |
|---|---|
| `duplicate_ride` | Records that arrived more than once |
| `missing_end_station` | Rides that never closed |
| `unknown_station` | Rides referencing a station absent from the reference |
| `unreliable_duration` | Rides where the reported duration disagrees with the recorded start and end times by more than a minute |

---

## How we will judge the work

Our analyst will run a set of checks against the three tables. They verify the numbers,
not the method — we genuinely do not mind how you build this, and we will not read your
notebooks.

Two things she will insist on, because we have been burned before:

**The maintenance trips must not appear as demand.** If our staff moving bikes shows up in
`hourly_demand`, every conclusion we draw from it is wrong.

**A ride with no end station still counts as a departure.** Someone took that bike out.
Dropping the record because it is incomplete would flatter the data and mislead the vans.

---

## Practical details

The files are in `data/`. The station reference is delivered once; the ride feed has seven
days of history and a new file lands every night, so build for that even though you are
only receiving a week today.

If something in this brief is ambiguous, decide, and write down what you decided and why.
We would rather read a documented assumption than answer a question three weeks late.

*Marta Kowalczyk — Head of Operations, Spoke*
