#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
NovaMarket - implementation de reference des couches M4 (historisation) et M5 (gold).

Meme principe que reference_stats.py : une seconde implementation, en Python pur, des
regles decrites dans les README des modules. Elle produit les comptages exacts contre
lesquels les graders sont calibres.

Usage :
    python generator/reference_gold.py
"""

from __future__ import annotations

import csv
import json
import os
from collections import Counter, defaultdict
from datetime import date, datetime, timedelta
from decimal import Decimal, ROUND_HALF_UP

from generate import build_categories, build_customers, build_products, build_sellers
from reference_stats import (analyse_events, clean_decimal, normalize_status, parse_qty,
                             parse_ts, read_event_files, read_order_files, validate,
                             NON_REVENUE_STATUSES)

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
LAKEBASE = os.path.join(ROOT, "data", "lakebase")

PLAN_COMMISSION = {
    "BASIC": Decimal("0.150"),
    "PLUS": Decimal("0.115"),
    "PREMIUM": Decimal("0.085"),
}

CUSTOMER_TRACKED = ["first_name", "last_name", "email", "country", "city", "zip_code",
                    "segment", "is_opt_in", "is_deleted"]
SELLER_TRACKED = ["seller_name", "seller_country", "seller_city", "main_top_category",
                  "plan_code", "is_active"]

LOOKBACK_DAYS = 90


def read_csv(name):
    with open(os.path.join(LAKEBASE, name), "r", encoding="utf-8", newline="") as fh:
        return list(csv.DictReader(fh))


def money(value: Decimal) -> Decimal:
    return value.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)


# --------------------------------------------------------------------------
# M4 — historisation SCD2
# --------------------------------------------------------------------------


def build_journal(versions, n_extractions=3):
    """Reconstitue le journal bronze produit par les extractions successives en `>=`.

    versions : [etat_initial, etat_apres_D1, etat_apres_D2]
    n_extractions : 2 pour l'etat a la fin de M2, 3 apres la vague D2.
    Renvoie une liste de (indice d'extraction, ligne).
    """
    journal = [(0, r) for r in versions[0]]
    wm = max(r["updated_at"] for r in versions[0])
    for i in range(1, n_extractions):
        delta = [r for r in versions[i] if r["updated_at"] >= wm]
        journal.extend((i, r) for r in delta)
        wm = max(r["updated_at"] for r in delta)
    return journal


def scd2(journal, key, tracked):
    """Construit la table SCD2 a partir du journal.

    Regles :
      - tri par (updated_at, ordre d'extraction) — le second critere departage les
        versions portant le meme horodatage ;
      - deux versions consecutives aux attributs suivis identiques sont fusionnees ;
      - valid_to d'une version = valid_from de la suivante ; NULL pour la courante.
    """
    by_key = defaultdict(list)
    for extraction, row in journal:
        by_key[row[key]].append((row["updated_at"], extraction, row))

    rows = []
    collapsed = 0
    for k, versions in by_key.items():
        versions.sort(key=lambda v: (v[0], v[1]))
        kept = []
        for updated_at, _extraction, row in versions:
            signature = tuple(row[c] for c in tracked)
            if kept and kept[-1][1] == signature:
                collapsed += 1
                continue
            kept.append((updated_at, signature, row))

        for i, (updated_at, _sig, row) in enumerate(kept):
            is_current = i == len(kept) - 1
            rows.append({
                key: k,
                "valid_from": updated_at,
                "valid_to": None if is_current else kept[i + 1][0],
                "is_current": is_current,
                **{c: row[c] for c in tracked},
            })
    return rows, collapsed


def analyse_scd2():
    cust_versions = [read_csv(n) for n in
                     ["app_customers.csv", "app_customers_v2.csv", "app_customers_v3.csv"]]
    sell_versions = [read_csv(n) for n in
                     ["app_sellers.csv", "app_sellers_v2.csv", "app_sellers_v3.csv"]]

    def versions_hist(rows, key):
        c = Counter(r[key] for r in rows)
        return dict(sorted(Counter(c.values()).items()))

    stages = {}
    for n in (2, 3):
        cj = build_journal(cust_versions, n)
        sj = build_journal(sell_versions, n)
        cust_scd, cust_collapsed = scd2(cj, "customer_id", CUSTOMER_TRACKED)
        sell_scd, sell_collapsed = scd2(sj, "seller_id", SELLER_TRACKED)
        stages[f"apres_{n}_extractions"] = {
            "journal_customer_rows": len(cj),
            "journal_seller_rows": len(sj),
            "customer_scd2_rows": len(cust_scd),
            "customer_scd2_current": sum(1 for r in cust_scd if r["is_current"]),
            "customer_scd2_collapsed": cust_collapsed,
            "customer_versions_histogram": versions_hist(cust_scd, "customer_id"),
            "customer_scd2_deleted_current": sum(
                1 for r in cust_scd if r["is_current"] and r["is_deleted"] == "true"),
            "seller_scd2_rows": len(sell_scd),
            "seller_scd2_current": sum(1 for r in sell_scd if r["is_current"]),
            "seller_scd2_collapsed": sell_collapsed,
            "seller_versions_histogram": versions_hist(sell_scd, "seller_id"),
            "sellers_with_plan_change": sum(
                1 for _k, n_v in Counter(r["seller_id"] for r in sell_scd).items() if n_v > 1),
        }
    return stages, sell_versions


# --------------------------------------------------------------------------
# M5 — gold
# --------------------------------------------------------------------------


def silver_order_lines():
    """Rejoue les regles de M3 et renvoie les lignes valides, typees."""
    seen = {}
    for _file, row, _n in read_order_files():
        seen.setdefault(row["order_line_id"], row)

    out = []
    for row in seen.values():
        if validate(row):
            continue
        qty = parse_qty(row["quantity"])
        unit = clean_decimal(row["unit_price"])
        disc = clean_decimal(row["discount_amount"]) or Decimal("0.00")
        gross = money(Decimal(qty) * unit)
        status = normalize_status(row["order_status"])
        out.append({
            "order_line_id": row["order_line_id"],
            "order_id": row["order_id"],
            "order_ts": parse_ts(row["order_ts"]),
            "customer_id": row["customer_id"],
            "seller_id": row["seller_id"],
            "product_id": row["product_id"],
            "quantity": qty,
            "net_amount": money(gross - disc),
            "order_status": status,
            "is_revenue": status not in NON_REVENUE_STATUSES,
        })
    return out


def analyse_gold(lines, sell_versions):
    categories = build_categories()
    sellers = build_sellers(categories)
    products = build_products(categories, sellers)

    cat_top = {c["category_id"]: c["top_category_code"] for c in categories}
    prod_top = {p["product_id"]: cat_top[p["category_id"]] for p in products}

    plan_initial = {r["seller_id"]: r["plan_code"] for r in sell_versions[0]}
    plan_current = {r["seller_id"]: r["plan_code"] for r in sell_versions[2]}

    total_hist = Decimal("0.00")
    total_naive = Decimal("0.00")
    monthly = defaultdict(lambda: [Decimal("0.00"), Decimal("0.00"), 0])
    agg_keys = set()
    status_by_seller = defaultdict(Counter)
    product_revenue = Counter()
    product_returns = Counter()

    max_date = max(l["order_ts"].date() for l in lines)
    cutoff = max_date - timedelta(days=LOOKBACK_DAYS)

    for line in lines:
        month = line["order_ts"].strftime("%Y-%m")
        top = prod_top.get(line["product_id"], "UNKNOWN")
        sid = line["seller_id"]

        status_by_seller[sid][line["order_status"]] += 1

        if not line["is_revenue"]:
            if line["order_status"] == "RETURNED":
                product_returns[line["product_id"]] += 1
            continue

        rate_hist = PLAN_COMMISSION[plan_initial[sid]]
        rate_naive = PLAN_COMMISSION[plan_current[sid]]
        c_hist = money(line["net_amount"] * rate_hist)
        c_naive = money(line["net_amount"] * rate_naive)

        total_hist += c_hist
        total_naive += c_naive

        m = monthly[month]
        m[0] += line["net_amount"]
        m[1] += c_hist
        m[2] += 1
        agg_keys.add((month, top, sid))

        if line["order_ts"].date() > cutoff:
            product_revenue[line["product_id"]] += int(line["net_amount"] * 100)

    top_product, top_cents = product_revenue.most_common(1)[0]

    return {
        "fact_rows": len(lines),
        "commission_historized_total": str(total_hist),
        "commission_current_plan_total": str(total_naive),
        "commission_gap": str(total_naive - total_hist),
        "agg_revenue_monthly_rows": len(agg_keys),
        "months": {k: {"net_amount": str(v[0]), "commission": str(v[1]), "lines": v[2]}
                   for k, v in sorted(monthly.items())},
        "lookback_cutoff_exclusive": str(cutoff),
        "max_order_date": str(max_date),
        "top_product_90d": top_product,
        "top_product_90d_net_amount": str(money(Decimal(top_cents) / 100)),
        "products_with_revenue_90d": len(product_revenue),
    }


def analyse_funnel():
    stages = ["product_view", "add_to_cart", "checkout_start", "purchase"]
    sessions_by_source = defaultdict(set)
    stage_sessions = {s: defaultdict(set) for s in stages}
    seen = set()

    for _file, line in read_event_files():
        try:
            evt = json.loads(line)
        except json.JSONDecodeError:
            continue
        if evt["event_id"] in seen:
            continue
        seen.add(evt["event_id"])

        source = (evt.get("context") or {}).get("utm", {}).get("source") or "unknown"
        session = (evt.get("user") or {}).get("session_id")
        sessions_by_source[source].add(session)
        if evt["event_type"] in stage_sessions:
            stage_sessions[evt["event_type"]][source].add(session)

    rows = {}
    for source in sorted(sessions_by_source):
        rows[source] = {"sessions": len(sessions_by_source[source])}
        for s in stages:
            rows[source][s] = len(stage_sessions[s][source])
    return {"sources": len(rows), "by_source": rows}


# --------------------------------------------------------------------------


def main():
    print("historisation SCD2…")
    scd, sell_versions = analyse_scd2()

    print("reconstitution de silver.order_line…")
    lines = silver_order_lines()

    print("agregats gold…")
    gold = analyse_gold(lines, sell_versions)

    print("entonnoir de conversion…")
    funnel = analyse_funnel()

    out = {"m4_scd2": scd, "m5_gold": gold, "m5_funnel": funnel}
    path = os.path.join(ROOT, "graders", "expected", "M4_M5.json")
    with open(path, "w", encoding="utf-8") as fh:
        json.dump(out, fh, indent=2, ensure_ascii=False)

    print(json.dumps(scd, indent=2, ensure_ascii=False))
    print(json.dumps({k: v for k, v in gold.items() if k != "months"}, indent=2, ensure_ascii=False))
    print(json.dumps(gold["months"], indent=2, ensure_ascii=False))
    print(f"sources utm : {funnel['sources']}")
    print(f"\n-> {path}")


if __name__ == "__main__":
    main()
