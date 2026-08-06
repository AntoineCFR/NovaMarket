#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
NovaMarket - implementation de reference des regles de la couche silver (M3).

Lit les fichiers reellement presents dans data/waves/, applique les regles de
nettoyage decrites dans modules/M3-silver/README.md, et produit les comptages
exacts attendus par le grader.

C'est volontairement du Python pur : le but est d'avoir une seconde implementation,
independante de Spark, qui donne les memes chiffres. Si le grader et ce script
divergent, l'un des deux a tort — et c'est une information utile.

Usage :
    python generator/reference_stats.py
"""

from __future__ import annotations

import argparse
import glob
import gzip
import json
import os
import re
from collections import Counter
from datetime import datetime
from decimal import Decimal, InvalidOperation

from generate import build_categories, build_customers, build_products, build_sellers

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# Etats successifs du parcours. Chaque module valide un etat different : ajouter une
# vague change tous les comptages, ce qui est normal et fait partie du sujet.
STATES = {
    "S2": ["W1_initial", "W2"],                    # M1, M3, M5 : etat de reference
    "S3": ["W1_initial", "W2", "W3"],              # M8 : apres la derive de schema
    "S4": ["W1_initial", "W2", "W3", "W4"],        # M9 : apres l'incident
}
WAVES = STATES["S2"]

ORDER_FIELDS = [
    "order_id", "order_line_id", "order_ts", "customer_id", "seller_id", "product_id",
    "quantity", "unit_price", "discount_amount", "currency", "shipping_country",
    "payment_method", "order_status", "shipping_address",
]
N_FIELDS = len(ORDER_FIELDS)

TS_FORMAT = "%Y-%m-%d %H:%M:%S"
VALID_STATUSES = {"DELIVERED", "SHIPPED", "PENDING", "CANCELLED", "RETURNED"}
NON_REVENUE_STATUSES = {"CANCELLED", "RETURNED"}

# Tout sauf les chiffres, le point, la virgule et le signe moins.
PRICE_NOISE = re.compile(r"[^0-9,.\-]")


# --------------------------------------------------------------------------
# Regles de nettoyage — le coeur du contrat de M3
# --------------------------------------------------------------------------


def clean_decimal(raw: str):
    """Normalise un montant : retire le bruit, ramene la virgule decimale au point.

    Renvoie un Decimal, ou None si la valeur reste inexploitable.
    """
    if raw is None:
        return None
    s = PRICE_NOISE.sub("", raw).strip()
    if not s:
        return None
    if "," in s:
        s = s.replace(",", ".")
    try:
        return Decimal(s)
    except InvalidOperation:
        return None


def parse_ts(raw: str):
    """Horodatage au format declare dans le contrat d'interface, ou None."""
    if not raw:
        return None
    try:
        return datetime.strptime(raw.strip(), TS_FORMAT)
    except ValueError:
        return None


def parse_qty(raw: str):
    try:
        return int(str(raw).strip())
    except (ValueError, TypeError):
        return None


def normalize_status(raw: str) -> str:
    return (raw or "").strip().upper()


# Prix catalogue, renseigne uniquement quand la regle de vraisemblance d'echelle est
# active (M9). Vide, la regle ne se declenche jamais : les modules M1 a M7 gardent
# exactement leurs comptages.
LIST_PRICES: dict = {}
SCALE_FACTOR = 10


def validate(row: dict) -> list:
    """Renvoie la liste des motifs de quarantaine. Vide = la ligne passe en silver."""
    reasons = []
    if parse_ts(row["order_ts"]) is None:
        reasons.append("INVALID_TIMESTAMP")
    qty = parse_qty(row["quantity"])
    if qty is None or qty <= 0:
        reasons.append("INVALID_QUANTITY")
    price = clean_decimal(row["unit_price"])
    if price is None or price <= 0:
        reasons.append("INVALID_PRICE")
    if normalize_status(row["order_status"]) not in VALID_STATUSES:
        reasons.append("UNKNOWN_STATUS")

    # Vraisemblance d'echelle : un prix unitaire tres au-dessus du prix catalogue
    # trahit un changement d'unite cote source. Ne s'applique qu'aux produits connus.
    if LIST_PRICES and price is not None and price > 0:
        catalog = LIST_PRICES.get(row.get("product_id"))
        if catalog is not None and price > SCALE_FACTOR * catalog:
            reasons.append("SUSPECTED_UNIT_SCALE")

    return reasons


# --------------------------------------------------------------------------
# Commandes
# --------------------------------------------------------------------------


def read_order_files():
    for wave in WAVES:
        pattern = os.path.join(ROOT, "data", "waves", wave, "orders", "*.csv")
        for path in sorted(glob.glob(pattern)):
            with open(path, "r", encoding="cp1252", newline="") as fh:
                header = fh.readline().rstrip("\r\n").split(";")
                for line in fh:
                    parts = line.rstrip("\r\n").split(";")
                    # Au-dela de N_FIELDS jetons, le surplus part au rescue cote Spark.
                    # En deca (ligne tronquee), les colonnes manquantes sont nulles.
                    # On reproduit les deux comportements pour rester comparable.
                    row = dict(zip(header[:N_FIELDS], parts[:N_FIELDS]))
                    for col in ORDER_FIELDS:
                        row.setdefault(col, None)
                    yield os.path.basename(path), row, len(parts)


def analyse_orders(customer_ids, product_ids):
    seen = {}
    n_raw = 0
    n_extra_cols = 0

    for _file, row, n_tokens in read_order_files():
        n_raw += 1
        if n_tokens > N_FIELDS:
            n_extra_cols += 1
        # deduplication sur la cle declaree : premiere occurrence gagnante.
        # Les doublons de ce jeu de donnees sont des copies conformes, donc le
        # choix de l'occurrence n'influe sur aucun comptage.
        seen.setdefault(row["order_line_id"], row)

    reasons_counter = Counter()
    n_quarantine = 0
    n_silver = 0
    n_orphan_customer = 0
    n_orphan_product = 0
    n_revenue_lines = 0
    sum_net = Decimal("0.00")
    sum_net_revenue = Decimal("0.00")
    sum_gross = Decimal("0.00")
    order_ids = set()
    status_counter = Counter()
    min_date, max_date = None, None

    for row in seen.values():
        reasons = validate(row)
        if reasons:
            n_quarantine += 1
            for r in reasons:
                reasons_counter[r] += 1
            continue

        n_silver += 1
        qty = Decimal(parse_qty(row["quantity"]))
        unit = clean_decimal(row["unit_price"])
        disc = clean_decimal(row["discount_amount"]) or Decimal("0.00")
        gross = (qty * unit).quantize(Decimal("0.01"))
        net = (gross - disc).quantize(Decimal("0.01"))
        status = normalize_status(row["order_status"])
        ts = parse_ts(row["order_ts"])

        sum_gross += gross
        sum_net += net
        status_counter[status] += 1
        order_ids.add(row["order_id"])
        if status not in NON_REVENUE_STATUSES:
            n_revenue_lines += 1
            sum_net_revenue += net
        if row["customer_id"] not in customer_ids:
            n_orphan_customer += 1
        if row["product_id"] not in product_ids:
            n_orphan_product += 1

        d = ts.date()
        min_date = d if min_date is None or d < min_date else min_date
        max_date = d if max_date is None or d > max_date else max_date

    return {
        "bronze_rows": n_raw,
        "rows_with_extra_columns": n_extra_cols,
        "distinct_order_line_id": len(seen),
        "duplicates_removed": n_raw - len(seen),
        "quarantine_rows": n_quarantine,
        "quarantine_by_reason": dict(sorted(reasons_counter.items())),
        "silver_rows": n_silver,
        "distinct_order_id": len(order_ids),
        "revenue_lines": n_revenue_lines,
        "orphan_customer_rows": n_orphan_customer,
        "orphan_product_rows": n_orphan_product,
        "sum_gross_amount": str(sum_gross),
        "sum_net_amount": str(sum_net),
        "sum_net_amount_revenue_only": str(sum_net_revenue),
        "status_distribution": dict(sorted(status_counter.items())),
        "min_order_date": str(min_date),
        "max_order_date": str(max_date),
    }


# --------------------------------------------------------------------------
# Evenements
# --------------------------------------------------------------------------


def read_event_files():
    for wave in WAVES:
        pattern = os.path.join(ROOT, "data", "waves", wave, "events", "*.jsonl.gz")
        for path in sorted(glob.glob(pattern)):
            with gzip.open(path, "rt", encoding="utf-8") as fh:
                for line in fh:
                    yield os.path.basename(path), line.rstrip("\n")


def analyse_events():
    n_raw = 0
    n_malformed = 0
    seen = {}
    n_epoch_ts = 0

    for _file, line in read_event_files():
        n_raw += 1
        try:
            evt = json.loads(line)
        except json.JSONDecodeError:
            n_malformed += 1
            continue
        if isinstance(evt.get("event_ts"), int):
            n_epoch_ts += 1
        seen.setdefault(evt["event_id"], evt)

    type_counter = Counter()
    n_items = 0
    n_identified = 0
    n_with_items = 0
    for evt in seen.values():
        type_counter[evt["event_type"]] += 1
        items = evt.get("items") or []
        n_items += len(items)
        if items:
            n_with_items += 1
        if (evt.get("user") or {}).get("customer_id"):
            n_identified += 1

    return {
        "bronze_rows": n_raw,
        "malformed_rows": n_malformed,
        "parseable_rows": n_raw - n_malformed,
        "distinct_event_id": len(seen),
        "duplicates_removed": (n_raw - n_malformed) - len(seen),
        "silver_event_rows": len(seen),
        "silver_event_item_rows": n_items,
        "events_with_items": n_with_items,
        "events_with_customer_id": n_identified,
        "events_with_epoch_ts": n_epoch_ts,
        "event_type_distribution": dict(sorted(type_counter.items())),
    }


# --------------------------------------------------------------------------


PRICE_CONTRACT = re.compile(r"^[0-9]+,[0-9]{2}$")


def analyse_contract_and_ldp():
    """Ecarts entre le contrat d'interface declare et la realite (M6),
    plus l'agregat du pipeline declaratif (M7).

    Portee des regles de valeur : les lignes bronze DEDUPLIQUEES sur order_line_id.
    Seule la regle d'unicite se mesure sur le brut, par definition.
    """
    seen = {}
    n_raw = 0
    for _file, row, _n in read_order_files():
        n_raw += 1
        seen.setdefault(row["order_line_id"], row)

    violations = Counter()
    monthly_country = Counter()
    for row in seen.values():
        if parse_ts(row["order_ts"]) is None:
            violations["ORDER_TS_PARSABLE"] += 1
        qty = parse_qty(row["quantity"])
        if qty is None or qty <= 0:
            violations["QUANTITY_POSITIVE"] += 1
        if not PRICE_CONTRACT.match(row["unit_price"] or ""):
            violations["UNIT_PRICE_NUMERIC"] += 1
        if row["currency"] != "EUR":
            violations["CURRENCY_ALWAYS_EUR"] += 1

        # agregat du pipeline declaratif : mois x pays, sur les lignes de CA valides
        if validate(row):
            continue
        status = normalize_status(row["order_status"])
        if status in NON_REVENUE_STATUSES:
            continue
        ts = parse_ts(row["order_ts"])
        monthly_country[(ts.strftime("%Y-%m"), row["shipping_country"])] += 1

    violations["ORDER_LINE_ID_UNIQUE"] = n_raw - len(seen)

    ev_seen = set()
    n_epoch = 0
    for _file, line in read_event_files():
        try:
            evt = json.loads(line)
        except json.JSONDecodeError:
            continue
        if evt["event_id"] in ev_seen:
            continue
        ev_seen.add(evt["event_id"])
        if isinstance(evt.get("event_ts"), int):
            n_epoch += 1
    violations["EVENT_TS_ISO8601"] = n_epoch

    return {
        "scope_orders_deduplicated": len(seen),
        "scope_events_deduplicated": len(ev_seen),
        "contract_violations": dict(sorted(violations.items())),
        "ldp_revenue_by_month_country_rows": len(monthly_country),
        "ldp_revenue_lines": sum(monthly_country.values()),
    }


def main():
    global WAVES, LIST_PRICES

    parser = argparse.ArgumentParser(description="Implementation de reference de la couche silver")
    parser.add_argument("--state", default="S2", choices=sorted(STATES),
                        help="S2 = W1+W2 (M1/M3/M5), S3 = +W3 (M8), S4 = +W4 (M9)")
    parser.add_argument("--scale-rule", action="store_true",
                        help="active la regle de vraisemblance d'echelle (M9)")
    args = parser.parse_args()

    WAVES = STATES[args.state]
    print(f"etat {args.state} : {', '.join(WAVES)}"
          + (" + regle d'echelle" if args.scale_rule else ""))

    categories = build_categories()
    sellers = build_sellers(categories)
    products = build_products(categories, sellers)
    customers = build_customers()

    customer_ids = {c["customer_id"] for c in customers}
    # les 60 comptes crees par la journee d'activite OLTP (voir generate_oltp.py)
    customer_ids |= {f"C90{i:04d}" for i in range(1, 61)}
    product_ids = {p["product_id"] for p in products}

    if args.scale_rule:
        LIST_PRICES = {p["product_id"]: Decimal(p["list_price"]) for p in products}

    print("analyse des commandes…")
    orders = analyse_orders(customer_ids, product_ids)
    print("analyse des evenements…")
    events = analyse_events()

    print("ecarts au contrat et agregat declaratif…")
    contract = analyse_contract_and_ldp()

    out = {"state": args.state, "waves": WAVES, "scale_rule": args.scale_rule,
           "orders": orders, "events": events, "contract_and_ldp": contract}
    suffix = args.state + ("_scale" if args.scale_rule else "")
    path = os.path.join(ROOT, "graders", "expected", f"M3_{suffix}.json")
    with open(path, "w", encoding="utf-8") as fh:
        json.dump(out, fh, indent=2, ensure_ascii=False)

    print(json.dumps(out, indent=2, ensure_ascii=False))
    print(f"\n-> {path}")


if __name__ == "__main__":
    main()
