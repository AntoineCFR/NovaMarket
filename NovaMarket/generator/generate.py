#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
NovaMarket - generateur de datasets pour le parcours Data Engineer Databricks.

Stdlib uniquement. Deterministe : pour une meme date, le contenu genere est
toujours identique (les RNG sont seedes par crc32(date) et non par le hash
Python, qui est sale entre deux process).

Usage :
    python generator/generate.py --waves W0_ref W1_initial W2
    python generator/generate.py --waves all --clean

Sortie :
    data/waves/<wave>/<source>/<fichier>
    graders/expected/<wave>.json     (comptages attendus, utilises par les graders)
    data/lakebase/lakebase_seed.sql  (source OLTP, utilisee a partir de M2)
"""

from __future__ import annotations

import argparse
import gzip
import json
import os
import random
import shutil
import zlib
from datetime import date, datetime, timedelta

# --------------------------------------------------------------------------
# Parametres globaux
# --------------------------------------------------------------------------

BASE_SEED = 20260605

N_CUSTOMERS = 25_000
N_SELLERS = 600
N_PRODUCTS = 8_000

ORDERS_PER_DAY = 660          # moyenne, module par la saisonnalite
EVENTS_PER_DAY = 8_000
LINES_PER_ORDER = (1, 2, 3, 4, 5)
LINES_PER_ORDER_W = (0.42, 0.28, 0.16, 0.09, 0.05)

CSV_ENCODING = "cp1252"       # export backoffice Windows : accents + € + espace insecable
CSV_SEP = ";"

# Taux de defauts injectes dans les commandes (par ligne)
P_DUP_EXACT = 0.012           # ligne dupliquee telle quelle dans le meme fichier
P_BAD_TS = 0.005              # horodatage inexploitable
P_BAD_QTY = 0.003             # quantite nulle ou negative
P_PRICE_JUNK = 0.004          # prix avec symbole monetaire / espace insecable
P_UNKNOWN_CUSTOMER = 0.006    # client absent du referentiel
P_UNKNOWN_PRODUCT = 0.002     # produit absent du catalogue
P_ADDR_SEP = 0.004            # separateur non echappe dans l'adresse -> colonnes en trop
P_STATUS_CASE = 0.30          # casse incoherente sur le statut

# Taux de defauts injectes dans les evenements
P_EVT_BAD_JSON = 0.003        # ligne JSON non parsable
P_EVT_DUP = 0.005             # event_id duplique
P_EVT_EPOCH_TS = 0.010        # horodatage en epoch millis au lieu d'ISO
P_EVT_STR_NUM = 0.020         # qty / price serialises en chaine
P_EVT_LATE = 0.015            # evenement arrive en retard (horodate la veille)

# --------------------------------------------------------------------------
# Calendrier du scenario. "Aujourd'hui" dans la fiction = 2026-06-05.
# --------------------------------------------------------------------------

HISTORY_MONTHS = ["2025-12", "2026-01", "2026-02", "2026-03", "2026-04", "2026-05"]
EVENT_HISTORY_START = date(2026, 5, 19)
EVENT_HISTORY_END = date(2026, 6, 1)

WAVES = {
    "W0_ref": {
        "label": "Referentiels (chargement initial)",
        "order_months": [],
        "order_days": [],
        "event_days": [],
        "ref": True,
    },
    "W1_initial": {
        "label": "Charge initiale : 6 mois d'historique + 1re journee",
        "order_months": HISTORY_MONTHS,
        "order_days": [date(2026, 6, 1)],
        "event_days": [
            EVENT_HISTORY_START + timedelta(days=i)
            for i in range((EVENT_HISTORY_END - EVENT_HISTORY_START).days + 1)
        ],
        "ref": False,
    },
    "W2": {
        "label": "Incremental J+1 (rejeu partiel de la veille)",
        "order_months": [],
        "order_days": [date(2026, 6, 2)],
        "event_days": [date(2026, 6, 2)],
        "ref": False,
    },
    "W3": {
        "label": "Incremental J+2 (derive de schema)",
        "order_months": [],
        "order_days": [date(2026, 6, 3)],
        "event_days": [date(2026, 6, 3)],
        "ref": False,
    },
    "W4": {
        "label": "Incremental J+3 (incident de production)",
        "order_months": [],
        "order_days": [date(2026, 6, 4)],
        "event_days": [date(2026, 6, 4)],
        "ref": False,
        # Le referentiel produit est re-livre ampute. Le fichier porte le meme nom
        # que d'habitude : rien ne signale l'anomalie.
        "truncated_ref_products": 500,
        # Le fichier de commandes est coupe en plein milieu de sa derniere ligne.
        "truncate_last_line": True,
    },
}

# A partir de cette date, la source ajoute deux colonnes sans prevenir.
SCHEMA_V2_FROM = date(2026, 6, 3)
# Cette journee rejoue une partie des lignes de la veille.
REPLAY_DAYS = {date(2026, 6, 2): 0.04}
# A partir de cette date, les evenements portent un champ supplementaire.
EVENT_V2_FROM = date(2026, 6, 3)

# --------------------------------------------------------------------------
# L'incident de production (vague W4)
# --------------------------------------------------------------------------
# Ce jour-la, la passerelle de facturation d'un lot de vendeurs se met a emettre
# les prix unitaires en CENTIMES, sous forme d'entier sans separateur decimal.
# La ligne reste parfaitement valide : elle parse, elle se caste, elle ne declenche
# ni sauvetage ni quarantaine. Seul le montant est faux, d'un facteur 100.
INCIDENT_DAY = date(2026, 6, 4)
N_INCIDENT_SELLERS = 25

# --------------------------------------------------------------------------
# Vocabulaire
# --------------------------------------------------------------------------

TOP_CATEGORIES = [
    ("HOME", "Maison & Cuisine"),
    ("TECH", "High-Tech"),
    ("FASH", "Mode"),
    ("BEAU", "Beauté & Santé"),
    ("SPORT", "Sport & Plein air"),
    ("KIDS", "Jeux & Enfants"),
    ("BOOK", "Culture & Livres"),
    ("AUTO", "Auto & Bricolage"),
]

SUB_CATEGORIES = {
    "HOME": ["Petit électroménager", "Arts de la table", "Linge de maison", "Rangement", "Luminaires", "Jardin"],
    "TECH": ["Smartphones", "Audio", "Informatique", "Objets connectés", "Photo", "Accessoires"],
    "FASH": ["Prêt-à-porter", "Chaussures", "Maroquinerie", "Montres & Bijoux", "Lingerie"],
    "BEAU": ["Soin du visage", "Parfums", "Capillaire", "Parapharmacie", "Maquillage"],
    "SPORT": ["Running", "Fitness", "Cycles", "Randonnée", "Sports collectifs"],
    "KIDS": ["Jeux de société", "Puériculture", "Figurines", "Jeux éducatifs"],
    "BOOK": ["Romans", "BD & Mangas", "Scolaire", "Vinyles"],
    "AUTO": ["Entretien auto", "Outillage", "Pièces détachées", "Peinture"],
}

BRANDS = [
    "Alvora", "Bexon", "Cirrus", "Delvia", "Ember", "Fjord", "Granit", "Halcyon",
    "Ivory", "Juniper", "Kestrel", "Lumen", "Meridian", "Nordis", "Orbit", "Pallas",
    "Quartz", "Rivage", "Solstice", "Terra", "Ursa", "Vertex", "Windrow", "Zephyr",
]

PRODUCT_NOUNS = {
    "HOME": ["Bouilloire", "Poêle", "Set de couverts", "Housse de couette", "Étagère", "Lampe", "Arrosoir"],
    "TECH": ["Casque", "Enceinte", "Clavier", "Souris", "Chargeur", "Montre connectée", "Webcam"],
    "FASH": ["Chemise", "Sneakers", "Sac à main", "Ceinture", "Écharpe", "Veste", "Montre"],
    "BEAU": ["Crème hydratante", "Sérum", "Eau de toilette", "Shampooing", "Palette", "Brosse"],
    "SPORT": ["Chaussures de trail", "Tapis de yoga", "Haltères", "Gourde", "Sac de sport", "Casque vélo"],
    "KIDS": ["Puzzle", "Jeu de plateau", "Figurine", "Peluche", "Trottinette", "Cube éducatif"],
    "BOOK": ["Roman", "Coffret BD", "Manuel", "Vinyle", "Beau livre"],
    "AUTO": ["Clé à cliquet", "Nettoyant jantes", "Perceuse", "Bâche", "Filtre à air"],
}

QUALIFIERS = ["Compact", "Pro", "Essentiel", "Premium", "Nomade", "XL", "Éco", "Signature", "Classic", "Ultra"]

FIRST_NAMES = [
    "Camille", "Lucas", "Emma", "Hugo", "Léa", "Nathan", "Chloé", "Théo", "Manon", "Enzo",
    "Sarah", "Gabriel", "Inès", "Raphaël", "Jade", "Adam", "Louise", "Noah", "Alice", "Sacha",
    "Zoé", "Ethan", "Anaïs", "Maxime", "Élise", "Antoine", "Julie", "Mehdi", "Fatou", "Yanis",
]

LAST_NAMES = [
    "Martin", "Bernard", "Dubois", "Thomas", "Robert", "Richard", "Petit", "Durand", "Leroy",
    "Moreau", "Simon", "Laurent", "Lefèvre", "Michel", "Garcia", "David", "Bertrand", "Roux",
    "Vincent", "Fournier", "Morel", "Girard", "André", "Mercier", "Blanc", "Guérin", "Boyer",
]

STREETS = [
    "rue de la République", "avenue des Tilleuls", "boulevard Saint-Michel", "impasse du Moulin",
    "chemin des Vignes", "place de l'Église", "rue Victor Hugo", "allée des Peupliers",
    "route de Bordeaux", "quai des Chartrons", "rue du Général Leclerc", "cours Gambetta",
]

CITIES = [
    ("FR", "Paris", "75011"), ("FR", "Lyon", "69003"), ("FR", "Marseille", "13008"),
    ("FR", "Toulouse", "31000"), ("FR", "Nantes", "44000"), ("FR", "Bordeaux", "33000"),
    ("FR", "Lille", "59000"), ("FR", "Strasbourg", "67000"), ("FR", "Rennes", "35000"),
    ("BE", "Bruxelles", "1000"), ("BE", "Anvers", "2000"), ("BE", "Liège", "4000"),
    ("DE", "Berlin", "10115"), ("DE", "Munich", "80331"), ("DE", "Hambourg", "20095"),
    ("ES", "Madrid", "28001"), ("ES", "Barcelone", "08001"), ("ES", "Valence", "46001"),
    ("IT", "Milan", "20121"), ("IT", "Rome", "00184"), ("NL", "Amsterdam", "1011"),
]

PAYMENT_METHODS = ["CARD", "PAYPAL", "TRANSFER", "GIFTCARD", "APPLEPAY"]
PAYMENT_W = (0.62, 0.21, 0.06, 0.04, 0.07)

ORDER_STATUSES = ["DELIVERED", "SHIPPED", "PENDING", "CANCELLED", "RETURNED"]
ORDER_STATUS_W = (0.70, 0.08, 0.05, 0.10, 0.07)

CHANNELS = ["WEB", "MOBILE_APP", "MARKETPLACE_API"]
CHANNEL_W = (0.48, 0.44, 0.08)

PROMO_CODES = ["", "", "", "", "SUMMER10", "WELCOME15", "FREESHIP", "VIP20", "FLASH5"]

SELLER_PLANS = ["BASIC", "PLUS", "PREMIUM"]
SELLER_PLAN_W = (0.58, 0.31, 0.11)
# Taux de commission par palier : la table de reference du gold.
PLAN_COMMISSION = {"BASIC": 0.150, "PLUS": 0.115, "PREMIUM": 0.085}

CUSTOMER_SEGMENTS = ["STANDARD", "PLUS", "VIP"]
CUSTOMER_SEGMENT_W = (0.78, 0.17, 0.05)

EVENT_TYPES = ["page_view", "search", "product_view", "add_to_cart", "checkout_start", "purchase"]

# Profondeur atteinte par une session dans l'entonnoir. C'est ce parametre, et non
# un tirage par evenement, qui donne un taux de conversion mesurable.
SESSION_DEPTHS = ["bounce", "browse", "cart", "checkout", "purchase"]
SESSION_DEPTH_W = (0.34, 0.36, 0.16, 0.08, 0.06)

OS_LIST = ["iOS", "Android", "Windows", "macOS", "Linux"]
OS_W = (0.31, 0.36, 0.22, 0.09, 0.02)

UTM_SOURCES = ["google", "meta", "direct", "newsletter", "tiktok", "affiliate", "bing"]
UTM_MEDIUMS = ["cpc", "organic", "email", "social", "referral", "none"]
UTM_CAMPAIGNS = ["always_on", "soldes_ete", "black_friday", "retargeting_30j", "brand", ""]

SEARCH_TERMS = [
    "casque bluetooth", "chaussures running", "cafetiere", "sac a dos", "montre connectee",
    "tapis de yoga", "creme visage", "clavier mecanique", "puzzle 1000 pieces", "veste impermeable",
]

# --------------------------------------------------------------------------
# Utilitaires
# --------------------------------------------------------------------------


def rng_for(*parts: object) -> random.Random:
    """RNG deterministe entre deux executions (crc32, pas le hash Python)."""
    key = "|".join(str(p) for p in parts).encode("utf-8")
    return random.Random(BASE_SEED ^ zlib.crc32(key))


def wchoice(rng: random.Random, values, weights):
    return rng.choices(values, weights=weights, k=1)[0]


def money(value: float) -> str:
    """Format monetaire francais : virgule decimale."""
    return f"{value:.2f}".replace(".", ",")


def daterange(start: date, end: date):
    cur = start
    while cur <= end:
        yield cur
        cur += timedelta(days=1)


def month_days(month: str):
    year, mon = (int(x) for x in month.split("-"))
    first = date(year, mon, 1)
    last = date(year + (mon == 12), (mon % 12) + 1, 1) - timedelta(days=1)
    return list(daterange(first, last))


def seasonality(d: date) -> float:
    """Multiplicateur de volume : pic de fin d'annee + effet week-end."""
    factor = 1.0
    if d.month == 12:
        factor *= 1.55 if d.day <= 22 else 0.70
    elif d.month == 1:
        factor *= 1.12          # soldes d'hiver
    elif d.month in (6,):
        factor *= 1.08          # soldes d'ete
    if d.weekday() >= 5:
        factor *= 1.18
    if d.weekday() == 0:
        factor *= 1.06
    return factor


# --------------------------------------------------------------------------
# Referentiels
# --------------------------------------------------------------------------


def build_categories():
    rows = []
    cat_id = 0
    for top_code, top_label in TOP_CATEGORIES:
        for sub in SUB_CATEGORIES[top_code]:
            cat_id += 1
            rows.append(
                {
                    "category_id": f"CAT{cat_id:03d}",
                    "category_label": sub,
                    "top_category_code": top_code,
                    "top_category_label": top_label,
                }
            )
    return rows


def build_sellers(categories):
    rng = rng_for("sellers")
    rows = []
    for i in range(1, N_SELLERS + 1):
        country, city, _ = rng.choice(CITIES)
        top = rng.choice(TOP_CATEGORIES)[0]
        created = date(2021, 1, 1) + timedelta(days=rng.randint(0, 1750))
        rows.append(
            {
                "seller_id": f"S{i:04d}",
                "seller_name": f"{rng.choice(BRANDS)} {rng.choice(['Store', 'Shop', 'Market', 'Distribution', 'Group'])}",
                "seller_country": country,
                "seller_city": city,
                "main_top_category": top,
                "plan_code": wchoice(rng, SELLER_PLANS, SELLER_PLAN_W),
                "is_active": "true" if rng.random() > 0.06 else "false",
                "onboarded_at": created.isoformat(),
            }
        )
    return rows


def build_products(categories, sellers):
    rng = rng_for("products")
    by_top = {}
    for c in categories:
        by_top.setdefault(c["top_category_code"], []).append(c)
    rows = []
    for i in range(1, N_PRODUCTS + 1):
        seller = rng.choice(sellers)
        top = seller["main_top_category"] if rng.random() < 0.75 else rng.choice(TOP_CATEGORIES)[0]
        cat = rng.choice(by_top[top])
        noun = rng.choice(PRODUCT_NOUNS[top])
        brand = rng.choice(BRANDS)
        price = round(rng.choice([1, 1, 1, 2, 3]) * rng.uniform(4.5, 95.0), 2)
        rows.append(
            {
                "product_id": f"P{i:06d}",
                "product_name": f"{brand} {noun} {rng.choice(QUALIFIERS)}",
                "brand": brand,
                "category_id": cat["category_id"],
                "seller_id": seller["seller_id"],
                "list_price": f"{price:.2f}",
                "weight_kg": f"{rng.uniform(0.05, 12.0):.3f}",
                "is_discontinued": "true" if rng.random() < 0.08 else "false",
            }
        )
    return rows


def build_customers():
    rng = rng_for("customers")
    rows = []
    for i in range(1, N_CUSTOMERS + 1):
        country, city, zipcode = rng.choice(CITIES)
        created = date(2022, 1, 1) + timedelta(days=rng.randint(0, 1580))
        first = rng.choice(FIRST_NAMES)
        last = rng.choice(LAST_NAMES)
        rows.append(
            {
                "customer_id": f"C{i:06d}",
                "first_name": first,
                "last_name": last,
                "email": f"{first.lower()}.{last.lower()}{rng.randint(1, 999)}@example.com",
                "country": country,
                "city": city,
                "zip_code": zipcode,
                "segment": wchoice(rng, CUSTOMER_SEGMENTS, CUSTOMER_SEGMENT_W),
                "created_at": created.isoformat(),
                "is_opt_in": "true" if rng.random() < 0.63 else "false",
            }
        )
    return rows


# --------------------------------------------------------------------------
# Commandes
# --------------------------------------------------------------------------

ORDER_FIELDS_V1 = [
    "order_id", "order_line_id", "order_ts", "customer_id", "seller_id", "product_id",
    "quantity", "unit_price", "discount_amount", "currency", "shipping_country",
    "payment_method", "order_status", "shipping_address",
]
ORDER_FIELDS_V2 = ORDER_FIELDS_V1 + ["promo_code", "channel"]


def orders_for_date(d: date, products, customers):
    """Genere les lignes de commande d'une journee. Pur et deterministe."""
    rng = rng_for("orders", d.isoformat())
    n_orders = max(1, int(ORDERS_PER_DAY * seasonality(d) * rng.uniform(0.88, 1.12)))
    schema_v2 = d >= SCHEMA_V2_FROM
    fields = ORDER_FIELDS_V2 if schema_v2 else ORDER_FIELDS_V1

    rows = []
    stats = {
        "orders": n_orders,
        "lines_clean": 0,
        "lines_written": 0,
        "dup_exact": 0,
        "bad_ts": 0,
        "bad_qty": 0,
        "price_junk": 0,
        "unknown_customer": 0,
        "unknown_product": 0,
        "extra_columns": 0,
    }

    for o in range(n_orders):
        order_id = f"ORD-{d.strftime('%Y%m%d')}-{o:05d}"
        cust = rng.choice(customers)
        hour = min(23, max(0, int(rng.gauss(15, 4.2))))
        ts = datetime(d.year, d.month, d.day, hour, rng.randint(0, 59), rng.randint(0, 59))
        status = wchoice(rng, ORDER_STATUSES, ORDER_STATUS_W)
        payment = wchoice(rng, PAYMENT_METHODS, PAYMENT_W)
        street = f"{rng.randint(1, 180)} {rng.choice(STREETS)}"
        address = f"{street}, {cust['zip_code']} {cust['city']}"
        n_lines = wchoice(rng, LINES_PER_ORDER, LINES_PER_ORDER_W)
        promo = rng.choice(PROMO_CODES) if schema_v2 else None
        channel = wchoice(rng, CHANNELS, CHANNEL_W) if schema_v2 else None

        for li in range(1, n_lines + 1):
            prod = rng.choice(products)
            qty = wchoice(rng, (1, 2, 3, 4), (0.68, 0.20, 0.08, 0.04))
            base = float(prod["list_price"])
            unit = round(base * rng.uniform(0.80, 1.05), 2)
            discount = round(unit * qty * rng.choice([0, 0, 0, 0.05, 0.10, 0.15]), 2)

            row = {
                "order_id": order_id,
                "order_line_id": f"{order_id}-{li}",
                "order_ts": ts.strftime("%Y-%m-%d %H:%M:%S"),
                "customer_id": cust["customer_id"],
                "seller_id": prod["seller_id"],
                "product_id": prod["product_id"],
                "quantity": str(qty),
                "unit_price": money(unit),
                "discount_amount": money(discount),
                "currency": "EUR",
                "shipping_country": cust["country"],
                "payment_method": payment,
                "order_status": status,
                "shipping_address": address,
            }
            if schema_v2:
                row["promo_code"] = promo
                row["channel"] = channel

            stats["lines_clean"] += 1

            # ---- injection des defauts -------------------------------------
            if rng.random() < P_STATUS_CASE:
                row["order_status"] = rng.choice(
                    [status.lower(), status.capitalize(), f" {status} "]
                )

            if rng.random() < P_BAD_TS:
                row["order_ts"] = rng.choice(
                    ["", "0000-00-00 00:00:00", f"{d.month:02d}/{d.day:02d}/{d.year}",
                     f"{d.isoformat()}T{hour:02d}:{rng.randint(0, 59):02d}", "2026-13-45 99:99:99"]
                )
                stats["bad_ts"] += 1

            if rng.random() < P_BAD_QTY:
                row["quantity"] = rng.choice(["0", "-1", "-2"])
                stats["bad_qty"] += 1

            if rng.random() < P_PRICE_JUNK:
                # symbole monetaire, espace insecable (U+00A0), point decimal anglo-saxon
                row["unit_price"] = rng.choice(
                    [f"{money(unit)} €", f" {money(unit)}", f"{unit:.2f}", f"EUR {money(unit)}"]
                )
                stats["price_junk"] += 1

            if rng.random() < P_UNKNOWN_CUSTOMER:
                row["customer_id"] = f"C9{rng.randint(90000, 99999):05d}"
                stats["unknown_customer"] += 1

            if rng.random() < P_UNKNOWN_PRODUCT:
                row["product_id"] = f"P9{rng.randint(90000, 99999):05d}"
                stats["unknown_product"] += 1

            # --- l'incident de W4 : prix emis en centimes ---------------------
            # Un vendeur sur 24. La valeur reste un nombre parfaitement valide :
            # aucun controle technique ne peut la distinguer d'un prix normal.
            if d == INCIDENT_DAY and int(row["seller_id"][1:]) % 24 == 7:
                row["unit_price"] = str(int(round(unit * 100)))
                stats["incident_cents_lines"] = stats.get("incident_cents_lines", 0) + 1

            has_extra = rng.random() < P_ADDR_SEP
            if has_extra:
                # separateur non echappe : la ligne aura des colonnes en trop
                row["shipping_address"] = (
                    f"{street}; Batiment {rng.choice('ABCD')}; {cust['zip_code']} {cust['city']}"
                )
                stats["extra_columns"] += 1

            rows.append((row, fields))
            stats["lines_written"] += 1

            if rng.random() < P_DUP_EXACT:
                rows.append((dict(row), fields))
                stats["lines_written"] += 1
                stats["dup_exact"] += 1

    # rejeu partiel de la veille (re-emission d'un fichier amont)
    if d in REPLAY_DAYS:
        prev_rows, _ = orders_for_date(d - timedelta(days=1), products, customers)
        rrng = rng_for("replay", d.isoformat())
        k = int(len(prev_rows) * REPLAY_DAYS[d])
        replayed = rrng.sample(prev_rows, k)
        for row, _f in replayed:
            row = dict(row)
            if schema_v2:                      # aligner sur le schema du jour
                row.setdefault("promo_code", "")
                row.setdefault("channel", "WEB")
            rows.append((row, fields))
        stats["replayed_from_previous_day"] = k
        stats["lines_written"] += k

    return rows, stats


def write_orders_csv(path: str, rows, fields, truncate_last: bool = False):
    """Ecrit le fichier de commandes.

    `truncate_last` coupe la derniere ligne en plein milieu, comme le ferait un
    transfert interrompu.
    """
    os.makedirs(os.path.dirname(path), exist_ok=True)
    lines = [CSV_SEP.join(fields)]
    for row, _f in rows:
        lines.append(CSV_SEP.join(str(row.get(c, "")) for c in fields))

    with open(path, "w", encoding=CSV_ENCODING, newline="", errors="replace") as fh:
        for line in lines[:-1]:
            fh.write(line + "\r\n")
        last = lines[-1]
        fh.write(last[: int(len(last) * 0.4)] if truncate_last else last + "\r\n")


# --------------------------------------------------------------------------
# Evenements
# --------------------------------------------------------------------------


def events_for_date(d: date, products, customers):
    """Genere une journee d'evenements, session par session.

    Une session regroupe plusieurs evenements partageant le meme visiteur, le meme
    terminal et la meme source d'acquisition. Sa "profondeur" decide jusqu'ou le
    visiteur est alle dans l'entonnoir : c'est ce qui rend le taux de conversion
    mesurable (question 5 du gold).
    """
    rng = rng_for("events", d.isoformat())
    target = int(EVENTS_PER_DAY * seasonality(d) * rng.uniform(0.9, 1.1))
    v2 = d >= EVENT_V2_FROM
    lines = []
    stats = {"events": 0, "sessions": 0, "bad_json": 0, "dup_event_id": 0,
             "epoch_ts": 0, "late": 0, "lines_written": 0}
    seq = 0

    while stats["events"] < target:
        # ---- attributs portes par la session entiere ----------------------
        depth = wchoice(rng, SESSION_DEPTHS, SESSION_DEPTH_W)
        cust = rng.choice(customers)
        customer_id = cust["customer_id"] if rng.random() > 0.12 else None
        session_id = f"S-{rng.getrandbits(48):012x}"
        device = {
            "os": wchoice(rng, OS_LIST, OS_W),
            "app_version": f"{rng.randint(3, 5)}.{rng.randint(0, 18)}.{rng.randint(0, 9)}",
            "is_mobile": rng.random() < 0.68,
        }
        utm = {
            "source": rng.choice(UTM_SOURCES),
            "medium": rng.choice(UTM_MEDIUMS),
            "campaign": rng.choice(UTM_CAMPAIGNS),
        }
        referrer = rng.choice(["", "https://www.google.com/", "https://m.facebook.com/",
                               "app://novamarket"])
        experiment = rng.choice(["exp_reco_v3", "exp_pdp_layout", "control", None]) if v2 else None

        hour = min(23, max(0, int(rng.gauss(16, 4.5))))
        ts = datetime(d.year, d.month, d.day, hour, rng.randint(0, 59), rng.randint(0, 59))
        if rng.random() < P_EVT_LATE:      # session arrivee en retard : horodatee la veille
            ts -= timedelta(days=1)
            stats["late"] += 1

        # ---- le parcours ---------------------------------------------------
        viewed = [rng.choice(products) for _ in range(rng.randint(1, 4))]
        steps = [("page_view", None)] * rng.randint(1, 2)
        if rng.random() < 0.45:
            steps.append(("search", None))
        if depth != "bounce":
            steps.extend(("product_view", p) for p in viewed)
            if depth in ("cart", "checkout", "purchase"):
                steps.append(("add_to_cart", rng.choice(viewed)))
            if depth in ("checkout", "purchase"):
                steps.append(("checkout_start", None))
            if depth == "purchase":
                steps.append(("purchase", None))

        cart = []
        for p in rng.sample(viewed, min(len(viewed), rng.randint(1, 3))):
            qty = rng.randint(1, 3)
            price = round(float(p["list_price"]) * rng.uniform(0.8, 1.05), 2)
            if rng.random() < P_EVT_STR_NUM:
                cart.append({"product_id": p["product_id"], "qty": str(qty), "price": money(price)})
            else:
                cart.append({"product_id": p["product_id"], "qty": qty, "price": price})

        stats["sessions"] += 1

        for etype, prod in steps:
            ts += timedelta(seconds=rng.randint(15, 300))
            page = {
                "page_view": "/home",
                "search": "/search",
                "product_view": f"/p/{prod['product_id']}" if prod else "/p/unknown",
                "add_to_cart": "/cart",
                "checkout_start": "/checkout",
                "purchase": "/checkout/confirm",
            }[etype]

            evt = {
                "event_id": f"E-{d.strftime('%Y%m%d')}-{seq:07d}",
                "event_ts": ts.strftime("%Y-%m-%dT%H:%M:%SZ"),
                "event_type": etype,
                "user": {
                    "customer_id": customer_id,
                    "session_id": session_id,
                    "segment": cust["segment"],
                },
                "device": dict(device),
                "context": {"page": page, "referrer": referrer, "utm": dict(utm)},
                "search_term": rng.choice(SEARCH_TERMS) if etype == "search" else None,
                "items": cart if etype in ("add_to_cart", "checkout_start", "purchase") else [],
                "order_id": (f"ORD-{d.strftime('%Y%m%d')}-{rng.randint(0, 600):05d}"
                             if etype == "purchase" else None),
            }
            if v2:
                evt["experiment_id"] = experiment

            seq += 1

            if rng.random() < P_EVT_EPOCH_TS:
                evt["event_ts"] = int(ts.timestamp() * 1000)
                stats["epoch_ts"] += 1

            line = json.dumps(evt, ensure_ascii=False, separators=(",", ":"))

            if rng.random() < P_EVT_BAD_JSON:
                line = line[: max(20, int(len(line) * rng.uniform(0.3, 0.7)))]
                stats["bad_json"] += 1

            lines.append(line)
            stats["events"] += 1
            stats["lines_written"] += 1

            if rng.random() < P_EVT_DUP:
                lines.append(line)
                stats["dup_event_id"] += 1
                stats["lines_written"] += 1

    return lines, stats


def write_events_jsonl_gz(path: str, lines):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with gzip.open(path, "wt", encoding="utf-8", newline="\n") as fh:
        for line in lines:
            fh.write(line + "\n")


# --------------------------------------------------------------------------
# Referentiels : ecriture
# --------------------------------------------------------------------------


def write_ref_csv(path: str, rows, fields):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8", newline="") as fh:
        fh.write(",".join(fields) + "\n")
        for row in rows:
            vals = []
            for f in fields:
                v = str(row[f])
                vals.append(f'"{v}"' if ("," in v or '"' in v) else v)
            fh.write(",".join(vals) + "\n")


# --------------------------------------------------------------------------
# Orchestration
# --------------------------------------------------------------------------


def generate(waves, out_root: str, clean: bool):
    data_root = os.path.join(out_root, "data")
    waves_root = os.path.join(data_root, "waves")
    expected_root = os.path.join(out_root, "graders", "expected")

    categories = build_categories()
    sellers = build_sellers(categories)
    products = build_products(categories, sellers)
    customers = build_customers()

    for wave in waves:
        spec = WAVES[wave]
        wave_dir = os.path.join(waves_root, wave)
        if clean and os.path.isdir(wave_dir):
            shutil.rmtree(wave_dir)

        summary = {"wave": wave, "label": spec["label"], "files": {}, "totals": {}}

        if spec["ref"]:
            write_ref_csv(
                os.path.join(wave_dir, "ref", "categories.csv"),
                categories, ["category_id", "category_label", "top_category_code", "top_category_label"],
            )
            write_ref_csv(
                os.path.join(wave_dir, "ref", "sellers.csv"),
                sellers, ["seller_id", "seller_name", "seller_country", "seller_city",
                          "main_top_category", "plan_code", "is_active", "onboarded_at"],
            )
            write_ref_csv(
                os.path.join(wave_dir, "ref", "products.csv"),
                products, ["product_id", "product_name", "brand", "category_id", "seller_id",
                           "list_price", "weight_kg", "is_discontinued"],
            )
            summary["files"]["ref/categories.csv"] = len(categories)
            summary["files"]["ref/sellers.csv"] = len(sellers)
            summary["files"]["ref/products.csv"] = len(products)
            summary["totals"] = {
                "categories": len(categories),
                "sellers": len(sellers),
                "products": len(products),
            }
            # La source OLTP a son propre generateur : voir generator/generate_oltp.py

        agg = {}
        for month in spec["order_months"]:
            rows, stats = [], {}
            for d in month_days(month):
                r, s = orders_for_date(d, products, customers)
                rows.extend(r)
                for k, v in s.items():
                    stats[k] = stats.get(k, 0) + v
            fields = rows[0][1] if rows else ORDER_FIELDS_V1
            name = f"orders/orders_{month}.csv"
            write_orders_csv(os.path.join(wave_dir, name), rows, fields)
            summary["files"][name] = len(rows)
            for k, v in stats.items():
                agg[k] = agg.get(k, 0) + v

        for d in spec["order_days"]:
            rows, stats = orders_for_date(d, products, customers)
            fields = rows[0][1] if rows else ORDER_FIELDS_V1
            name = f"orders/orders_{d.isoformat()}.csv"
            truncate = bool(spec.get("truncate_last_line"))
            write_orders_csv(os.path.join(wave_dir, name), rows, fields, truncate_last=truncate)
            # La ligne coupee reste une ligne physique : Spark la lira, avec ses
            # colonnes manquantes a null. Elle compte donc dans le bronze.
            summary["files"][name] = len(rows)
            if truncate:
                stats["truncated_last_line"] = 1
            for k, v in stats.items():
                agg[k] = agg.get(k, 0) + v

        # Referentiel produits re-livre ampute : le fichier porte le meme nom,
        # rien ne signale l'anomalie.
        n_truncated = spec.get("truncated_ref_products")
        if n_truncated:
            write_ref_csv(
                os.path.join(wave_dir, "ref", "products.csv"),
                products[:n_truncated],
                ["product_id", "product_name", "brand", "category_id", "seller_id",
                 "list_price", "weight_kg", "is_discontinued"],
            )
            summary["files"]["ref/products.csv"] = n_truncated
            agg["truncated_ref_products"] = n_truncated

        eagg = {}
        for d in spec["event_days"]:
            lines, stats = events_for_date(d, products, customers)
            name = f"events/events_{d.isoformat()}.jsonl.gz"
            write_events_jsonl_gz(os.path.join(wave_dir, name), lines)
            summary["files"][name] = len(lines)
            for k, v in stats.items():
                eagg[k] = eagg.get(k, 0) + v

        if agg:
            summary["totals"]["orders"] = agg
        if eagg:
            summary["totals"]["events"] = eagg

        os.makedirs(expected_root, exist_ok=True)
        with open(os.path.join(expected_root, f"{wave}.json"), "w", encoding="utf-8") as fh:
            json.dump(summary, fh, indent=2, ensure_ascii=False)

        n_files = len(summary["files"])
        n_rows = sum(summary["files"].values())
        print(f"[{wave}] {spec['label']}")
        print(f"         {n_files} fichier(s), {n_rows:,} lignes -> {wave_dir}".replace(",", " "))


def main():
    parser = argparse.ArgumentParser(description="Generateur de datasets NovaMarket")
    parser.add_argument("--waves", nargs="+", default=["W0_ref", "W1_initial", "W2"],
                        help="vagues a generer, ou 'all'")
    parser.add_argument("--out", default=os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                        help="racine du projet")
    parser.add_argument("--clean", action="store_true", help="vider les repertoires de vague avant generation")
    args = parser.parse_args()

    waves = list(WAVES) if args.waves == ["all"] else args.waves
    unknown = [w for w in waves if w not in WAVES]
    if unknown:
        raise SystemExit(f"Vague(s) inconnue(s) : {unknown}. Connues : {list(WAVES)}")

    generate(waves, args.out, args.clean)


if __name__ == "__main__":
    main()
