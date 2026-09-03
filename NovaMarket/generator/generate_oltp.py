#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
NovaMarket - artefacts de la source OLTP (Lakebase Postgres).

Produit, dans data/lakebase/ :

    01_ddl.sql                schema des deux tables applicatives
    app_customers.csv         etat initial (25 000 lignes)
    app_sellers.csv           etat initial (600 lignes)
    02_changes_D1.sql         journee d'activite 1, a rejouer dans Lakebase
    app_customers_v2.csv      etat apres D1                      -> voie fichier
    app_sellers_v2.csv        idem
    03_changes_D2.sql         journee d'activite 2 (utilisee en M4)
    app_customers_v3.csv      etat apres D2
    app_sellers_v3.csv        idem
    expected_changes.json     comptages exacts, pour les graders M2 et M4

Les deux voies d'acces (Lakebase reel ou fichiers) menent au meme resultat : ce qui est
evalue, c'est la logique d'extraction incrementale, pas la plomberie de connexion.

Usage :
    python generator/generate_oltp.py
"""

from __future__ import annotations

import csv
import json
import os

from generate import build_categories, build_customers, build_sellers, rng_for

SEGMENTS = ["STANDARD", "PLUS", "VIP"]
PLANS = ["BASIC", "PLUS", "PREMIUM"]

CUSTOMER_COLS = ["customer_id", "first_name", "last_name", "email", "country", "city",
                 "zip_code", "segment", "is_opt_in", "created_at", "updated_at", "is_deleted"]
SELLER_COLS = ["seller_id", "seller_name", "seller_country", "seller_city",
               "main_top_category", "plan_code", "is_active", "onboarded_at", "updated_at"]

# Les deux journees d'activite applicative.
WAVES = [
    {
        "name": "D1",
        "change_ts": "2026-06-05 08:15:00",
        "n_updated": 300,
        "n_new": 60,
        "new_prefix": "C90",
        "n_deleted": 25,
        "n_sellers": 40,
        # Lignes modifiees exactement a l'horodatage du watermark : le piege de M2.
        "n_boundary": 5,
    },
    {
        "name": "D2",
        "change_ts": "2026-06-06 07:30:00",
        "n_updated": 150,
        "n_new": 20,
        "new_prefix": "C91",
        "n_deleted": 10,
        "n_sellers": 25,
        "n_boundary": 0,
    },
]


def sqlq(value) -> str:
    return str(value).replace("'", "''")


# --------------------------------------------------------------------------
# Etat initial
# --------------------------------------------------------------------------


def customer_rows(customers):
    """Etat initial des clients. updated_at = date de creation : rien n'a encore bouge."""
    return [
        {
            "customer_id": c["customer_id"], "first_name": c["first_name"],
            "last_name": c["last_name"], "email": c["email"], "country": c["country"],
            "city": c["city"], "zip_code": c["zip_code"], "segment": c["segment"],
            "is_opt_in": c["is_opt_in"],
            "created_at": f"{c['created_at']} 09:00:00",
            "updated_at": f"{c['created_at']} 09:00:00",
            "is_deleted": "false",
        }
        for c in customers
    ]


def seller_rows(sellers):
    return [
        {
            "seller_id": s["seller_id"], "seller_name": s["seller_name"],
            "seller_country": s["seller_country"], "seller_city": s["seller_city"],
            "main_top_category": s["main_top_category"], "plan_code": s["plan_code"],
            "is_active": s["is_active"], "onboarded_at": s["onboarded_at"],
            "updated_at": f"{s['onboarded_at']} 09:00:00",
        }
        for s in sellers
    ]


def write_csv(path: str, rows, cols) -> None:
    with open(path, "w", encoding="utf-8", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=cols)
        w.writeheader()
        w.writerows(rows)


def write_ddl(path: str) -> None:
    with open(path, "w", encoding="utf-8", newline="\n") as fh:
        fh.write("""-- NovaMarket : schema de la base applicative (Lakebase Postgres)
-- A executer dans le SQL Editor de ton instance Lakebase.

DROP TABLE IF EXISTS app_customers;
DROP TABLE IF EXISTS app_sellers;

CREATE TABLE app_customers (
  customer_id   text PRIMARY KEY,
  first_name    text,
  last_name     text,
  email         text,
  country       text,
  city          text,
  zip_code      text,
  segment       text,
  is_opt_in     boolean,
  created_at    timestamp,
  updated_at    timestamp,
  is_deleted    boolean DEFAULT false
);

CREATE TABLE app_sellers (
  seller_id         text PRIMARY KEY,
  seller_name       text,
  seller_country    text,
  seller_city       text,
  main_top_category text,
  plan_code         text,
  is_active         boolean,
  onboarded_at      date,
  updated_at        timestamp
);

CREATE INDEX idx_customers_updated_at ON app_customers (updated_at);
CREATE INDEX idx_sellers_updated_at   ON app_sellers   (updated_at);
""")


# --------------------------------------------------------------------------
# Une journee d'activite applicative
# --------------------------------------------------------------------------


def apply_wave(cust_rows, sell_rows, cfg):
    """Applique une journee d'activite. Renvoie (clients, vendeurs, instructions, infos)."""
    rng = rng_for(f"oltp_changes_{cfg['name']}")
    change_ts = cfg["change_ts"]

    by_cust = {r["customer_id"]: dict(r) for r in cust_rows}
    by_sell = {r["seller_id"]: dict(r) for r in sell_rows}

    # Horodatage le plus recent visible avant cette vague : c'est le watermark que
    # portera l'extraction precedente, et donc la cible du piege de bordure.
    wm_customers = max(r["updated_at"] for r in cust_rows)
    wm_sellers = max(r["updated_at"] for r in sell_rows)

    alive = sorted(cid for cid, r in by_cust.items() if r["is_deleted"] == "false")
    pool = rng.sample(alive, cfg["n_updated"] + cfg["n_deleted"] + cfg["n_boundary"])
    updated = pool[:cfg["n_updated"]]
    deleted = pool[cfg["n_updated"]:cfg["n_updated"] + cfg["n_deleted"]]
    boundary = pool[cfg["n_updated"] + cfg["n_deleted"]:]

    stmts = [f"-- Journee d'activite {cfg['name']} — {change_ts}",
             "\n-- 1. Changements de segment et de consentement"]
    for cid in updated:
        row = by_cust[cid]
        new_segment = rng.choice([s for s in SEGMENTS if s != row["segment"]])
        new_opt = str(rng.random() < 0.6).lower()
        row.update(segment=new_segment, is_opt_in=new_opt, updated_at=change_ts)
        stmts.append(
            f"UPDATE app_customers SET segment = '{new_segment}', is_opt_in = {new_opt}, "
            f"updated_at = '{change_ts}' WHERE customer_id = '{cid}';"
        )

    stmts.append("\n-- 2. Nouveaux comptes clients")
    new_ids = []
    for i in range(1, cfg["n_new"] + 1):
        cid = f"{cfg['new_prefix']}{i:04d}"
        new_ids.append(cid)
        src = by_cust[rng.choice(alive)]
        first, last = src["first_name"], src["last_name"]
        email = f"{first.lower()}.{last.lower()}{rng.randint(1, 999)}@example.com"
        by_cust[cid] = {
            "customer_id": cid, "first_name": first, "last_name": last, "email": email,
            "country": "FR", "city": "Paris", "zip_code": "75011", "segment": "STANDARD",
            "is_opt_in": "true", "created_at": change_ts, "updated_at": change_ts,
            "is_deleted": "false",
        }
        stmts.append(
            "INSERT INTO app_customers (customer_id,first_name,last_name,email,country,city,"
            "zip_code,segment,is_opt_in,created_at,updated_at,is_deleted) VALUES "
            f"('{cid}','{sqlq(first)}','{sqlq(last)}','{email}','FR','Paris','75011',"
            f"'STANDARD',true,'{change_ts}','{change_ts}',false);"
        )

    stmts.append("\n-- 3. Suppressions douces (droit a l'effacement)")
    for cid in deleted:
        by_cust[cid].update(is_deleted="true", email="", updated_at=change_ts)
        stmts.append(
            f"UPDATE app_customers SET is_deleted = true, email = NULL, "
            f"updated_at = '{change_ts}' WHERE customer_id = '{cid}';"
        )

    stmts.append(
        "\n-- 4. Changements de plan vendeur\n"
        "--    Ce sont eux qui rendent l'historisation SCD2 obligatoire (M4) : le taux de\n"
        "--    commission d'une commande depend du plan a la date de la commande."
    )
    plan_changes = []
    for sid in rng.sample(sorted(by_sell), cfg["n_sellers"]):
        row = by_sell[sid]
        new_plan = rng.choice([p for p in PLANS if p != row["plan_code"]])
        plan_changes.append({"seller_id": sid, "from": row["plan_code"], "to": new_plan})
        row.update(plan_code=new_plan, updated_at=change_ts)
        stmts.append(
            f"UPDATE app_sellers SET plan_code = '{new_plan}', updated_at = '{change_ts}' "
            f"WHERE seller_id = '{sid}';"
        )

    if boundary:
        stmts.append(
            f"\n-- 5. Mises a jour horodatees EXACTEMENT a {wm_customers}\n"
            "--    (transactions ouvertes avant l'extraction initiale, validees juste apres).\n"
            "--    Une extraction incrementale en `updated_at > watermark` les rate."
        )
        for cid in boundary:
            by_cust[cid].update(city="Toulouse", zip_code="31000", updated_at=wm_customers)
            stmts.append(
                f"UPDATE app_customers SET city = 'Toulouse', zip_code = '31000', "
                f"updated_at = '{wm_customers}' WHERE customer_id = '{cid}';"
            )

    info = {
        "wave": cfg["name"],
        "change_ts": change_ts,
        "watermark_before_customers": wm_customers,
        "watermark_before_sellers": wm_sellers,
        "updated_customers": cfg["n_updated"],
        "new_customers": cfg["n_new"],
        "new_customer_ids": new_ids,
        "soft_deleted_customers": cfg["n_deleted"],
        "soft_deleted_ids": sorted(deleted),
        "boundary_customer_ids": sorted(boundary),
        "updated_sellers": cfg["n_sellers"],
        "plan_changes": plan_changes,
    }

    return ([by_cust[k] for k in sorted(by_cust)],
            [by_sell[k] for k in sorted(by_sell)],
            stmts, info)


def delta(rows, watermark):
    """Lignes que ramene une extraction en `updated_at >= watermark`."""
    return [r for r in rows if r["updated_at"] >= watermark]


# --------------------------------------------------------------------------


def main():
    root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    out = os.path.join(root, "data", "lakebase")
    os.makedirs(out, exist_ok=True)

    categories = build_categories()
    sellers = build_sellers(categories)
    customers = build_customers()

    cust = [customer_rows(customers)]
    sell = [seller_rows(sellers)]
    infos = []

    for i, cfg in enumerate(WAVES):
        c, s, stmts, info = apply_wave(cust[i], sell[i], cfg)
        cust.append(c)
        sell.append(s)
        infos.append(info)
        fname = f"{i + 2:02d}_changes_{cfg['name']}.sql"
        with open(os.path.join(out, fname), "w", encoding="utf-8", newline="\n") as fh:
            fh.write("-- NovaMarket : activite sur la base applicative.\n")
            fh.write("-- A executer entre deux extractions.\n\n")
            fh.write("\n".join(stmts) + "\n")

    write_ddl(os.path.join(out, "01_ddl.sql"))
    for i, suffix in enumerate(["", "_v2", "_v3"]):
        write_csv(os.path.join(out, f"app_customers{suffix}.csv"), cust[i], CUSTOMER_COLS)
        write_csv(os.path.join(out, f"app_sellers{suffix}.csv"), sell[i], SELLER_COLS)

    # Ce que ramene chaque extraction successive, en semantique `>=`.
    wm_c = max(r["updated_at"] for r in cust[0])
    wm_s = max(r["updated_at"] for r in sell[0])
    extractions = [{"n": 1, "customers": len(cust[0]), "sellers": len(sell[0]),
                    "watermark_customers_after": wm_c, "watermark_sellers_after": wm_s}]
    for i in (1, 2):
        dc, ds = delta(cust[i], wm_c), delta(sell[i], wm_s)
        wm_c = max(r["updated_at"] for r in dc)
        wm_s = max(r["updated_at"] for r in ds)
        extractions.append({"n": i + 1, "customers": len(dc), "sellers": len(ds),
                            "watermark_customers_after": wm_c, "watermark_sellers_after": wm_s})

    manifest = {
        "waves": infos,
        "extractions_gte": extractions,
        "journal_customers_rows": sum(e["customers"] for e in extractions),
        "journal_sellers_rows": sum(e["sellers"] for e in extractions),
        "final_customers": len(cust[-1]),
        "final_sellers": len(sell[-1]),
    }
    with open(os.path.join(out, "expected_changes.json"), "w", encoding="utf-8") as fh:
        json.dump(manifest, fh, indent=2, ensure_ascii=False)

    for e in extractions:
        print(f"extraction {e['n']} : {e['customers']:>6} client(s), {e['sellers']:>4} vendeur(s) "
              f"-> watermark {e['watermark_customers_after']}")
    print(f"\njournal bronze attendu : {manifest['journal_customers_rows']} lignes clients, "
          f"{manifest['journal_sellers_rows']} lignes vendeurs")
    print(f"etat final : {manifest['final_customers']} clients, {manifest['final_sellers']} vendeurs")
    print(f"piege de bordure (D1) : {', '.join(infos[0]['boundary_customer_ids'])}")
    print(f"\n-> {out}")


if __name__ == "__main__":
    main()
