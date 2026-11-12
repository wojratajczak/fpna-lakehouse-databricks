#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Generator danych zrodlowych pod projekt portfolio "pipeline FP&A na Databricks".

Tworzy dane, ktore wygladaja jak realny feed kosztowy z ERP: miesieczne pliki,
spozniona korekty, brudne kody kont, reorganizacje centrow kosztow w polowie roku
i zmiane schematu w trakcie historii.

Uruchomienie:
    python generate_source_data.py --out ./data
    python generate_source_data.py --out ./data --rows 200000 --seed 42

Wymaga wylacznie biblioteki standardowej (bez pandas).

Struktura wyjscia:
    data/
      landing/costs/costs_YYYY_MM.csv     24 pliki - to czyta Auto Loader (bronze)
      landing/fx/fx_rates.csv             kursy NBP (srednie miesieczne) lub fallback
      reference/chart_of_accounts.csv     plan kont - wymiar
      reference/cost_centers.csv          centra kosztow z historia - zrodlo pod SCD2
      _manifest.json                      co zostalo wygenerowane i z jakimi "wadami"
"""

import argparse
import csv
import json
import os
import random
import urllib.request
import urllib.error
from datetime import date, timedelta
from decimal import Decimal, ROUND_HALF_UP

CURRENCIES = ["EUR", "USD", "GBP"]
BASE = "PLN"

# --- plan kont: 6 grup, razem 60 pozycji -------------------------------------
ACCOUNT_GROUPS = [
    ("4", "Koszty rodzajowe", [
        "Amortyzacja", "Zuzycie materialow", "Zuzycie energii", "Uslugi obce",
        "Podatki i oplaty", "Wynagrodzenia", "Ubezpieczenia spoleczne",
        "Pozostale koszty rodzajowe", "Transport", "Remonty",
    ]),
    ("5", "Koszty wedlug typu dzialalnosci", [
        "Koszty produkcji", "Koszty wydzialowe", "Koszty zarzadu",
        "Koszty sprzedazy", "Koszty zakupu", "Koszty magazynowania",
        "Koszty jakosci", "Koszty utrzymania ruchu", "Koszty IT", "Koszty HR",
    ]),
    ("6", "Rozliczenia miedzyokresowe", [
        "RMK czynne", "RMK bierne", "Rezerwy na urlopy", "Rezerwy na premie",
        "Odpisy aktualizujace", "Rozliczenia miedzyokresowe przychodow",
        "Koszty przyszlych okresow", "Przychody przyszlych okresow",
        "Rozliczenie kosztow zakupu", "Rozliczenie odchylen",
    ]),
    ("7", "Przychody i koszty finansowe", [
        "Odsetki od kredytow", "Roznice kursowe ujemne", "Roznice kursowe dodatnie",
        "Prowizje bankowe", "Odsetki otrzymane", "Koszty leasingu finansowego",
        "Odpisy na naleznosci", "Koszty faktoringu", "Pozostale koszty finansowe",
        "Pozostale przychody finansowe",
    ]),
    ("2", "Rozrachunki", [
        "Rozrachunki z dostawcami", "Rozrachunki z odbiorcami",
        "Rozrachunki publicznoprawne", "Rozrachunki z pracownikami",
        "Rozrachunki wewnatrzgrupowe", "VAT naliczony", "VAT nalezny",
        "Zaliczki na dostawy", "Naleznosci sporne", "Pozostale rozrachunki",
    ]),
    ("3", "Zapasy", [
        "Materialy", "Produkcja w toku", "Wyroby gotowe", "Towary",
        "Odchylenia od cen ewidencyjnych", "Odpis aktualizujacy zapasy",
        "Materialy w drodze", "Opakowania", "Czesci zamienne", "Zlom",
    ]),
]

# --- centra kosztow -----------------------------------------------------------
# Uwaga: CC-204 i CC-205 zostaja zlaczone w CC-250 w polowie historii.
# To jest Twoj przypadek pod SCD typ 2 - nie usuwaj go, bo na nim stoi tydzien T4.
COST_CENTERS = [
    ("CC-101", "Produkcja Linia A", "Operations", "PL"),
    ("CC-102", "Produkcja Linia B", "Operations", "PL"),
    ("CC-103", "Magazyn centralny", "Operations", "PL"),
    ("CC-104", "Utrzymanie ruchu", "Operations", "PL"),
    ("CC-201", "Sprzedaz Polska", "Commercial", "PL"),
    ("CC-202", "Sprzedaz DACH", "Commercial", "DE"),
    ("CC-203", "Sprzedaz UK", "Commercial", "GB"),
    ("CC-204", "Marketing Produktowy", "Commercial", "PL"),
    ("CC-205", "Marketing Cyfrowy", "Commercial", "PL"),
    ("CC-301", "Finanse i Controlling", "Corporate", "PL"),
    ("CC-302", "IT", "Corporate", "PL"),
    ("CC-303", "HR", "Corporate", "PL"),
]
MERGED_AWAY = {"CC-204", "CC-205"}
MERGED_INTO = ("CC-250", "Marketing", "Commercial", "PL")

ENTITIES = [("PL01", "PLN"), ("DE01", "EUR"), ("UK01", "GBP"), ("US01", "USD")]


def months_back(n, end_year, end_month):
    out = []
    y, m = end_year, end_month
    for _ in range(n):
        out.append((y, m))
        m -= 1
        if m == 0:
            y, m = y - 1, 12
    return list(reversed(out))


def month_end(y, m):
    return date(y + (m == 12), (m % 12) + 1, 1) - timedelta(days=1)


def build_accounts():
    rows = []
    for prefix, group, names in ACCOUNT_GROUPS:
        for i, name in enumerate(names, start=1):
            rows.append({
                "account_code": f"{prefix}{i:02d}0",
                "account_name": name,
                "account_group": group,
                "pnl_or_bs": "P&L" if prefix in ("4", "5", "7") else "BS",
            })
    return rows


def build_cost_centers(periods):
    """Zwraca wiersze z historia waznosci - surowe zrodlo pod SCD2."""
    first = periods[0]
    last = periods[-1]
    split_idx = len(periods) // 2
    reorg = periods[split_idx]
    reorg_from = date(reorg[0], reorg[1], 1)

    rows = []
    for code, name, segment, country in COST_CENTERS:
        valid_to = (reorg_from - timedelta(days=1)).isoformat() if code in MERGED_AWAY else "9999-12-31"
        rows.append({
            "cost_center": code, "cost_center_name": name, "segment": segment,
            "country": country,
            "valid_from": date(first[0], first[1], 1).isoformat(),
            "valid_to": valid_to,
        })
    code, name, segment, country = MERGED_INTO
    rows.append({
        "cost_center": code, "cost_center_name": name, "segment": segment,
        "country": country, "valid_from": reorg_from.isoformat(),
        "valid_to": "9999-12-31",
    })
    # jedna zwykla zmiana nazwy w trakcie - drugi przypadek SCD2
    rows.append({
        "cost_center": "CC-302", "cost_center_name": "IT & Digital",
        "segment": "Corporate", "country": "PL",
        "valid_from": date(last[0], last[1], 1).isoformat(), "valid_to": "9999-12-31",
    })
    for r in rows:
        if r["cost_center"] == "CC-302" and r["cost_center_name"] == "IT":
            r["valid_to"] = (date(last[0], last[1], 1) - timedelta(days=1)).isoformat()
    return rows, reorg_from


def active_centers(periods, reorg_from, y, m):
    first_of_month = date(y, m, 1)
    out = []
    for code, *_ in COST_CENTERS:
        if code in MERGED_AWAY and first_of_month >= reorg_from:
            continue
        out.append(code)
    if first_of_month >= reorg_from:
        out.append(MERGED_INTO[0])
    return out


def fetch_nbp(cur, start, end, timeout=12):
    """Srednie kursy NBP (tabela A). Zwraca {(y,m): [kursy]} albo None przy bledzie."""
    url = (f"https://api.nbp.pl/api/exchangerates/rates/a/{cur.lower()}/"
           f"{start.isoformat()}/{end.isoformat()}/?format=json")
    try:
        with urllib.request.urlopen(url, timeout=timeout) as resp:
            payload = json.loads(resp.read().decode("utf-8"))
    except Exception:
        return None
    buckets = {}
    for rate in payload.get("rates", []):
        d = date.fromisoformat(rate["effectiveDate"])
        buckets.setdefault((d.year, d.month), []).append(float(rate["mid"]))
    return buckets


def build_fx(periods, rng, offline=False):
    """Srednie miesieczne kursy. Najpierw NBP, przy braku sieci - deterministyczny fallback."""
    start = date(periods[0][0], periods[0][1], 1)
    end = month_end(*periods[-1])
    source = "NBP api.nbp.pl tabela A (srednia miesieczna z notowan dziennych)"
    buckets = {}

    if not offline:
        for cur in CURRENCIES:
            merged, chunk_start = {}, start
            while chunk_start <= end:          # NBP: max 367 dni na zapytanie
                chunk_end = min(date(chunk_start.year + 1, chunk_start.month, 1)
                                - timedelta(days=1), end)
                part = fetch_nbp(cur, chunk_start, chunk_end)
                if part is None:
                    merged = None
                    break
                for k, v in part.items():
                    merged.setdefault(k, []).extend(v)
                chunk_start = chunk_end + timedelta(days=1)
            if merged is None:
                buckets = {}
                break
            buckets[cur] = merged

    if not buckets:
        source = "FALLBACK - syntetyczny bladzenie losowe (brak polaczenia z api.nbp.pl)"
        anchors = {"EUR": 4.30, "USD": 3.95, "GBP": 5.05}
        buckets = {}
        for cur in CURRENCIES:
            level, series = anchors[cur], {}
            for y, m in periods:
                level *= (1 + rng.gauss(0, 0.012))
                series[(y, m)] = [level]
            buckets[cur] = series

    rows = []
    for cur in CURRENCIES:
        for y, m in periods:
            vals = buckets.get(cur, {}).get((y, m))
            if not vals:
                continue
            avg = sum(vals) / len(vals)
            rows.append({
                "period": f"{y}-{m:02d}",
                "from_currency": cur,
                "to_currency": BASE,
                "rate_type": "AVG",
                "rate": str(Decimal(avg).quantize(Decimal("0.0001"), ROUND_HALF_UP)),
            })
    rows.append({"period": f"{periods[0][0]}-{periods[0][1]:02d}", "from_currency": BASE,
                 "to_currency": BASE, "rate_type": "AVG", "rate": "1.0000"})
    return rows, source


def dirty_account(code, rng):
    """Brudzi kod konta tak, jak brudzi go realny eksport z ERP."""
    roll = rng.random()
    if roll < 0.04:
        return " " + code
    if roll < 0.07:
        return code + " "
    if roll < 0.09:
        return "0" + code
    if roll < 0.10:
        return code.lower()
    return code


def write_csv(path, rows, fieldnames):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", newline="", encoding="utf-8") as fh:
        w = csv.DictWriter(fh, fieldnames=fieldnames, extrasaction="ignore")
        w.writeheader()
        w.writerows(rows)


def main():
    ap = argparse.ArgumentParser(description="Generator danych zrodlowych FP&A")
    ap.add_argument("--out", default="./data", help="katalog wyjsciowy")
    ap.add_argument("--rows", type=int, default=200000, help="docelowa liczba wierszy kosztowych")
    ap.add_argument("--months", type=int, default=24)
    ap.add_argument("--end-year", type=int, default=2026)
    ap.add_argument("--end-month", type=int, default=10)
    ap.add_argument("--seed", type=int, default=42)
    ap.add_argument("--offline", action="store_true", help="pomin NBP, uzyj fallbacku")
    args = ap.parse_args()

    rng = random.Random(args.seed)
    periods = months_back(args.months, args.end_year, args.end_month)

    accounts = build_accounts()
    pnl_accounts = [a["account_code"] for a in accounts if a["pnl_or_bs"] == "P&L"]
    cc_rows, reorg_from = build_cost_centers(periods)
    fx_rows, fx_source = build_fx(periods, rng, offline=args.offline)

    write_csv(os.path.join(args.out, "reference", "chart_of_accounts.csv"), accounts,
              ["account_code", "account_name", "account_group", "pnl_or_bs"])
    write_csv(os.path.join(args.out, "reference", "cost_centers.csv"), cc_rows,
              ["cost_center", "cost_center_name", "segment", "country", "valid_from", "valid_to"])
    write_csv(os.path.join(args.out, "landing", "fx", "fx_rates.csv"), fx_rows,
              ["period", "from_currency", "to_currency", "rate_type", "rate"])

    per_month = max(1, args.rows // len(periods))
    schema_change_at = len(periods) // 2 + 1   # od tego miesiaca dochodzi kolumna project_code
    stats = {"files": [], "late_corrections": 0, "duplicates": 0,
             "unknown_accounts": 0, "null_amounts": 0, "total_rows": 0}
    doc_seq = 1

    for idx, (y, m) in enumerate(periods, start=1):
        centers = active_centers(periods, reorg_from, y, m)
        last_day = month_end(y, m)
        has_project = idx >= schema_change_at
        rows = []

        n = per_month + rng.randint(-per_month // 10, per_month // 10)
        for _ in range(n):
            entity, ent_cur = rng.choice(ENTITIES)
            currency = ent_cur if rng.random() < 0.8 else rng.choice([BASE] + CURRENCIES)
            acc = rng.choice(pnl_accounts)
            amount = round(rng.lognormvariate(6.2, 1.25), 2)
            if rng.random() < 0.06:          # storna / korekty na minus
                amount = -amount
            row = {
                "period": f"{y}-{m:02d}",
                "posting_date": date(y, m, rng.randint(1, last_day.day)).isoformat(),
                "entity": entity,
                "cost_center": rng.choice(centers),
                "account_code": dirty_account(acc, rng),
                "currency": currency,
                "amount": f"{amount:.2f}",
                "source_doc": f"DOC{doc_seq:08d}",
                "created_at": f"{last_day.isoformat()}T23:5{rng.randint(0,9)}:00",
            }
            doc_seq += 1

            if rng.random() < 0.003:         # konto spoza slownika -> material na expectations
                row["account_code"] = f"9{rng.randint(100, 999)}"
                stats["unknown_accounts"] += 1
            if rng.random() < 0.002:         # pusta kwota
                row["amount"] = ""
                stats["null_amounts"] += 1
            if has_project:
                row["project_code"] = (f"PRJ-{rng.randint(1, 40):03d}"
                                       if rng.random() < 0.35 else "")
            rows.append(row)

        # spoznione korekty do zamknietych miesiecy - bez nich MERGE w silver nie ma sensu
        if idx > 2:
            for _ in range(max(3, n // 120)):
                back = rng.choice([1, 2])
                py, pm = periods[idx - 1 - back]
                src = rng.choice(rows)
                corr = dict(src)
                corr["period"] = f"{py}-{pm:02d}"
                corr["posting_date"] = month_end(py, pm).isoformat()
                corr["amount"] = f"{-float(src['amount'] or 0):.2f}"
                corr["source_doc"] = src["source_doc"]      # ten sam klucz = upsert, nie append
                corr["created_at"] = f"{last_day.isoformat()}T23:59:00"
                rows.append(corr)
                stats["late_corrections"] += 1

        # duplikaty - realny eksport potrafi wyslac ten sam wiersz dwa razy
        for _ in range(max(2, n // 200)):
            rows.append(dict(rng.choice(rows)))
            stats["duplicates"] += 1

        rng.shuffle(rows)
        fields = ["period", "posting_date", "entity", "cost_center", "account_code",
                  "currency", "amount", "source_doc", "created_at"]
        if has_project:
            fields.append("project_code")
        fname = f"costs_{y}_{m:02d}.csv"
        write_csv(os.path.join(args.out, "landing", "costs", fname), rows, fields)
        stats["files"].append({"file": fname, "rows": len(rows), "has_project_code": has_project})
        stats["total_rows"] += len(rows)

    manifest = {
        "generated_for": "Projekt portfolio: pipeline FP&A na Databricks",
        "seed": args.seed,
        "periods": [f"{y}-{m:02d}" for y, m in periods],
        "fx_source": fx_source,
        "reorg_effective_from": reorg_from.isoformat(),
        "reorg_note": "CC-204 i CC-205 zlaczone w CC-250; CC-302 zmienia nazwe w ostatnim miesiacu",
        "schema_change": f"kolumna project_code pojawia sie od pliku nr {schema_change_at}",
        "intentional_data_issues": {
            "brudne kody kont": "spacje wiodace/koncowe, wiodace zero, male litery (~10% wierszy)",
            "konta spoza slownika": stats["unknown_accounts"],
            "puste kwoty": stats["null_amounts"],
            "duplikaty": stats["duplicates"],
            "spoznione korekty": stats["late_corrections"],
        },
        "total_rows": stats["total_rows"],
        "files": stats["files"],
    }
    with open(os.path.join(args.out, "_manifest.json"), "w", encoding="utf-8") as fh:
        json.dump(manifest, fh, ensure_ascii=False, indent=2)

    print(f"OK  {stats['total_rows']} wierszy w {len(periods)} plikach -> {args.out}")
    print(f"    FX: {fx_source}")
    print(f"    celowe wady: {stats['duplicates']} duplikatow, "
          f"{stats['late_corrections']} spoznionych korekt, "
          f"{stats['unknown_accounts']} nieznanych kont, {stats['null_amounts']} pustych kwot")
    print(f"    reorganizacja centrow kosztow od {reorg_from.isoformat()}")
    print(f"    project_code dochodzi od pliku nr {schema_change_at}")


if __name__ == "__main__":
    main()
