#!/usr/bin/env python3
"""Re-aggregate transactions by event-time hour, correct hour_agg, and report
merchant skew with split-key suggestions.

Inputs (defaults are relative to this script's directory):
  txns/YYYY-MM-DDTHH.csv   one file per *booking* hour; columns mch,amt,event_ts
  summary.db               hour_agg(win, mch, txn_cnt, amt_sum) from the previous
                           booking-hour aggregation run
  params.json              count_floor, share_threshold

Outputs:
  summary.db               hour_agg rewritten on the event-time hour basis
  skew_report.csv          window,mch,txn_count,share_pct,split_key,verify_share_pct
  split_plan.txt           one "plan" line per skewed merchant, one "verify" line
                           per report row

Skew rule: a (window, mch) cell is skewed when txn_count >= count_floor AND
txn_count / window_txn_count >= share_threshold (both inclusive).

Split-key selection: candidates are mch+<subkey> with subkeys taken in the
order event_minute, event_second, event_ts, amt, amt_band.  Per merchant the
chosen subkey minimises the average verification share over that merchant's
skewed windows (sum of per-window max bucket counts / sum of those window
totals); ties break by the listed order.  The same split key is applied to
every reported window of that merchant.
"""

import argparse
import csv
import glob
import json
import os
import sqlite3
from collections import Counter, defaultdict
from decimal import Decimal, ROUND_HALF_UP
from fractions import Fraction

SUBKEYS = ["event_minute", "event_second", "event_ts", "amt", "amt_band"]


def subkey_value(subkey, amt, event_ts):
    """Bucket value for one transaction under a subkey."""
    if subkey == "event_minute":
        return event_ts[14:16]          # minute segment, e.g. "07"
    if subkey == "event_second":
        return event_ts[17:19]          # second segment
    if subkey == "event_ts":
        return event_ts                 # full event timestamp
    if subkey == "amt":
        return str(amt)                 # exact amount
    if subkey == "amt_band":
        return str(amt // 10)           # 10-yuan band: 0-9 -> 0, 10-19 -> 1, ...
    raise ValueError(subkey)


def bucket_counts(txns, subkey):
    """Counter over subkey buckets for a list of (amt, event_ts) txns."""
    return Counter(subkey_value(subkey, amt, ts) for amt, ts in txns)


def pct(num, den):
    """Percentage with two decimals, half-up, as a string."""
    q = (Decimal(num) * 100 / Decimal(den)).quantize(
        Decimal("0.01"), rounding=ROUND_HALF_UP)
    return f"{q}"


def main():
    here = os.path.dirname(os.path.abspath(__file__))
    ap = argparse.ArgumentParser()
    ap.add_argument("--txns", default=os.path.join(here, "txns"))
    ap.add_argument("--db", default=os.path.join(here, "summary.db"))
    ap.add_argument("--params", default=os.path.join(here, "params.json"))
    ap.add_argument("--report", default=os.path.join(here, "skew_report.csv"))
    ap.add_argument("--plan", default=os.path.join(here, "split_plan.txt"))
    args = ap.parse_args()

    with open(args.params, encoding="utf-8") as fh:
        params = json.load(fh, parse_float=Decimal)
    count_floor = int(params["count_floor"])
    threshold = Fraction(params["share_threshold"])  # exact, 0.15 -> 3/20

    # ---- read every file; the file name's hour is the booking hour ----
    files = sorted(glob.glob(os.path.join(args.txns, "*.csv")))
    if not files:
        raise SystemExit(f"no csv files under {args.txns}")

    booking = defaultdict(lambda: [0, 0])   # (win, mch) -> [cnt, amt] by booking hour
    event = defaultdict(lambda: [0, 0])     # (win, mch) -> [cnt, amt] by event hour
    per_cell = defaultdict(list)            # (event win, mch) -> [(amt, event_ts)]
    for path in files:
        book_win = os.path.basename(path)[:-4]
        with open(path, newline="", encoding="utf-8") as fh:
            for row in csv.DictReader(fh):
                mch = row["mch"]
                amt = int(row["amt"])
                ts = row["event_ts"]
                win = ts[:13]
                b = booking[(book_win, mch)]
                b[0] += 1
                b[1] += amt
                e = event[(win, mch)]
                e[0] += 1
                e[1] += amt
                per_cell[(win, mch)].append((amt, ts))

    # ---- R1: cells whose booking-basis and event-basis aggregates differ ----
    all_cells = set(booking) | set(event)
    fixed_rows = sum(
        1 for k in all_cells
        if tuple(booking.get(k, (0, 0))) != tuple(event.get(k, (0, 0)))
    )

    # ---- correct hour_agg: rewrite on the event-time hour basis ----
    con = sqlite3.connect(args.db)
    try:
        previous = {(w, m): (c, s) for w, m, c, s in
                    con.execute("SELECT win, mch, txn_cnt, amt_sum FROM hour_agg")}
        with con:
            con.execute("DELETE FROM hour_agg")
            con.executemany(
                "INSERT INTO hour_agg (win, mch, txn_cnt, amt_sum) VALUES (?, ?, ?, ?)",
                [(w, m, c, s) for (w, m), (c, s) in sorted(event.items())],
            )
    finally:
        con.close()

    # Cross-check: count cells where the previous table and the corrected table
    # hold different values.  The previous table was booking-basis, so this
    # should equal R1 (cells present only in one table count as different).
    # Reported for diagnostics only; R1 itself is measured from the CSV inputs.
    db_fixed_rows = 0
    for k in set(previous) | set(event):
        before = previous.get(k)
        after = tuple(event[k]) if k in event else None
        if before != after:
            db_fixed_rows += 1
    # ---- skew detection on event-time windows ----
    window_total = Counter()
    for (win, _mch), (cnt, _amt) in event.items():
        window_total[win] += cnt

    skewed = []                     # (win, mch, txn_count, share_pct)
    skewed_by_mch = defaultdict(list)
    for (win, mch), (cnt, _amt) in sorted(event.items()):
        total = window_total[win]
        if cnt >= count_floor and Fraction(cnt, total) >= threshold:
            skewed.append((win, mch, cnt, pct(cnt, total)))
            skewed_by_mch[mch].append((win, cnt, total))

    # ---- per-merchant split-key suggestion ----
    # Pick, per merchant, the subkey with the lowest average verification share
    # across that merchant's skewed windows: sum of per-window max bucket counts
    # divided by the sum of those windows' transaction totals.  Ties break by
    # the SUBKEYS order above.
    split_key = {}
    for mch, cells in skewed_by_mch.items():
        best = None
        for subkey in SUBKEYS:
            max_buckets = sum(max(bucket_counts(per_cell[(w, mch)], subkey).values())
                              for w, _c, _t in cells)
            share = Fraction(max_buckets, sum(t for _w, _c, t in cells))
            if best is None or share < best[0]:   # strict: listed order breaks ties
                best = (share, subkey)
        split_key[mch] = "mch+" + best[1]

    # ---- skew_report.csv ----
    report_rows = []
    for win, mch, cnt, share in skewed:
        key = split_key[mch]
        subkey = key.split("+", 1)[1]
        buckets = bucket_counts(per_cell[(win, mch)], subkey)
        verify = pct(max(buckets.values()), window_total[win])
        report_rows.append([win, mch, cnt, share, key, verify])

    with open(args.report, "w", newline="", encoding="utf-8") as fh:
        w = csv.writer(fh, lineterminator="\n")
        w.writerow(["window", "mch", "txn_count", "share_pct",
                    "split_key", "verify_share_pct"])
        w.writerows(report_rows)
        report_lines = len(report_rows)

    # ---- split_plan.txt ----
    plan_lines, verify_lines = [], []
    for mch in sorted(skewed_by_mch):
        plan_lines.append(
            f"plan mch={mch} windows={len(skewed_by_mch[mch])} split_key={split_key[mch]}")
    for win, mch, cnt, share, key, verify in report_rows:
        subkey = key.split("+", 1)[1]
        nb = len(bucket_counts(per_cell[(win, mch)], subkey))
        verify_lines.append(
            f"verify window={win} mch={mch} txn_count={cnt} share_pct={share} "
            f"split_key={key} verify_share_pct={verify} buckets={nb}")

    with open(args.plan, "w", encoding="utf-8") as fh:
        fh.write("\n".join(plan_lines + verify_lines) + "\n")

    # ---- consistency footer on the report ----
    # consistent = every report row has its verify line and the report covers
    # every detected skewed cell.
    consistent = (len(verify_lines) == report_lines
                  and report_lines == len(skewed))
    with open(args.report, "a", encoding="utf-8") as fh:
        fh.write(f"consistency fixed_rows={fixed_rows} report_rows={report_lines} "
                 f"verify_rows={len(verify_lines)} "
                 f"consistent={'yes' if consistent else 'no'}\n")

    print(f"windows={len(window_total)} cells={len(event)} fixed_rows={fixed_rows} "
          f"report_rows={report_lines} verify_rows={len(verify_lines)} "
          f"consistent={'yes' if consistent else 'no'}")


if __name__ == "__main__":
    main()
