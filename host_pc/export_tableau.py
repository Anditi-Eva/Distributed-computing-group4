"""Milestone 7 - convert YOUR experiment results into flat CSVs for Tableau.

    python ft_experiments.py --only all --trials 5      # produces results/milestone7/*.json on your machine
    python export_tableau.py                            # writes results/milestone7/tableau/*.csv

Only files that exist are exported, so you can run a single experiment (e.g. --only matrix) first.
Also reads incident logs written by ft_master.py when you test on the real VMs:
    python export_tableau.py --incidents ft_incidents.json
"""
import argparse
import csv
import json
import os

HERE = os.path.dirname(os.path.abspath(__file__))
SRC = os.path.join(HERE, "results", "milestone7")


def write(out, name, rows):
    if not rows:
        return
    cols = []
    for r in rows:
        for k, v in r.items():
            if k not in cols and not isinstance(v, (list, dict)):
                cols.append(k)
    with open(os.path.join(out, f"{name}.csv"), "w", newline="") as fh:
        wr = csv.DictWriter(fh, fieldnames=cols, extrasaction="ignore")
        wr.writeheader()
        wr.writerows(rows)
    print(f"  {name}.csv  ({len(rows)} rows)")


def load(name):
    path = os.path.join(SRC, name)
    return json.load(open(path)) if os.path.exists(path) else None


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--incidents", help="incident log from ft_master.py (real-VM runs)")
    args = p.parse_args()
    out = os.path.join(SRC, "tableau")
    os.makedirs(out, exist_ok=True)

    m = load("matrix.json")
    if m:
        for r in m:
            r.pop("timeline", None)
        write(out, "failure_matrix", m)
        long = []
        for r in m:
            for cfg, a, o in (("No FT", "baseline_availability", "baseline_longest_outage_s"),
                              ("Failover only", "failover_availability", "failover_longest_outage_s"),
                              ("Full FT", "ft_availability", "ft_longest_outage_s")):
                long.append({"scenario": r["scenario"], "taxonomy": r["taxonomy"], "trial": r["trial"],
                             "configuration": cfg, "availability_pct": 100 * r[a], "outage_s": r[o]})
        write(out, "availability_long", long)
    d = load("design.json")
    for kind, rows in (d or {}).items():
        write(out, f"design_{kind}", rows)
    write(out, "byzantine", load("byzantine.json"))
    s = load("soak.json")
    if s:
        write(out, "soak_faults", [{"fault_no": i + 1, "time_s": f["t"], "scenario": f["scenario"]} for i, f in enumerate(s["faults"])])
        write(out, "soak_incidents", s["incidents"])
        write(out, "soak_summary", [{"configuration": c, "availability_pct": 100 * s[a], "downtime_s": s[dt],
                                     "outages": s[o], "mttr_s": s[mt] or 0}
                                    for c, a, dt, o, mt in (
            ("No FT", "baseline_availability", "baseline_downtime_s", "baseline_outages", "baseline_mttr_s"),
            ("Failover only", "failover_availability", "failover_downtime_s", "failover_outages", "failover_mttr_s"),
            ("Full FT", "ft_availability", "ft_downtime_s", "ft_outages", "ft_mttr_s"))])
    if args.incidents:
        inc = json.load(open(args.incidents))
        rows = []
        for i in inc["incidents"]:
            t0 = i.get("t_first_fail") or i["t_detected"]
            rows.append({"incident": i["id"], "node": i["node"], "cause": i["cause"],
                         "detect_s": i["t_detected"] - t0,
                         "failover_s": (i["t_failover_done"] - t0) if i.get("t_failover_done") else None,
                         "recovered_s": (i["t_recovered"] - t0) if i.get("t_recovered") else None})
        write(out, "vm_incidents", rows)
    print(f"done -> {out}")


if __name__ == "__main__":
    main()