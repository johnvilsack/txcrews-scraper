# /// script
# requires-python = ">=3.11"
# dependencies = [
#   "requests>=2.32.3",
#   "pandas>=2.2.2",
# ]
# ///
"""
TX CREWS Majors → MajorTrans harvester

What it does
1) GET https://api.txcrews.org/api/Majors (fresh list)
2) For each programId (optionally within --start-id/--end-id), GET https://api.txcrews.org/api/MajorTrans/{programId}
   - Saves each JSON to ./MajorTrans/{programId}.json (resume-safe; skip unless --force)
3) Normalize for a target year (default 2022):
   - Keep: programId, programLongName, instituteLegalName, degree level metrics
   - Include schoolProfileId
   - Add apiUrl + dashboardUrl per programId
   - Generate placeholders for institutes missing the target year
   - EXCEPT: drop (no placeholders) any rows/institutes with excluded schoolProfileId values
       Default excluded rollups: {134, 132, 141}
       Override with --exclude-school-ids; pass empty string "" to disable exclusion entirely

Examples
  uv run main.py
  uv run main.py --start-id 25 --end-id 50 --verbose
  uv run main.py --exclude-school-ids ""           # disable default exclusions
  uv run main.py --exclude-school-ids 101,202      # replace defaults with your list
"""

from __future__ import annotations

import argparse
import json
import logging
import time
from pathlib import Path
from typing import Any, Dict, List, Optional, Set

import pandas as pd
import requests


API_BASE = "https://api.txcrews.org/api"
MAJORS_URL = f"{API_BASE}/Majors"
MAJOR_TRANS_URL_TPL = f"{API_BASE}/MajorTrans/{{program_id}}"
DASHBOARD_URL_TPL = "https://txcrews.org/major-dashboard/{program_id}"

DEFAULT_YEAR = 2022
DEFAULT_EXCLUDE_IDS: Set[int] = {134, 142, 141}  # rollup IDs to drop completely
OUT_DIR = Path("MajorTrans")
OUT_DIR.mkdir(parents=True, exist_ok=True)


# ---------- helpers ----------

def setup_logging(verbose: bool) -> None:
    logging.basicConfig(
        level=(logging.DEBUG if verbose else logging.INFO),
        format="%(asctime)s %(levelname)s %(message)s",
    )


def parse_id_list(raw: Optional[str]) -> Optional[Set[int]]:
    """
    Parse comma/space-separated ints.
    Returns:
      - set[int] if user provided a list,
      - empty set if user passed empty string "" (explicitly disable exclusions),
      - None if user omitted the flag (use defaults).
    """
    if raw is None:
        return None  # use defaults
    if raw.strip() == "":
        return set()  # disable all exclusions
    parts = [p.strip() for p in raw.replace(",", " ").split()]
    out: Set[int] = set()
    for p in parts:
        if not p:
            continue
        try:
            out.add(int(p))
        except ValueError:
            raise ValueError(f"--exclude-school-ids contains a non-integer: {p!r}")
    return out


def get_json(session: requests.Session, url: str, *, retries: int = 3, backoff: float = 1.5, timeout: int = 30) -> Any:
    """
    GET JSON with retry/backoff.

    Errors in plain terms:
      - ConnectionError/Timeout: network/server slow or unreachable
      - HTTPError: non-2xx status (4xx = request issue; 5xx = server)
      - JSONDecodeError: response wasn’t valid JSON
    """
    last_exc: Optional[Exception] = None
    for attempt in range(1, retries + 1):
        try:
            resp = session.get(url, timeout=timeout)
            resp.raise_for_status()
            return resp.json()
        except (requests.ConnectionError, requests.Timeout) as e:
            last_exc = e
            logging.warning("Network issue on attempt %d/%d: %s", attempt, retries, e)
        except requests.HTTPError as e:
            status = e.response.status_code if e.response is not None else None
            if status == 429 or (isinstance(status, int) and 500 <= status < 600):
                last_exc = e
                logging.warning("HTTP %s on attempt %d/%d: %s", status, attempt, retries, e)
            else:
                raise
        except json.JSONDecodeError:
            raise
        if attempt < retries:
            time.sleep(backoff ** (attempt - 1))
    assert last_exc is not None
    raise last_exc


def load_or_fetch_majortrans(session: requests.Session, program_id: int, *, force: bool) -> Dict[str, Any]:
    """Load from disk unless --force; else fetch & save to ./MajorTrans/{id}.json"""
    path = OUT_DIR / f"{program_id}.json"
    if path.exists() and not force:
        with path.open("r", encoding="utf-8") as f:
            return json.load(f)
    url = MAJOR_TRANS_URL_TPL.format(program_id=program_id)
    data = get_json(session, url)
    with path.open("w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False)
    return data


def collect_institute_universe(dld: List[Dict[str, Any]]) -> List[str]:
    """All instituteLegalName values across all (filtered) rows."""
    names = {r.get("instituteLegalName") for r in dld if r.get("instituteLegalName")}
    return sorted(names)


def normalize_program(
    program: Dict[str, Any],
    majortrans: Dict[str, Any],
    year: int,
    exclude_school_ids: Set[int],
) -> List[Dict[str, Any]]:
    """
    One program → list of output rows.

    Rules:
      - Drop any rows whose schoolProfileId is in exclude_school_ids (no placeholders).
      - Build the institute universe from the already-filtered rows (so excluded institutes disappear entirely).
      - For remaining institutes, emit:
          * one row per degree level at target year, has_year_data=1
          * or a single placeholder row (has_year_data=0) when institute lacks target-year rows
    """
    program_id = int(program.get("programId"))
    program_long = program.get("programLongName")

    dld_all: List[Dict[str, Any]] = majortrans.get("degreeLevelData", []) or []

    # 1) Hard-drop excluded schoolProfileIds from ALL rows
    if exclude_school_ids:
        dld_all = [
            r for r in dld_all
            if (r.get("schoolProfileId") is None) or (int(r.get("schoolProfileId")) not in exclude_school_ids)
        ]

    # 2) Universe derived from filtered rows only
    institutes = collect_institute_universe(dld_all)

    # 3) Target-year rows (already exclude-filtered)
    rows_yr = [r for r in dld_all if r.get("year") == year]
    by_institute_yr: Dict[str, List[Dict[str, Any]]] = {}
    for r in rows_yr:
        name = r.get("instituteLegalName")
        if name:
            by_institute_yr.setdefault(name, []).append(r)

    metric_keys = [
        "schoolProfileId",
        "degreeLevelId",
        "degreeLevelName",
        "numberOfGraduates",
        "tuitionFee",
        "degreeTimeFinish",
        "loanPercent",
        "loanAmount",
        "wagesYear1",
        "wagesYear3",
        "wagesYear5",
        "wagesYear8",
        "wagesYear10",
        "loanPercentWagesYear1",
    ]

    api_url = MAJOR_TRANS_URL_TPL.format(program_id=program_id)
    dashboard_url = DASHBOARD_URL_TPL.format(program_id=program_id)

    out_rows: List[Dict[str, Any]] = []

    for inst in institutes:
        rows = by_institute_yr.get(inst, [])
        if rows:
            for r in rows:
                row = {
                    "programId": program_id,
                    "programLongName": program_long,
                    "year": year,
                    "instituteLegalName": inst,
                    "has_year_data": 1,
                    "apiUrl": api_url,
                    "dashboardUrl": dashboard_url,
                }
                for k in metric_keys:
                    row[k] = r.get(k)
                out_rows.append(row)
        else:
            # Placeholder only for *non-excluded* institutes
            row = {
                "programId": program_id,
                "programLongName": program_long,
                "year": year,
                "instituteLegalName": inst,
                "has_year_data": 0,
                "apiUrl": api_url,
                "dashboardUrl": dashboard_url,
                **{k: None for k in metric_keys},
            }
            out_rows.append(row)

    return out_rows


# ---------- main ----------

def main() -> None:
    ap = argparse.ArgumentParser(description="TX CREWS Majors → MajorTrans harvester")
    ap.add_argument("--start-id", type=int, default=None, help="Start programId (inclusive)")
    ap.add_argument("--end-id", type=int, default=None, help="End programId (inclusive)")
    ap.add_argument("--out", type=Path, default=Path("majortrans_2022.csv"), help="Output CSV path")
    ap.add_argument("--year", type=int, default=DEFAULT_YEAR, help="Target year (default: 2022)")
    ap.add_argument(
        "--exclude-school-ids",
        type=str,
        default=None,
        help="Comma/space-separated schoolProfileId values to omit. "
             "Omit flag: use defaults ({134,132,141}). Empty string: disable exclusions. "
             "Provide list: replace defaults.",
    )
    ap.add_argument("--force", action="store_true", help="Re-download MajorTrans even if file exists")
    ap.add_argument("--sleep", type=float, default=0.0, help="Sleep seconds between requests (politeness throttle)")
    ap.add_argument("--verbose", action="store_true", help="Verbose logging")
    args = ap.parse_args()

    setup_logging(args.verbose)

    # Determine exclusion set
    parsed = parse_id_list(args.exclude_school_ids)
    if parsed is None:
        exclude_school_ids = set(DEFAULT_EXCLUDE_IDS)  # use defaults
        logging.info("Excluding default schoolProfileId(s): %s", sorted(exclude_school_ids))
    else:
        exclude_school_ids = parsed  # user override (possibly empty)
        if exclude_school_ids:
            logging.info("Excluding schoolProfileId(s): %s", sorted(exclude_school_ids))
        else:
            logging.info("No schoolProfileId exclusions in effect.")

    with requests.Session() as s:
        s.headers.update({"Accept": "application/json"})
        logging.info("Fetching Majors… %s", MAJORS_URL)
        majors = get_json(s, MAJORS_URL)

        if not isinstance(majors, list):
            raise ValueError("Majors payload is not a list.")

        # Filter by range and sort by programId
        def in_range(pid: int) -> bool:
            if args.start_id is not None and pid < args.start_id:
                return False
            if args.end_id is not None and pid > args.end_id:
                return False
            return True

        majors_filtered = [
            m for m in majors
            if isinstance(m, dict) and "programId" in m and in_range(int(m["programId"]))
        ]
        majors_filtered.sort(key=lambda m: int(m["programId"]))
        logging.info("Total majors: %d; selected: %d", len(majors), len(majors_filtered))

        all_rows: List[Dict[str, Any]] = []

        for m in majors_filtered:
            pid = int(m["programId"])
            try:
                logging.debug("Processing programId=%s", pid)
                mt = load_or_fetch_majortrans(s, pid, force=args.force)
                rows = normalize_program(m, mt, args.year, exclude_school_ids)
                all_rows.extend(rows)
                if args.sleep > 0:
                    time.sleep(args.sleep)
            except Exception as e:
                logging.error("Failed programId=%s: %s", pid, e)

        if all_rows:
            df = pd.DataFrame(all_rows)
            # readable column order
            FIRST = [
                "programId", "programLongName", "year",
                "apiUrl", "dashboardUrl",
                "instituteLegalName", "has_year_data", "schoolProfileId",
            ]
            REST = [c for c in df.columns if c not in FIRST]
            df = df[FIRST + REST]
            args.out.parent.mkdir(parents=True, exist_ok=True)
            df.to_csv(args.out, index=False)
            logging.info("Wrote %d rows to %s", len(df), args.out.resolve())
        else:
            logging.warning("No rows produced. Check filters/year and API responses.")


if __name__ == "__main__":
    main()
