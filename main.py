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
   - Saves each JSON to ./MajorTrans/{programId}.json (resume-safe; skip if exists unless --force)
3) Normalizes to CSV:
   - From Majors: ProgramId, ProgramLongName
   - From MajorTrans: one row per instituteLegalName in YEAR=2022 (all degreeLevelIds)
   - If an institute has no 2022 record, emits a row with has_2022_data=0 and blank metrics
4) Writes CSV (default: majortrans_2022.csv)

Run
  uv run main.py --start-id 25 --end-id 50 --out majortrans_2022.csv --year 2022 --verbose
"""

from __future__ import annotations

import argparse
import json
import logging
import time
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

import pandas as pd
import requests


API_BASE = "https://api.txcrews.org/api"
MAJORS_URL = f"{API_BASE}/Majors"
MAJOR_TRANS_URL = f"{API_BASE}/MajorTrans/{{program_id}}"

DEFAULT_YEAR = 2022
OUT_DIR = Path("MajorTrans")
OUT_DIR.mkdir(parents=True, exist_ok=True)


def setup_logging(verbose: bool) -> None:
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s %(levelname)s %(message)s",
    )


def get_json(session: requests.Session, url: str, *, retries: int = 3, backoff: float = 1.5, timeout: int = 30) -> Any:
    """
    GET JSON with simple retry/backoff. Raises on failure.

    Errors explained simply:
      - ConnectionError/Timeout: network or server slow/unreachable
      - HTTPError: non-2xx status; check URL or server status
      - JSONDecodeError: server didn't return valid JSON
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
            # Don't retry 4xx except 429; do retry 5xx
            status = e.response.status_code if e.response is not None else "?"
            if status == 429 or (500 <= int(status) < 600):
                last_exc = e
                logging.warning("HTTP %s on attempt %d/%d: %s", status, attempt, retries, e)
            else:
                raise
        except json.JSONDecodeError as e:
            # Usually not transient; raise
            raise
        if attempt < retries:
            sleep_s = backoff ** (attempt - 1)
            time.sleep(sleep_s)
    assert last_exc is not None
    raise last_exc


def load_or_fetch_majortrans(
    session: requests.Session,
    program_id: int,
    force: bool = False,
) -> Dict[str, Any]:
    """
    Load from disk if present (unless force), else fetch and save.
    """
    path = OUT_DIR / f"{program_id}.json"
    if path.exists() and not force:
        with path.open("r", encoding="utf-8") as f:
            return json.load(f)
    url = MAJOR_TRANS_URL.format(program_id=program_id)
    data = get_json(session, url)
    with path.open("w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False)
    return data


def collect_institute_universe(degree_level_data: List[Dict[str, Any]]) -> List[str]:
    """
    Build the set of all instituteLegalName values seen at any year for a program.
    """
    names = {row.get("instituteLegalName") for row in degree_level_data if row.get("instituteLegalName")}
    return sorted(names)


def normalize_program(
    program: Dict[str, Any],
    majortrans: Dict[str, Any],
    year: int,
) -> List[Dict[str, Any]]:
    """
    Produce normalized rows for one program:
      - one row per institute in the universe
      - if a 2022 record exists for that institute, fill fields and has_2022_data=1
      - else emit row with has_2022_data=0 and metrics blank
    """
    program_id = program.get("programId")
    program_long = program.get("programLongName")

    dld: List[Dict[str, Any]] = majortrans.get("degreeLevelData", []) or []

    # Universe of institutes across all years in this program
    institutes = collect_institute_universe(dld)

    # Index 2022 rows by institute (there can be multiple degreeLevelIds; we keep all)
    rows_2022 = [r for r in dld if r.get("year") == year]
    by_institute_2022: Dict[str, List[Dict[str, Any]]] = {}
    for r in rows_2022:
        name = r.get("instituteLegalName")
        if not name:
            continue
        by_institute_2022.setdefault(name, []).append(r)

    # Common keys we expect (present or absent)
    metric_keys = [
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

    out_rows: List[Dict[str, Any]] = []

    # Ensure we also cover institutes that *only* exist at year=2022 (edge-case)
    institutes_set = set(institutes) | set(by_institute_2022.keys())
    for inst in sorted(institutes_set):
        rows = by_institute_2022.get(inst, [])
        if rows:
            # One output row *per degree level* at the target year
            for r in rows:
                row: Dict[str, Any] = {
                    "programId": program_id,
                    "programLongName": program_long,
                    "year": year,
                    "instituteLegalName": inst,
                    "has_2022_data": 1,
                }
                for k in metric_keys:
                    row[k] = r.get(k)
                out_rows.append(row)
        else:
            # No 2022 data for this institute
            row = {
                "programId": program_id,
                "programLongName": program_long,
                "year": year,
                "instituteLegalName": inst,
                "has_2022_data": 0,
                **{k: None for k in metric_keys},
            }
            out_rows.append(row)

    return out_rows


def main() -> None:
    ap = argparse.ArgumentParser(description="TX CREWS Majors → MajorTrans harvester")
    ap.add_argument("--start-id", type=int, default=None, help="Start programId (inclusive)")
    ap.add_argument("--end-id", type=int, default=None, help="End programId (inclusive)")
    ap.add_argument("--out", type=Path, default=Path("majortrans_2022.csv"), help="Output CSV path")
    ap.add_argument("--year", type=int, default=DEFAULT_YEAR, help="Target year to extract (default: 2022)")
    ap.add_argument("--force", action="store_true", help="Re-download MajorTrans even if file exists")
    ap.add_argument("--sleep", type=float, default=0.0, help="Sleep seconds between requests (optional)")
    ap.add_argument("--verbose", action="store_true", help="Verbose logging")
    args = ap.parse_args()

    setup_logging(args.verbose)

    with requests.Session() as s:
        s.headers.update({"Accept": "application/json"})
        logging.info("Fetching Majors… %s", MAJORS_URL)
        majors = get_json(s, MAJORS_URL)

        # Minimal shape validation
        if not isinstance(majors, list):
            raise ValueError("Majors payload is not a list. Unexpected response shape.")

        # Filter + order by programId
        def in_range(pid: int) -> bool:
            if args.start_id is not None and pid < args.start_id:
                return False
            if args.end_id is not None and pid > args.end_id:
                return False
            return True

        majors_filtered = [m for m in majors if isinstance(m, dict) and "programId" in m and in_range(int(m["programId"]))]
        majors_filtered.sort(key=lambda m: int(m["programId"]))

        logging.info("Total majors: %d; selected: %d", len(majors), len(majors_filtered))

        all_rows: List[Dict[str, Any]] = []

        for m in majors_filtered:
            pid = int(m["programId"])
            try:
                logging.debug("Processing programId=%s", pid)
                mt = load_or_fetch_majortrans(s, pid, force=args.force)
                rows = normalize_program(m, mt, args.year)
                all_rows.extend(rows)
                if args.sleep > 0:
                    time.sleep(args.sleep)
            except Exception as e:
                logging.error("Failed programId=%s: %s", pid, e)

        # Create DataFrame and write CSV
        if all_rows:
            df = pd.DataFrame(all_rows)
            # Order columns for readability
            FIRST = ["programId", "programLongName", "year", "instituteLegalName", "has_2022_data"]
            REST = [c for c in df.columns if c not in FIRST]
            df = df[FIRST + REST]
            args.out.parent.mkdir(parents=True, exist_ok=True)
            df.to_csv(args.out, index=False)
            logging.info("Wrote %d rows to %s", len(df), args.out.resolve())
        else:
            logging.warning("No rows produced. Check filters/year and API responses.")


if __name__ == "__main__":
    main()
