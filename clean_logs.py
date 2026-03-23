#!/usr/bin/env python3
"""
clean_logs.py
=============
Deletes log files under /logs whose filenames encode a creation timestamp.

Supported filename formats:
    <service>-YYMMDD-HHMMSS.log   e.g. payment-260318-135227.log
    YYMMDD-HHMMSS.log             e.g. 260318-135227.log

Usage:
    python clean_logs.py                      # delete all log files
    python clean_logs.py --minutes 5          # keep files created in last 5 min
    python clean_logs.py --hours 2            # keep files created in last 2 hours
    python clean_logs.py --days 1             # keep files created in last day
    python clean_logs.py --days 1 --hours 2 --minutes 30  # combined threshold
"""

import argparse
import os
import re
import sys
from datetime import datetime, timedelta

LOG_DIR = os.path.join(os.getcwd(), "logs")

# Matches the timestamp tail of the filename: YYMMDD-HHMMSS.log
# Works for both "service-YYMMDD-HHMMSS.log" and "YYMMDD-HHMMSS.log"
TIMESTAMP_RE = re.compile(r"(\d{6})-(\d{6})\.log$")


def parse_args():
    parser = argparse.ArgumentParser(
        description="Delete old log files under /logs based on filename timestamp."
    )
    parser.add_argument("--days",    type=float, default=0, help="Retention days")
    parser.add_argument("--hours",   type=float, default=0, help="Retention hours")
    parser.add_argument("--minutes", type=float, default=0, help="Retention minutes")
    parser.add_argument("--dry-run", action="store_true",
                        help="Print what would be deleted without deleting")
    return parser.parse_args()


def parse_timestamp(filename: str) -> datetime | None:
    """Extract creation datetime from the filename, scanning right-to-left."""
    m = TIMESTAMP_RE.search(filename)
    if not m:
        return None
    date_part, time_part = m.group(1), m.group(2)
    try:
        return datetime.strptime(date_part + time_part, "%y%m%d%H%M%S")
    except ValueError:
        return None


def main():
    args = parse_args()

    retention = timedelta(days=args.days, hours=args.hours, minutes=args.minutes)
    has_retention = retention.total_seconds() > 0
    cutoff = datetime.now() - retention if has_retention else None

    if not os.path.isdir(LOG_DIR):
        print(f"[ERROR] Log directory not found: {LOG_DIR}")
        sys.exit(1)

    deleted = 0
    skipped_recent = 0
    skipped_unmatched = 0

    # Walk every subdirectory under /logs
    for root, _, files in os.walk(LOG_DIR):
        # Sort files back-to-front (newest timestamp last alphabetically,
        # so we process oldest first and can stop early if desired)
        for filename in sorted(files, reverse=True):
            filepath = os.path.join(root, filename)

            ts = parse_timestamp(filename)
            if ts is None:
                print(f"  [SKIP ] {filepath}  (no timestamp in filename)")
                skipped_unmatched += 1
                continue

            if cutoff is not None and ts >= cutoff:
                print(f"  [KEEP ] {filepath}  (created {ts:%Y-%m-%d %H:%M:%S}, within retention window)")
                skipped_recent += 1
                continue

            if args.dry_run:
                print(f"  [DRY  ] would delete {filepath}  (created {ts:%Y-%m-%d %H:%M:%S})")
            else:
                try:
                    os.remove(filepath)
                    print(f"  [DEL  ] {filepath}  (created {ts:%Y-%m-%d %H:%M:%S})")
                    deleted += 1
                except OSError as e:
                    print(f"  [ERROR] {filepath}: {e}")

    print()
    print(f"  Done.")
    if has_retention:
        print(f"  Retention threshold : {retention}  (cutoff: {cutoff:%Y-%m-%d %H:%M:%S})")
    if args.dry_run:
        print(f"  Dry run — nothing deleted.")
    else:
        print(f"  Deleted             : {deleted}")
    print(f"  Kept (recent)       : {skipped_recent}")
    print(f"  Skipped (no match)  : {skipped_unmatched}")


if __name__ == "__main__":
    main()