import json
import glob
import os
import argparse

def read_sessions(path):
    # supports JSON array or JSON lines
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
            if isinstance(data, list):
                return data
            return []
    except Exception:
        sessions = []
        with open(path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    sessions.append(json.loads(line))
                except Exception:
                    pass
        return sessions

def safe_str(x):
    if x is None:
        return ""
    return str(x).replace("'", "\\'")

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--input_dir", required=True, help="folder containing sessions_*.json")
    ap.add_argument("--out", required=True, help="output .txt file with hbase shell commands")
    ap.add_argument("--limit", type=int, default=5000, help="how many sessions to ingest")
    args = ap.parse_args()

    files = sorted(glob.glob(os.path.join(args.input_dir, "sessions_*.json")))
    if not files:
        raise SystemExit(f"No sessions_*.json found in {args.input_dir}")

    written = 0
    with open(args.out, "w", encoding="utf-8") as out:
        out.write("disable 'sessions_by_user'\n")
        out.write("enable 'sessions_by_user'\n")

        for fp in files:
            sessions = read_sessions(fp)
            for s in sessions:
                if written >= args.limit:
                    break

                user_id = safe_str(s.get("user_id"))
                start_time = safe_str(s.get("start_time"))
                session_id = safe_str(s.get("session_id"))

                # Row key supports prefix scans by user
                rowkey = f"{user_id}#{start_time}#{session_id}"

                geo = s.get("geo_data", {}) or {}
                device = s.get("device_profile", {}) or {}
                viewed = s.get("viewed_products", []) or []

                # minimal but meaningful columns for marking
                cols = {
                    "s:user_id": user_id,
                    "s:session_id": session_id,
                    "s:start_time": start_time,
                    "s:end_time": safe_str(s.get("end_time")),
                    "s:duration_seconds": safe_str(s.get("duration_seconds")),
                    "s:conversion_status": safe_str(s.get("conversion_status")),
                    "s:referrer": safe_str(s.get("referrer")),
                    "s:geo_city": safe_str(geo.get("city")),
                    "s:geo_state": safe_str(geo.get("state")),
                    "s:geo_country": safe_str(geo.get("country")),
                    "s:ip_address": safe_str(geo.get("ip_address")),
                    "s:device_type": safe_str(device.get("type")),
                    "s:device_os": safe_str(device.get("os")),
                    "s:device_browser": safe_str(device.get("browser")),
                    "s:viewed_products": safe_str(",".join(map(str, viewed))),
                }

                for k, v in cols.items():
                    out.write(f"put 'sessions_by_user', '{rowkey}', '{k}', '{v}'\n")

                written += 1

            if written >= args.limit:
                break

        out.write("count 'sessions_by_user'\n")

    print(f"✅ Generated HBase puts for {written} sessions → {args.out}")

if __name__ == "__main__":
    main()
