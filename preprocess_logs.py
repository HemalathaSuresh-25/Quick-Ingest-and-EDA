import os
import re
import pandas as pd
from datetime import datetime

INPUT_DIR = "C:/Users/hemalatha/Desktop/attest-eda/data/standardized"
OUTPUT_CSV = "C:/Users/hemalatha/Desktop/attest-eda/data/logs_preprocessed.csv"

# Predefined suite list
SUITES = [
    "ptp-oc","ptp-tc","dtmf","tcp-xp-tec","ipv6-host","v6bgp4",
    "rtpp","sct","lacp-tec","ipv4","ptp-bc","udp-tec","sip"
]

# ---------------- Header Extraction ---------------- #
def extract_header_info(file_path):
    dut_name, dut_version, os_version, config, test_case = None, None, None, None, None
    with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
        for line in f:
            line = line.strip()
            m = re.search(r"DUT\s*NAME\s*[:=]\s*(.*)", line, re.IGNORECASE)
            if m: dut_name = m.group(1).strip()
            m = re.search(r"DUT\s*VERSION\s*[:=]\s*(.*)", line, re.IGNORECASE)
            if m: dut_version = m.group(1).strip()
            m = re.search(r"OS\s*VERSION\s*[:=]\s*(.*)", line, re.IGNORECASE)
            if m: os_version = m.group(1).strip()
            m = re.search(r"CONFIGURATION\s*[:=]\s*(.*)", line, re.IGNORECASE)
            if m: config = m.group(1).strip()
            m = re.search(r"Test\s*Case\s*[:=]\s*([A-Za-z0-9_\-\.]+)", line, re.IGNORECASE)
            if m: test_case = m.group(1).strip()
            if all([dut_name, config, test_case]): break
    return dut_name, dut_version, os_version, config, test_case

# ---------------- Log Line Feature Extraction ---------------- #
def extract_log_line_features(line, line_number, file_date=None, filename=None):
    line = line.strip()
    timestamp, status, error_msg, run_date, suite = None, None, None, None, None

    # Extract time-only timestamp
    ts_match = re.match(r'(\d{2}:\d{2}:\d{2}\.\d+)', line)
    if ts_match and file_date:
        time_str = ts_match.group(1)
        try:
            timestamp = datetime.strptime(f"{file_date} {time_str}", "%Y-%m-%d %H:%M:%S.%f")
            run_date = timestamp.date()
        except:
            timestamp = None
            run_date = file_date

    # Extract status and error_msg from # Result: lines
    result_match = re.search(r'# Result:\s*(FAILED|PASSED|ABORTED)\s*(.*)', line, re.IGNORECASE)
    if result_match:
        s = result_match.group(1).upper()
        if s == "FAILED":
            status = "FAIL"
        elif s == "PASSED":
            status = "PASS"
        elif s == "ABORTED":
            status = "ABORT"
        error_msg = result_match.group(2).strip()

    # Suite extraction from filename if not found in line
    if filename:
        for su in SUITES:
            if su.lower() in filename.lower():
                suite = su
                break

    # OS version set to Linux
    os_version = "Linux"

    return timestamp, status, error_msg, run_date, suite, line, os_version

# ---------------- Main Log Processing ---------------- #
def process_logs(input_dir):
    all_data = []
    for root, _, files in os.walk(input_dir):
        for file in files:
            if not file.endswith(".log"):
                continue
            filepath = os.path.join(root, file)
            # Infer run_date from filename if possible
            date_match = re.search(r'(\d{8})', file)
            file_date = datetime.strptime(date_match.group(1), "%Y%m%d").date() if date_match else None

            dut_name, dut_version, _, config, test_case = extract_header_info(filepath)
            if not dut_name: dut_name = "Unknown_DUT"
            if not dut_version:
                m = re.search(r'_(\w+)_\d{8}-', file)
                dut_version = m.group(1) if m else "Unknown_Version"
            if not config: config = "Default_Config"

            with open(filepath, 'r', encoding='utf-8', errors='ignore') as f:
                for idx, line in enumerate(f, start=1):
                    timestamp, status, error_msg, run_date, suite, raw_line, os_version = extract_log_line_features(
                        line, idx, file_date, file
                    )
                    all_data.append({
                        "filename": file,
                        "dut": dut_name,
                        "dut_version": dut_version,
                        "os_version": os_version,
                        "config": config,
                        "test_case_id": test_case,
                        "line_number": idx,
                        "timestamp": timestamp,
                        "run_date": run_date,
                        "status": status,
                        "error_msg": error_msg,
                        "raw_line": raw_line,
                        "suite": suite
                    })
    return pd.DataFrame(all_data)

# ---------------- Run Script ---------------- #
df = process_logs(INPUT_DIR)
print("Total rows extracted:", len(df))
print(df.head(20))

df.to_csv(OUTPUT_CSV, index=False)
print(f"\nPreprocessed log data saved → {OUTPUT_CSV}")
