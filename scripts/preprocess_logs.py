import os
import re
import pandas as pd
from datetime import datetime

#Paths
INPUT_DIR = "C:/Users/hemalatha/Desktop/attest-eda/data/standardized"
OUTPUT_CSV = "C:/Users/hemalatha/Desktop/attest-eda/data/logs_preprocessed.csv"

#Suite List
SUITES = [
    "ptp-oc", "ptp-tc", "dtmf", "tcp-xp-tec", "ipv6-host", "v6bgp4",
    "rtp", "sctp", "lacp-tec", "ipv4", "ptp-bc", "udp-tec", "sip", "udp", "tcp", "lacp"
]

#DUT Header Extraction
def extract_header_info(file_path):
    """Extract DUT, version, OS, configuration, and test case info from header lines."""
    dut_name, dut_version, os_version, config, test_case = None, None, None, None, None
    with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
        for line in f:
            line = line.strip()
            patterns = {
                "dut_name": r"DUT\s*NAME\s*[:=]\s*(.+)",
                "dut_version": r"DUT\s*VERSION\s*[:=]\s*(.+)",
                "os_version": r"OS\s*VERSION\s*[:=]\s*(.+)",
                "config": r"CONFIGURATION\s*[:=]\s*(.+)",
                "test_case": r"Test\s*Case\s*[:=]\s*([A-Za-z0-9_\-\.]+)"
            }
            for key, pat in patterns.items():
                m = re.search(pat, line, re.IGNORECASE)
                if m:
                    val = m.group(1).strip()
                    if key == "dut_name": dut_name = val
                    elif key == "dut_version": dut_version = val
                    elif key == "os_version": os_version = val
                    elif key == "config": config = val
                    elif key == "test_case": test_case = val
            if all([dut_name, dut_version, os_version, config, test_case]):
                break
    return dut_name, dut_version, os_version, config, test_case


#Log Line Extraction
def extract_log_line_features(line, file_date=None, filename=None):
    """Extract timestamp, status (PASS/FAIL/ABORT), and reason message from log lines."""
    line = line.strip()
    timestamp, status, error_msg, run_date, suite = None, None, None, None, None

    # Timestamp extraction
    ts_match = re.match(r"(\d{2}:\d{2}:\d{2}\.\d+)", line)
    if ts_match and file_date:
        time_str = ts_match.group(1)
        try:
            timestamp = datetime.strptime(f"{file_date} {time_str}", "%Y-%m-%d %H:%M:%S.%f")
            run_date = timestamp.date()
        except Exception:
            run_date = file_date

    # Status & error message extraction
    patterns = [
        r"#\s*Result\s*[:\-]?\s*(PASSED|FAILED|ABORTED|PASS|FAIL|ABORT)\b\s*[:\-]?\s*(.*)",
        r"#\s*TEST\s*CASE\s*(PASSED|FAILED|ABORTED|PASS|FAIL|ABORT)\b\s*[:\-]?\s*(.*)",
        r"\b(TEST\s*CASE\s*)?(PASSED|FAILED|ABORTED|PASS|FAIL|ABORT)\b\s*[:\-]?\s*(.*)",
    ]
    for pat in patterns:
        m = re.search(pat, line, re.IGNORECASE)
        if m:
            groups = [g for g in m.groups() if g]
            for g in groups:
                if g.upper() in ["PASSED", "FAILED", "ABORTED", "PASS", "FAIL", "ABORT"]:
                    status = g.upper()
                    if status == "PASSED": status = "PASS"
                    elif status == "FAILED": status = "FAIL"
                    elif status == "ABORTED": status = "ABORT"
            if len(groups) > 1:
                error_msg = groups[-1].strip()
            break

    # Suite inference
    if filename:
        for su in SUITES:
            if su.lower() in filename.lower():
                suite = su
                break

    os_version = "Linux"
    return timestamp, status, error_msg, run_date, suite, os_version


#DUT Version Fallback
def infer_dut_version(file, lines):
    m = re.search(r'_(AS|TEC|IP|BC|OC|V\d+)[-_]?\d{8}', file, re.IGNORECASE)
    if m:
        return m.group(1).upper()
    for line in lines:
        m = re.search(r"version\s*[:=]\s*([A-Za-z0-9\.\-_]+)", line, re.IGNORECASE)
        if m:
            return m.group(1).strip()
    return "Generic_v1.0"


#Smart Missing Value Handling
def fix_missing_values(df):
    """Context-aware repair for missing values, with improved FAIL/ABORT error lookup."""

    #Timestamp and run_date repair 
    df["timestamp"] = df.groupby("filename")["timestamp"].ffill().bfill()

    def infer_date(fname):
        m = re.search(r"(\d{8})", fname)
        return datetime.strptime(m.group(1), "%Y%m%d").date() if m else None

    df["run_date"] = df.groupby("filename")["run_date"].ffill().bfill()
    df["run_date"] = df.apply(
        lambda x: x["run_date"] if pd.notna(x["run_date"]) else infer_date(x["filename"]),
        axis=1
    )

    # Suite inference 
    def infer_suite(row):
        if pd.notna(row["suite"]):
            return row["suite"]
        for su in SUITES:
            if su.lower() in row["filename"].lower():
                return su
        return None

    df["suite"] = df.apply(infer_suite, axis=1)
    df["suite"] = df.groupby(["dut", "test_case_id"])["suite"].ffill().bfill()

    #PASS rows → always "No Error"
    df.loc[df["status"] == "PASS", "error_msg"] = "No Error"

    #FAIL/ABORT rows should NEVER be "No Error" 
    df.loc[df["status"].isin(["FAIL", "ABORT"]) & (df["error_msg"].str.lower() == "no error"), "error_msg"] = None

    #Context-based lookup (±10 lines) for FAIL/ABORT ---
    df.reset_index(drop=True, inplace=True)
    for i, row in df.iterrows():
        if row["status"] in ["FAIL", "ABORT"] and (pd.isna(row["error_msg"]) or row["error_msg"] == ""):
            start = max(0, i - 10)
            end = min(len(df) - 1, i + 10)
            window_lines = df.loc[start:end, "raw_line"].tolist()
            msg = next(
                (
                    ln for ln in window_lines
                    if re.search(r"(error|fail|reason|exception|invalid|timeout|abort|crash|assert|not\s+transmit)", ln, re.IGNORECASE)
                ),
                None
            )
            if msg:
                df.at[i, "error_msg"] = msg.strip()
            else:
                df.at[i, "error_msg"] = "Failure reason not found"

    #Final clean-up 
    df["error_msg"].fillna("Failure reason not found", inplace=True)
    df = df[~df["run_date"].isna()].reset_index(drop=True)
    return df


#Main Processing
def process_logs(input_dir):
    all_data = []
    for root, _, files in os.walk(input_dir):
        for file in files:
            if not file.endswith(".log"):
                continue

            filepath = os.path.join(root, file)
            date_match = re.search(r'(\d{8})', file)
            file_date = datetime.strptime(date_match.group(1), "%Y%m%d").date() if date_match else None

            with open(filepath, 'r', encoding='utf-8', errors='ignore') as f:
                lines = f.readlines()

            # Header extraction
            dut_name, dut_version, os_version, config, test_case = extract_header_info(filepath)

            # Fallbacks
            if not dut_name:
                m = re.search(r'(DUT[^\W_]+)', file, re.IGNORECASE)
                dut_name = m.group(1) if m else "DUT_Auto"
            if not dut_version or dut_version.lower().startswith("unknown"):
                dut_version = infer_dut_version(file, lines)
            if not os_version:
                os_version = "Linux"
            if not config:
                config = "Standard_Config"
            if not test_case:
                m = re.search(r'(tc_[a-zA-Z0-9_\-\.]+)', file)
                test_case = m.group(1) if m else "default_tc"

            # Parse each log line
            for idx, line in enumerate(lines, start=1):
                timestamp, status, error_msg, run_date, suite, os_version = extract_log_line_features(
                    line, file_date, file
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
                    "suite": suite,
                    "raw_line": line.strip(),
                })

    # Build DataFrame
    df = pd.DataFrame(all_data)
    df = df[(df["status"].notna()) | (df["error_msg"].notna())]

    # Fix missing values
    df = fix_missing_values(df)
    return df


#Run Script 
if __name__ == "__main__":
    df = process_logs(INPUT_DIR)
    print("Total rows extracted:", len(df))

    print("\nStatus Summary:")
    print(df["status"].value_counts())

    print("\nSuite Summary:")
    print(df["suite"].value_counts(dropna=False))

    print("\nRemaining Missing Values:")
    print(df.isna().sum())

    df.to_csv(OUTPUT_CSV, index=False)
    print(f"\nClean preprocessed log data saved → {OUTPUT_CSV}")

    # Quick Failure Summary
    fail_summary = df[df["status"].isin(["FAIL", "ABORT"])].groupby("error_msg").size().reset_index(name="count")
    fail_summary = fail_summary.sort_values(by="count", ascending=False)
    print("\nTop Failure Reasons:")
    print(fail_summary.head(10))
