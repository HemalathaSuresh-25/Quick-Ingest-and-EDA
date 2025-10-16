import os
import re
import pandas as pd
from datetime import datetime

INPUT_DIR = "C:/Users/hemalatha/Desktop/attest-eda/data/standardized"
OUTPUT_CSV = "C:/Users/hemalatha/Desktop/attest-eda/data/logs_preprocessed.csv"

# ---------------- Suite List ---------------- #
SUITES = [
    "ptp-oc","ptp-tc","dtmf","tcp-xp-tec","ipv6-host","v6bgp4",
    "rtpp","sct","lacp-tec","ipv4","ptp-bc","udp-tec","sip"
]

# ---------------- DUT Header Extraction ---------------- #
def extract_header_info(file_path):
    """Extract DUT, version, OS, configuration, and test case info from header lines."""
    dut_name, dut_version, os_version, config, test_case = None, None, None, None, None
    with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
        for line in f:
            line = line.strip()
            # Extract key header values
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

            # Stop early if key info found
            if all([dut_name, dut_version, os_version, config, test_case]):
                break
    return dut_name, dut_version, os_version, config, test_case

# ---------------- Log Line Extraction ---------------- #
def extract_log_line_features(line, file_date=None, filename=None):
    """Extract timestamp, status (PASS/FAIL/ABORT), and reason message from log lines."""
    line = line.strip()
    timestamp, status, error_msg, run_date, suite = None, None, None, None, None

    # Extract timestamp
    ts_match = re.match(r"(\d{2}:\d{2}:\d{2}\.\d+)", line)
    if ts_match and file_date:
        time_str = ts_match.group(1)
        try:
            timestamp = datetime.strptime(f"{file_date} {time_str}", "%Y-%m-%d %H:%M:%S.%f")
            run_date = timestamp.date()
        except Exception:
            run_date = file_date

    # Unified regex patterns for status & message extraction
    patterns = [
        # Covers "# Result: FAILED reason"
        r"#\s*Result\s*[:\-]?\s*(PASSED|FAILED|ABORTED|PASS|FAIL|ABORT)\b\s*[:\-]?\s*(.*)",
        # Covers "# TEST CASE FAILED : reason"
        r"#\s*TEST\s*CASE\s*(PASSED|FAILED|ABORTED|PASS|FAIL|ABORT)\b\s*[:\-]?\s*(.*)",
        # Covers "TEST CASE FAILED due to..." without hash
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

    # Extract suite name from filename
    if filename:
        for su in SUITES:
            if su.lower() in filename.lower():
                suite = su
                break

    # Always set OS version = Linux
    os_version = "Linux"

    return timestamp, status, error_msg, run_date, suite, os_version


# ---------------- DUT Version Fallbacks ---------------- #
def infer_dut_version(file, lines):
    """Try to find DUT version from filename or inside file content."""
    # First, try to extract from filename (e.g., INTERFACE_IP_2_AS_20240722-113936.log)
    m = re.search(r'_(AS|TEC|IP|BC|OC|V\d+)[-_]?\d{8}', file, re.IGNORECASE)
    if m:
        return m.group(1).upper()

    # Try scanning for version patterns inside the file content
    for line in lines:
        m = re.search(r"version\s*[:=]\s*([A-Za-z0-9\.\-_]+)", line, re.IGNORECASE)
        if m:
            return m.group(1).strip()

    return "Generic_v1.0"  # fallback default version label


# ---------------- Main Processing ---------------- #
def process_logs(input_dir):
    all_data = []
    for root, _, files in os.walk(input_dir):
        for file in files:
            if not file.endswith(".log"):
                continue
            filepath = os.path.join(root, file)
            # Extract run date from filename
            date_match = re.search(r'(\d{8})', file)
            file_date = datetime.strptime(date_match.group(1), "%Y%m%d").date() if date_match else None

            # Load file lines once
            with open(filepath, 'r', encoding='utf-8', errors='ignore') as f:
                lines = f.readlines()

            # Extract header info
            dut_name, dut_version, os_version, config, test_case = extract_header_info(filepath)

            # Intelligent fallback for missing header values
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

            # Process line-by-line for log events
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

    df = pd.DataFrame(all_data)
    # Clean up: drop rows without meaningful status or error message if needed
    df = df[(df["status"].notna()) | (df["error_msg"].notna())]
    return df


# ---------------- Run Script ---------------- #
if __name__ == "__main__":
    df = process_logs(INPUT_DIR)
    print("Total rows extracted:", len(df))

    # Summary of extracted data
    print("\n🔹 Status Summary:")
    print(df["status"].value_counts())

    print("\n🔹 Suite Summary:")
    print(df["suite"].value_counts())

    df.to_csv(OUTPUT_CSV, index=False)
    print(f"\nPreprocessed log data saved → {OUTPUT_CSV}")
