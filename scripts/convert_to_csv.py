import os
import re
import pandas as pd

STANDARDIZED_PATH = "C:/Users/hemalatha/Desktop/attest-eda/data/standardized"
OUTPUT_CSV = "C:/Users/hemalatha/Desktop/attest-eda/data/logs_combined.csv"

TIMESTAMP_PATTERN = re.compile(r"\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}")
TESTCASE_PATTERN = re.compile(r"(tc[_\-]?\S+)", re.IGNORECASE)
DUT_VERSION_PATTERN = re.compile(r"DUT[_\s]?(v?\d+(\.\d+)*)", re.IGNORECASE)
CONFIG_PATTERN = re.compile(r"config[:=]\s*([a-zA-Z0-9_\-\.]+)", re.IGNORECASE)

def detect_status(line: str):
    line_low = line.lower()
    if "pass" in line_low and "fail" not in line_low:
        return "PASS"
    elif "fail" in line_low or "error" in line_low or "failed" in line_low:
        return "FAIL"
    elif "abort" in line_low or "aborted" in line_low or "stopped" in line_low:
        return "ABORT"
    fail_phrases = ["test failed", "step failed", "execution failed", "unexpected error", "exception occurred"]
    abort_phrases = ["timeout", "interrupted", "terminated", "stopped unexpectedly"]
    pass_phrases = ["successfully completed", "execution passed", "step passed"]

    for phrase in fail_phrases:
        if phrase in line_low:
            return "FAIL"
    for phrase in abort_phrases:
        if phrase in line_low:
            return "ABORT"
    for phrase in pass_phrases:
        if phrase in line_low:
            return "PASS"
    return None

def extract_first(pattern, text):
    match = pattern.search(text)
    if match:
        return match.group(1) if match.lastindex else match.group(0)
    return None

def parse_log(filepath, run_date, dut, suite, filename):
    rows = []
    with open(filepath, "r", encoding="utf-8", errors="ignore") as f:
        for i, line in enumerate(f, start=1):
            line = line.strip()
            if not line:
                continue

            status = detect_status(line)
            timestamp = extract_first(TIMESTAMP_PATTERN, line)
            test_case_id = extract_first(TESTCASE_PATTERN, line)
            dut_version = extract_first(DUT_VERSION_PATTERN, line)
            config = extract_first(CONFIG_PATTERN, line)

            fname_lower = filename.lower()
            if not config:
                if "dtmf" in fname_lower: config = "DTMF"
                elif "ptp" in fname_lower: config = "PTP"
                elif "ipv6" in fname_lower: config = "IPv6"
                elif "ipv4" in fname_lower: config = "IPv4"
                elif "sip" in fname_lower: config = "SIP"
                elif "lacp" in fname_lower: config = "LACP"
                elif "tcp" in fname_lower: config = "TCP"
                elif "udp" in fname_lower: config = "UDP"
                elif "sct" in fname_lower: config = "SCTP"
                else: config = None

            if timestamp:
                try:
                    run_date = pd.to_datetime(timestamp).date().isoformat()
                except:
                    pass

            rows.append({
                "run_date": run_date,
                "dut": dut,
                "suite": suite,
                "filename": filename,
                "line_number": i,
                "timestamp": timestamp,
                "test_case_id": test_case_id,
                "status": status,
                "error_msg": None if status == "PASS" else line if status else None,
                "dut_version": dut_version,
                "config": config,
                "raw_line": line
            })
    return rows

def convert_logs_to_csv(standardized_path, output_csv):
    all_rows = []

    for root, dirs, files in os.walk(standardized_path):
        for file in files:
            if file.endswith((".log", ".txt")):
                file_path = os.path.join(root, file)
                parts = os.path.relpath(file_path, standardized_path).split(os.sep)

                run_date = parts[0] if len(parts) > 0 else None
                dut = parts[1] if len(parts) > 1 else os.path.splitext(file)[0]
                suite = parts[2] if len(parts) > 2 else os.path.splitext(file)[0]

                if not run_date or run_date.lower() in ["unknown_date"]: run_date = None
                if not dut or dut.lower() in ["generic_dut", "unknown_dut"]: dut = os.path.splitext(file)[0]

                rows = parse_log(file_path, run_date, dut, suite, file)
                all_rows.extend(rows)

    if all_rows:
        df = pd.DataFrame(all_rows)
        df.drop_duplicates(inplace=True)
        df.to_csv(output_csv, index=False)
        print(f"CSV generated successfully at: {output_csv}")
        print(f"Total lines parsed: {len(df)}")
        print("Status counts:")
        print(df["status"].value_counts(dropna=False))
    else:
        print("No logs were parsed. Please check log formats or folder structure.")

if __name__ == "__main__":
    convert_logs_to_csv(STANDARDIZED_PATH, OUTPUT_CSV)


