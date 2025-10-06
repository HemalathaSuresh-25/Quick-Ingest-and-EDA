import os
import pandas as pd
import re
STANDARDIZED_PATH = "C:/Users/hemalatha/Desktop/attest-eda/data/standardized"
OUTPUT_CSV = "C:/Users/hemalatha/Desktop/attest-eda/data/logs_combined.csv"
def parse_log_file(filepath):
    """Parse a single log file into structured rows (customize based on log format)"""
    rows = []
    with open(filepath, "r", encoding="utf-8", errors="ignore") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            match = re.match(r"(\S+)\s+-\s+(\S+)\s+-\s+(\S+)", line)
            if match:
                timestamp, testcase, status = match.groups()
                rows.append({"timestamp": timestamp, "testcase": testcase, "status": status})
            else:
                rows.append({"timestamp": None, "testcase": None, "status": None, "raw": line})
    return rows

def convert_logs_to_csv(standardized_path, output_csv):
    all_rows = []

    for root, dirs, files in os.walk(standardized_path):
        for file in files:
            if file.endswith((".log", ".txt")):
                file_path = os.path.join(root, file)
                parts = os.path.relpath(file_path, standardized_path).split(os.sep)
                run_date = parts[0] if len(parts) > 0 else "unknown_date"
                dut = parts[1] if len(parts) > 1 else "generic_dut"
                suite = parts[2] if len(parts) > 2 else "unknown_suite"

                rows = parse_log_file(file_path)
                for row in rows:
                    row.update({"run_date": run_date, "dut": dut, "suite": suite, "filename": file})
                all_rows.extend(rows)
    if all_rows:
        df=pd.DataFrame(all_rows)
        df.to_csv(output_csv, index=False)
        print(f"CSV generated successfully at: {output_csv}")
    else:
        print("No logs were parsed. Please check log formats or folder structure.")
if __name__=="__main__":
    convert_logs_to_csv(STANDARDIZED_PATH, OUTPUT_CSV)