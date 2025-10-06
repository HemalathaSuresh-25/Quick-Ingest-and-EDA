import pandas as pd
import re
import os

INPUT_CSV = "C:/Users/hemalatha/Desktop/attest-eda/data/logs_combined.csv"
OUTPUT_CSV = "C:/Users/hemalatha/Desktop/attest-eda/data/logs_preprocessed.csv"

TIMESTAMP_PATTERNS = [
    r"\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d+",
    r"\d{8}-\d{6}",
    r"\d{2}:\d{2}:\d{2}\.\d+"
]

TESTCASE_PATTERNS = [
    r"(tc[_\-]?[a-z0-9]+[_\-]?\d+)",
    r"(test[_\-]?case[_\-]?\d+)",
    r"(TC\d+)",
    r"(case[_\-]?\d+)",
    r"(scenario[_\-]?\d+)"
]

ENV_PATTERNS = {
    "os_version": r"(Linux|Windows|Ubuntu)[^\s]*",
    "config": r"config[:=]\s*([a-zA-Z0-9_\-\.]+)"
}

def extract_first_match(text, patterns):
    if pd.isna(text):
        return None
    for pat in patterns:
        m = re.search(pat, str(text), re.IGNORECASE)
        if m:
            return m.group(1) if m.lastindex else m.group(0)
    return None

def normalize_status(status):
    if pd.isna(status):
        return None
    s = str(status).strip().lower()
    if "pass" in s:
        return "PASS"
    if "fail" in s:
        return "FAIL"
    if "abort" in s:
        return "ABORT"
    return None

def extract_dut(line, filename):
    patterns = [
        r"DUT[:=]?\s*([A-Za-z0-9_\-]+)",
        r"(dut[_\s]?[0-9a-zA-Z\-]+)"
    ]
    for pat in patterns:
        m = re.search(pat, str(line), re.IGNORECASE)
        if m:
            return m.group(1)
    fname = os.path.splitext(filename)[0]
    return fname.split("_")[0] if fname else None

def extract_env_from_lines(lines, pat):
    for line in lines:
        m = re.search(pat, str(line), re.IGNORECASE)
        if m:
            return m.group(1) if m.lastindex else m.group(0)
    return None

def extract_timestamp(line, run_date=None):
    for pat in TIMESTAMP_PATTERNS:
        m = re.search(pat, str(line))
        if m:
            ts = m.group(0)
            try:
                if re.match(r"\d{8}-\d{6}", ts):
                    return pd.to_datetime(ts, format="%Y%m%d-%H%M%S", errors='coerce')
                elif re.match(r"\d{2}:\d{2}:\d{2}\.\d+", ts) and run_date:
                    return pd.to_datetime(f"{run_date} {ts}", errors='coerce')
                else:
                    return pd.to_datetime(ts, errors='coerce')
            except:
                return None
    return None

def infer_config(filename, current=None):
    if pd.notna(current):
        return current
    fname = str(filename).lower()
    if "dtmf" in fname: return "DTMF"
    if "ptp" in fname: return "PTP"
    if "ipv6" in fname: return "IPv6"
    if "ipv4" in fname: return "IPv4"
    if "sip" in fname: return "SIP"
    if "lacp" in fname: return "LACP"
    if "tcp" in fname: return "TCP"
    if "udp" in fname: return "UDP"
    if "sct" in fname: return "SCTP"
    return "Default_Config"

def infer_os_version(row):
    if pd.notna(row.get("os_version")):
        return row["os_version"]

    text = str(row["raw_line"]).lower() + " " + str(row["filename"]).lower()

    if "windows" in text or "win" in text:
        return "Windows"
    if "ubuntu" in text:
        return "Ubuntu"
    if "linux" in text:
        return "Linux"

    return "Unknown_OS"

def main():
    df = pd.read_csv(INPUT_CSV, low_memory=False, dtype=str)
    print("Initial shape:", df.shape)

    df["status"] = df["status"].apply(normalize_status)
    df = df[df["status"].notna()].copy()

   
    df["timestamp"] = df.apply(lambda row: extract_timestamp(row["raw_line"], row.get("run_date")), axis=1)
    df["run_date"] = df["timestamp"].dt.date

    df.loc[df["run_date"].isna(), "run_date"] = df["filename"].apply(
        lambda f: extract_first_match(f, [r"(\d{8})"])
    )

    df["test_case_id"] = df.apply(
        lambda row: extract_first_match(row["raw_line"], TESTCASE_PATTERNS)
        if pd.isna(row.get("test_case_id"))
        else row["test_case_id"],
        axis=1
    )
    df["test_case_id"] = df["test_case_id"].fillna(df["filename"])

    df["dut"] = df.apply(lambda row: extract_dut(row["raw_line"], row["filename"]), axis=1)

    for col, pat in ENV_PATTERNS.items():
        for fname, group in df.groupby("filename"):
            first_lines = group["raw_line"].head(50).tolist()
            value = extract_env_from_lines(first_lines, pat)
            if value:
                df.loc[df["filename"] == fname, col] = value

    df["os_version"] = df.apply(infer_os_version, axis=1)

    df["config"] = df.apply(lambda row: infer_config(row["filename"], row.get("config")), axis=1)

    df["os_version"] = df.groupby("filename")["os_version"].ffill().bfill()
    df["config"] = df.groupby("filename")["config"].ffill().bfill()

    df["timestamp"] = df.apply(
        lambda row: pd.to_datetime(str(row["run_date"]))
        if pd.notna(row["run_date"]) and pd.isna(row["timestamp"])
        else row["timestamp"],
        axis=1
    )
    df["run_date"] = df["timestamp"].dt.date

    df = df[df["dut"].notna()]
    df = df[~df["dut"].str.lower().isin(["unknown_dut", "generic_dut"])]
    df = df[df["run_date"].notna()]

    if "error_msg" in df.columns:
        df.loc[df["status"] == "PASS", "error_msg"] = df.loc[df["status"] == "PASS", "error_msg"].fillna("No Error")

    drop_cols = [c for c in ["error_category", "dut_version"] if c in df.columns]
    if drop_cols:
        df.drop(columns=drop_cols, inplace=True)

    df.drop_duplicates(inplace=True)

    df.to_csv(OUTPUT_CSV, index=False)
    print(f"Cleaned dataset saved → {OUTPUT_CSV}")

    print("\nFinal Missing values:\n", df.isna().sum())
    print("\nSample rows:\n", df.head(10))

if __name__ == "__main__":
    main()
