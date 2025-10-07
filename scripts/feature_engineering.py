"""
feature_engineering.py
----------------------
Generates derived features from standardized ATTEST logs
to support failure pattern detection (Task 2).

Input:  data/logs_preprocessed.csv
Output: data/features/failure_features.csv
Memory-efficient version for very large datasets.
"""

import os
import pandas as pd

# Configuration
INPUT_FILE = "C:/Users/hemalatha/Desktop/attest-eda/data/logs_preprocessed.csv"
OUTPUT_DIR = "data/features"
os.makedirs(OUTPUT_DIR, exist_ok=True)
OUTPUT_FILE = os.path.join(OUTPUT_DIR, "failure_features.csv")


def generate_features():
    print("Starting feature engineering...")

    # Load dataset
    df = pd.read_csv(INPUT_FILE)
    print(f"Loaded dataset: {df.shape[0]} rows, {df.shape[1]} columns")

    # Normalize status column
    df["status"] = df["status"].astype(str).str.strip().str.upper()

    # Ensure timestamp is datetime
    if "timestamp" in df.columns:
        df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")

    # ==============================
    # Failure Frequency per Suite and DUT
    fail_df = df[df["status"] == "FAIL"]

    suite_fail_freq = fail_df.groupby("suite")["status"].count().rename("failure_freq_suite")
    dut_fail_freq = fail_df.groupby("dut")["status"].count().rename("failure_freq_dut")

    df = df.merge(suite_fail_freq, on="suite", how="left")
    df = df.merge(dut_fail_freq, on="dut", how="left")

    df["failure_freq_suite"].fillna(0, inplace=True)
    df["failure_freq_dut"].fillna(0, inplace=True)

    # Failure Ratio per suite
    total_suite = df.groupby("suite")["status"].count().rename("total_runs_suite")
    df = df.merge(total_suite, on="suite", how="left")
    df["failure_ratio_suite"] = df["failure_freq_suite"] / df["total_runs_suite"]

    # ==============================
    # Time Since Last Failure (memory-efficient)
    df = df.sort_values(["dut", "timestamp"])
    df["time_since_last_failure"] = 0

    print("Calculating time_since_last_failure per DUT...")
    for dut, group in df.groupby("dut"):
        last_fail_time = None
        times = []
        for ts, status in zip(group["timestamp"], group["status"]):
            if pd.isna(ts):
                times.append(0)
                continue
            if status == "FAIL":
                if last_fail_time is None:
                    times.append(0)
                else:
                    times.append((ts - last_fail_time).total_seconds())
                last_fail_time = ts
            else:
                times.append(0)
        df.loc[group.index, "time_since_last_failure"] = times

    # ==============================
    # Average Execution Duration per Suite
    if "execution_duration" in df.columns:
        avg_duration = df.groupby("suite")["execution_duration"].mean().rename("avg_exec_duration_suite")
        df = df.merge(avg_duration, on="suite", how="left")

    # ==============================
    # Encode Config/Environment Info
    if "config" in df.columns:
        df["config_hash"] = df["config"].astype(str).apply(lambda x: abs(hash(x)) % (10 ** 8))

    # ==============================
    # Recent Failure Indicator
    df["recent_failure_flag"] = df["status"].apply(lambda x: 1 if x == "FAIL" else 0)

    # ==============================
    # Fill Remaining Missing Values
    df.fillna({
        "failure_freq_suite": 0,
        "failure_freq_dut": 0,
        "failure_ratio_suite": 0,
        "time_since_last_failure": 0,
        "avg_exec_duration_suite": 0
    }, inplace=True)

    # Save features
    df.to_csv(OUTPUT_FILE, index=False)
    print(f"Feature dataset saved → {OUTPUT_FILE}")
    print("Feature engineering complete!\n")

    return df


if __name__ == "__main__":
    generate_features()
