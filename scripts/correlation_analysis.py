import os
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from datetime import datetime
# Configuration
PREFERRED_PATH = "data/clusters/failure_clusters.csv"
FALLBACK_PATH = "data/cluster/failure_clusters.csv"
OUTPUT_DIR = "data/outputs"

os.makedirs(OUTPUT_DIR, exist_ok=True)

# Helper Functions
def find_input_file():
    """Locate the correct failure_clusters.csv file."""
    if os.path.exists(PREFERRED_PATH):
        print(f"Found input file → {PREFERRED_PATH}")
        return PREFERRED_PATH
    elif os.path.exists(FALLBACK_PATH):
        print(f"Using fallback file → {FALLBACK_PATH}")
        return FALLBACK_PATH
    else:
        raise FileNotFoundError(
            "Could not find 'failure_clusters.csv' in either:\n"
            f"  • {PREFERRED_PATH}\n"
            f"  • {FALLBACK_PATH}\n"
            "Please run 'failure_clustering.py' first to generate it."
        )


def save_plot(fig, filename):
    """Utility to save plots cleanly."""
    out_path = os.path.join(OUTPUT_DIR, filename)
    fig.savefig(out_path, bbox_inches="tight", dpi=300)
    plt.close(fig)
    print(f"Saved: {out_path}")

# Core Analysis
def analyze_failure_correlations():
    input_file = find_input_file()
    print(f"\n🔍 Starting correlation analysis from: {input_file}\n")

    # Load dataset
    df = pd.read_csv(input_file)
    print(f"Loaded {df.shape[0]} rows, {df.shape[1]} columns")
    print("Available columns:", df.columns.tolist())

    # Ensure critical columns
    for col in ["cluster", "dut_version", "config", "run_date"]:
        if col not in df.columns:
            print(f"Missing column '{col}' → creating placeholder.")
            df[col] = "Unknown"

    # Fill missing values
    df["dut_version"] = df["dut_version"].fillna("Unknown")
    df["config"] = df["config"].fillna("Unknown")
    df["cluster"] = df["cluster"].fillna("Unknown")

    #Cluster frequency summary 
    cluster_counts = df["cluster"].value_counts().reset_index()
    cluster_counts.columns = ["cluster", "count"]
    summary_path = os.path.join(OUTPUT_DIR, "cluster_correlation_summary.csv")
    cluster_counts.to_csv(summary_path, index=False)
    print(f"Cluster summary saved → {summary_path}")
    print(cluster_counts.head(), "\n")

    # Visualization: Clusters by DUT Version 
    fig, ax = plt.subplots(figsize=(12, 6))
    sns.countplot(data=df, x="dut_version", hue="cluster", ax=ax, palette="tab10")
    ax.set_title("Failure Clusters by DUT Version", fontsize=14, weight="bold")
    ax.set_xlabel("DUT Version")
    ax.set_ylabel("Number of Failures")
    plt.xticks(rotation=45, ha="right")
    plt.legend(title="Cluster")
    plt.tight_layout()
    save_plot(fig, "clusters_by_dut_version.png")

    #Visualization: Clusters by Configuration 
    fig, ax = plt.subplots(figsize=(12, 6))
    sns.countplot(data=df, x="config", hue="cluster", ax=ax, palette="tab20")
    ax.set_title("Failure Clusters by Configuration", fontsize=14, weight="bold")
    ax.set_xlabel("Configuration")
    ax.set_ylabel("Number of Failures")
    plt.xticks(rotation=45, ha="right")
    plt.legend(title="Cluster")
    plt.tight_layout()
    save_plot(fig, "clusters_by_config.png")

    #Visualization: Trend Over Time
    df["run_date"] = pd.to_datetime(df["run_date"], errors="coerce")
    if df["run_date"].notna().any():
        trend = (
            df.groupby([df["run_date"].dt.to_period("M"), "cluster"])
            .size()
            .unstack(fill_value=0)
        )
        trend.index = trend.index.to_timestamp()

        fig, ax = plt.subplots(figsize=(12, 6))
        trend.plot(ax=ax, marker="o")
        ax.set_title("Failure Cluster Trend Over Time", fontsize=14, weight="bold")
        ax.set_xlabel("Month")
        ax.set_ylabel("Number of Failures")
        plt.xticks(rotation=45)
        plt.tight_layout()
        save_plot(fig, "cluster_trends_over_time.png")
    else:
        print("Skipping time trend plot (invalid or missing run_date values).")

    print(f"\nCorrelation analysis complete. Results saved in → {OUTPUT_DIR}")

# Entry Point
if __name__ == "__main__":
    analyze_failure_correlations()
