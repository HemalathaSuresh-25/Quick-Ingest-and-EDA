import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import os

INPUT_FILE = "data/cluster/failure_clusters.csv"
OUTPUT_DIR = "data/outputs"
os.makedirs(OUTPUT_DIR, exist_ok=True)

def analyze_failure_correlations():
    # Load data
    df = pd.read_csv(INPUT_FILE)
    print("Columns in dataset:", df.columns.tolist())
    
    # Ensure critical columns exist
    for col in ["cluster", "dut_version", "config", "run_date"]:
        if col not in df.columns:
            print(f"Warning: '{col}' not found. Creating placeholder column.")
            df[col] = "Unknown"

    # Fill missing values in categorical columns
    df['dut_version'] = df['dut_version'].fillna('Unknown')
    df['config'] = df['config'].fillna('Unknown')
    df['cluster'] = df['cluster'].fillna('Unknown')

    # --- Frequency by cluster ---
    cluster_counts = df['cluster'].value_counts().reset_index()
    cluster_counts.columns = ['cluster', 'count']
    print("Cluster counts:\n", cluster_counts)

    # --- Correlation with DUT versions ---
    plt.figure(figsize=(12,6))
    sns.countplot(data=df, x='dut_version', hue='cluster')
    plt.title("Failure Clusters by DUT Version")
    plt.xlabel("DUT Version")
    plt.ylabel("Number of Failures")
    plt.xticks(rotation=45)
    plt.legend(title='Cluster')
    plt.tight_layout()
    plt.savefig(os.path.join(OUTPUT_DIR, "clusters_by_dut_version.png"))
    plt.close()

    # --- Correlation with Configurations ---
    plt.figure(figsize=(12,6))
    sns.countplot(data=df, x='config', hue='cluster')
    plt.title("Failure Clusters by Configuration")
    plt.xlabel("Configuration")
    plt.ylabel("Number of Failures")
    plt.xticks(rotation=45)
    plt.legend(title='Cluster')
    plt.tight_layout()
    plt.savefig(os.path.join(OUTPUT_DIR, "clusters_by_config.png"))
    plt.close()

    # --- Trend over Time ---
    df["run_date"] = pd.to_datetime(df["run_date"], errors='coerce')
    if df["run_date"].isnull().all():
        print("Warning: run_date column could not be parsed. Skipping trend plot.")
    else:
        trend = df.groupby([df["run_date"].dt.to_period("M"), "cluster"]).size().unstack(fill_value=0)
        trend.index = trend.index.to_timestamp()  # convert PeriodIndex to datetime
        trend.plot(kind='line', figsize=(12,6), marker='o', title="Failure Cluster Trend Over Time")
        plt.xlabel("Month")
        plt.ylabel("Number of Failures")
        plt.xticks(rotation=45)
        plt.tight_layout()
        plt.savefig(os.path.join(OUTPUT_DIR, "cluster_trends_over_time.png"))
        plt.close()
    print("Correlation visualizations saved in:", OUTPUT_DIR)

if __name__ == "__main__":
    analyze_failure_correlations()
