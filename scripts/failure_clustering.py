"""
failure_clustering.py
----------------------
Failure Signature Generation with BERT embeddings:
- Excludes "no_error" rows from clustering
- Convert real failure logs into dense vectors using BERT
- Cluster similar messages using KMeans
- Detect top recurring error keywords per cluster

Input:  data/features/failure_features.csv
Output: data/features/failure_clusters_bert.csv
"""

import os
import pandas as pd
import numpy as np
from sklearn.cluster import KMeans
from sklearn.feature_extraction.text import TfidfVectorizer
from sentence_transformers import SentenceTransformer

# Config
INPUT_FILE = "data/features/failure_features.csv"
#OUTPUT_FILE = "data/features/failure_clusters_bert.csv"
OUTPUT_DIR = "data/cluster"
os.makedirs(OUTPUT_DIR, exist_ok=True)
OUTPUT_FILE = os.path.join(OUTPUT_DIR, "failure_clusters.csv")

TOP_KEYWORDS = 7        # Number of keywords to show per cluster
KMEANS_CLUSTERS = 20
BERT_MODEL = "all-MiniLM-L6-v2"  # lightweight, fast sentence-transformer

os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)


def cluster_failures_bert():
    print("Loading dataset...")
    df = pd.read_csv(INPUT_FILE)
    print(f"Loaded dataset: {df.shape[0]} rows, {df.shape[1]} columns")

    # Ensure error message column exists
    if "error_msg" not in df.columns:
        raise ValueError("Column 'error_msg' not found in dataset.")
    df["error_msg"] = df["error_msg"].fillna("")

    # ==============================
    # Separate real failures vs no_error
    df["is_failure"] = df["error_msg"].apply(lambda x: 0 if str(x).lower() == "No Error" else 1)
    df_failures = df[df["is_failure"] == 1].copy()

    if df_failures.empty:
        raise ValueError("No real failures found for clustering!")

    # ==============================
    # BERT embeddings
    print(f"Encoding {df_failures.shape[0]} failure messages using BERT model: {BERT_MODEL} ...")
    model = SentenceTransformer(BERT_MODEL)
    embeddings = model.encode(df_failures["error_msg"].tolist(), show_progress_bar=True, convert_to_numpy=True)

    # ==============================
    # KMeans clustering
    print(f"Clustering embeddings with KMeans (k={KMEANS_CLUSTERS})...")
    kmeans = KMeans(n_clusters=KMEANS_CLUSTERS, random_state=42, n_init=10)
    cluster_labels = kmeans.fit_predict(embeddings)

    # Assign clusters back to df
    df["cluster"] = -1  # default for no_error rows
    df.loc[df_failures.index, "cluster"] = cluster_labels

    # ==============================
    # Top keywords per cluster (TF-IDF)
    print("Extracting top keywords per cluster using TF-IDF...")
    vectorizer = TfidfVectorizer(max_features=3000, stop_words="english")
    X_tfidf = vectorizer.fit_transform(df_failures["error_msg"])
    feature_names = np.array(vectorizer.get_feature_names_out())

    top_keywords_per_cluster = {}
    for cluster_num in range(KMEANS_CLUSTERS):
        cluster_indices = np.where(cluster_labels == cluster_num)[0]
        if len(cluster_indices) == 0:
            top_keywords_per_cluster[cluster_num] = []
            continue
        cluster_tfidf = X_tfidf[cluster_indices].mean(axis=0)
        top_indices = np.asarray(cluster_tfidf).flatten().argsort()[::-1][:TOP_KEYWORDS]
        top_keywords = feature_names[top_indices].tolist()
        top_keywords_per_cluster[cluster_num] = top_keywords
        print(f"Cluster {cluster_num}: {', '.join(top_keywords)}")

    # ==============================
    # Save results
    df.to_csv(OUTPUT_FILE, index=False)
    print(f"\nFailure clusters with BERT saved → {OUTPUT_FILE}")

    return df, top_keywords_per_cluster


if __name__ == "__main__":
    cluster_failures_bert()
