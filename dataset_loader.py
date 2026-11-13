import csv
import json
import time
from pathlib import Path
from typing import Dict, List, Any, Optional

from config import (
    CLASSES_FILE, EDGELIST_FILE, FEATURES_FILE,
    PROCESSED_DATA_DIR, PROCESSED_TXS_FILE, PROCESSED_EDGES_FILE, METADATA_FILE
)


def preprocess_elliptic_dataset(
    feature_count: int = 15,
    max_records: Optional[int] = None,
    force: bool = False
) -> Dict[str, Any]:
    """
    Loads raw Elliptic Bitcoin dataset files, merges features with classes,
    validates edges, and saves cached processed CSVs under data/processed/.
    Implemented with pure-Python streaming to guarantee 100% environment compatibility
    without binary C-extension dependencies.
    """
    PROCESSED_DATA_DIR.mkdir(parents=True, exist_ok=True)

    if not force and PROCESSED_TXS_FILE.exists() and PROCESSED_EDGES_FILE.exists() and METADATA_FILE.exists():
        try:
            with open(METADATA_FILE, "r", encoding="utf-8") as f:
                meta = json.load(f)
                meta["cached"] = True
                return meta
        except Exception:
            pass

    start_time = time.time()

    # 1. Load classes mapping: tx_id -> class
    classes_map: Dict[str, str] = {}
    with open(CLASSES_FILE, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            tx_id = row.get("txId") or row.get("tx_id")
            if tx_id:
                classes_map[tx_id.strip()] = row.get("class", "unknown").strip()

    # 2. Stream features, merge with class label, and write processed transactions
    feat_cols = [f"feat_{i}" for i in range(1, feature_count + 1)]
    tx_headers = ["tx_id", "time_step"] + feat_cols + ["class"]

    illicit_count = 0
    licit_count = 0
    unknown_count = 0
    timestep_counts: Dict[str, int] = {}
    valid_tx_ids = set()

    with open(FEATURES_FILE, "r", encoding="utf-8") as f_in, \
         open(PROCESSED_TXS_FILE, "w", newline="", encoding="utf-8") as f_out:

        reader = csv.reader(f_in)
        writer = csv.writer(f_out)
        writer.writerow(tx_headers)

        row_idx = 0
        for row in reader:
            if not row or len(row) < 2:
                continue

            if max_records and row_idx >= max_records:
                break

            tx_id = row[0].strip()
            time_step = row[1].strip()
            features = [row[i].strip() if i < len(row) else "0.0" for i in range(2, 2 + feature_count)]

            # Lookup class label
            label = classes_map.get(tx_id, "unknown")
            if label == "1":
                illicit_count += 1
            elif label == "2":
                licit_count += 1
            else:
                label = "unknown"
                unknown_count += 1

            # Update time step distribution
            timestep_counts[time_step] = timestep_counts.get(time_step, 0) + 1
            valid_tx_ids.add(tx_id)

            writer.writerow([tx_id, time_step] + features + [label])
            row_idx += 1

    total_transactions = row_idx

    # 3. Stream and validate edges
    total_edges = 0
    with open(EDGELIST_FILE, "r", encoding="utf-8") as f_in, \
         open(PROCESSED_EDGES_FILE, "w", newline="", encoding="utf-8") as f_out:

        reader = csv.DictReader(f_in)
        writer = csv.writer(f_out)
        writer.writerow(["tx_id_1", "tx_id_2"])

        for row in reader:
            tx1 = (row.get("txId1") or row.get("tx_id_1", "")).strip()
            tx2 = (row.get("txId2") or row.get("tx_id_2", "")).strip()

            # Include edge only if both nodes are present in active transaction set
            if tx1 in valid_tx_ids and tx2 in valid_tx_ids:
                writer.writerow([tx1, tx2])
                total_edges += 1

    elapsed = round(time.time() - start_time, 2)

    # Sort time steps numerically
    sorted_timesteps = {}
    for k in sorted(timestep_counts.keys(), key=lambda x: int(x) if x.isdigit() else 999):
        sorted_timesteps[k] = timestep_counts[k]

    metadata = {
        "dataset_name": "Elliptic Bitcoin Dataset",
        "total_transactions": total_transactions,
        "total_relationships": total_edges,
        "illicit_transactions": illicit_count,
        "licit_transactions": licit_count,
        "unknown_transactions": unknown_count,
        "time_step_count": len(timestep_counts),
        "min_time_step": min((int(x) for x in timestep_counts.keys() if x.isdigit()), default=1),
        "max_time_step": max((int(x) for x in timestep_counts.keys() if x.isdigit()), default=49),
        "feature_count": feature_count,
        "feature_columns": feat_cols,
        "class_distribution": {
            "illicit": illicit_count,
            "licit": licit_count,
            "unknown": unknown_count
        },
        "time_step_distribution": sorted_timesteps,
        "processing_time_sec": elapsed,
        "processed_tx_file": str(PROCESSED_TXS_FILE),
        "processed_edge_file": str(PROCESSED_EDGES_FILE),
        "cached": False
    }

    with open(METADATA_FILE, "w", encoding="utf-8") as f:
        json.dump(metadata, f, indent=2)

    return metadata


def get_dataset_summary() -> Dict[str, Any]:
    """
    Returns high-level metadata and distribution for the Elliptic dataset.
    Loads cached metadata if available, otherwise triggers preprocessing.
    """
    if METADATA_FILE.exists():
        try:
            with open(METADATA_FILE, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            pass

    if CLASSES_FILE.exists() and EDGELIST_FILE.exists() and FEATURES_FILE.exists():
        return preprocess_elliptic_dataset(feature_count=15)

    return {
        "dataset_name": "Elliptic Bitcoin Dataset",
        "total_transactions": 0,
        "total_relationships": 0,
        "illicit_transactions": 0,
        "licit_transactions": 0,
        "unknown_transactions": 0,
        "time_step_count": 0,
        "class_distribution": {"illicit": 0, "licit": 0, "unknown": 0},
        "time_step_distribution": {},
        "files_found": False
    }


def preview_transactions(limit: int = 25, offset: int = 0) -> Dict[str, Any]:
    """
    Returns a slice of merged Elliptic transactions for table display.
    """
    if not PROCESSED_TXS_FILE.exists():
        get_dataset_summary()

    if not PROCESSED_TXS_FILE.exists():
        return {"rows": [], "total": 0, "offset": offset, "limit": limit}

    rows = []
    total = get_dataset_summary().get("total_transactions", 0)

    with open(PROCESSED_TXS_FILE, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        # Skip offset rows
        for _ in range(offset):
            try:
                next(reader)
            except StopIteration:
                break

        for row in reader:
            rows.append(row)
            if len(rows) >= limit:
                break

    return {
        "rows": rows,
        "total": total,
        "offset": offset,
        "limit": limit
    }


def preview_relationships(limit: int = 25, offset: int = 0) -> Dict[str, Any]:
    """
    Returns a slice of Elliptic directed edgelist for table display.
    """
    if not PROCESSED_EDGES_FILE.exists():
        get_dataset_summary()

    if not PROCESSED_EDGES_FILE.exists():
        return {"rows": [], "total": 0, "offset": offset, "limit": limit}

    rows = []
    total = get_dataset_summary().get("total_relationships", 0)

    with open(PROCESSED_EDGES_FILE, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for _ in range(offset):
            try:
                next(reader)
            except StopIteration:
                break

        for row in reader:
            rows.append(row)
            if len(rows) >= limit:
                break

    return {
        "rows": rows,
        "total": total,
        "offset": offset,
        "limit": limit
    }


if __name__ == "__main__":
    print("Testing pure-Python Elliptic Dataset preprocessing...")
    res = preprocess_elliptic_dataset(feature_count=15, force=True)
    print("Preprocessed successfully:")
    print(f"Transactions: {res['total_transactions']}, Edges: {res['total_relationships']}")
    print(f"Illicit: {res['illicit_transactions']}, Licit: {res['licit_transactions']}, Unknown: {res['unknown_transactions']}")
    print(f"Time taken: {res['processing_time_sec']}s")
