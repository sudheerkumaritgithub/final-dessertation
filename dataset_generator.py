import csv
import math
import random
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Any, Optional

from config import DATA_DIR, TRANSACTIONS_FILE, DEFAULT_TRANSACTION_COUNT, DEFAULT_ACCOUNT_COUNT, DEFAULT_SUSPICIOUS_PCT

DEFAULT_TYPES = ["TRANSFER", "PAYMENT", "CASH_OUT", "DEPOSIT"]


def generate_synthetic_transactions(
    num_transactions: int = DEFAULT_TRANSACTION_COUNT,
    num_accounts: int = DEFAULT_ACCOUNT_COUNT,
    suspicious_pct: float = DEFAULT_SUSPICIOUS_PCT,
    transaction_types: Optional[List[str]] = None,
    output_path: Optional[Path] = None,
    start_time: Optional[datetime] = None,
    time_step_seconds: float = 1.0,
    seed: int = 42
) -> Dict[str, Any]:
    """
    Generates a realistic synthetic financial transaction dataset with embedded
    velocity and network fraud patterns.
    """
    random.seed(seed)
    if transaction_types is None or len(transaction_types) == 0:
        transaction_types = DEFAULT_TYPES

    if output_path is None:
        output_path = TRANSACTIONS_FILE

    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    if start_time is None:
        start_time = datetime(2026, 3, 1, 9, 0, 0)

    # Initialize account balances
    accounts = [f"ACC_{i:05d}" for i in range(1, num_accounts + 1)]
    balances = {acc: round(random.uniform(5000.0, 100000.0), 2) for acc in accounts}

    total_suspicious = int(num_transactions * (suspicious_pct / 100.0))
    total_normal = num_transactions - total_suspicious

    # Allocate fraud pattern counts
    # Patterns: HIGH_FREQUENCY, UNUSUAL_AMOUNT, MULTI_HOP, CIRCULAR, SHARED_BENEFICIARY
    fraud_types = ["HIGH_FREQUENCY", "UNUSUAL_AMOUNT", "MULTI_HOP", "CIRCULAR", "SHARED_BENEFICIARY"]
    fraud_counts = {ft: 0 for ft in fraud_types}
    
    # We will generate rows in batches or sequences
    records: List[Dict[str, Any]] = []
    current_time = start_time
    tx_counter = 1

    # Function to create a clean record and update balances
    def make_record(sender: str, receiver: str, amt: float, tx_type: str, ts: datetime, is_susp: int, f_type: str) -> Dict[str, Any]:
        nonlocal tx_counter
        amt = round(max(1.0, amt), 2)
        s_before = round(balances.get(sender, 10000.0), 2)
        r_before = round(balances.get(receiver, 10000.0), 2)

        s_after = round(max(0.0, s_before - amt), 2)
        r_after = round(r_before + amt, 2)

        balances[sender] = s_after
        balances[receiver] = r_after

        rec = {
            "transaction_id": f"TX_{tx_counter:08d}",
            "sender_id": sender,
            "receiver_id": receiver,
            "amount": amt,
            "transaction_type": tx_type,
            "timestamp": ts.isoformat() + "Z",
            "sender_balance_before": s_before,
            "sender_balance_after": s_after,
            "receiver_balance_before": r_before,
            "receiver_balance_after": r_after,
            "is_suspicious": is_susp,
            "fraud_type": f_type
        }
        tx_counter += 1
        return rec

    # Generate fraud clusters first or interleave
    fraud_records: List[Dict[str, Any]] = []
    target_fraud = total_suspicious

    while len(fraud_records) < target_fraud:
        chosen_pattern = random.choice(fraud_types)
        pattern_ts = start_time + timedelta(seconds=random.randint(100, int(num_transactions * time_step_seconds)))

        if chosen_pattern == "HIGH_FREQUENCY":
            # 5 to 8 rapid transactions from single sender within 30 seconds
            burst_sender = random.choice(accounts)
            burst_len = min(random.randint(5, 8), target_fraud - len(fraud_records))
            burst_recipients = random.sample(accounts, burst_len)
            for i in range(burst_len):
                burst_t = pattern_ts + timedelta(seconds=i * 2)
                amt = random.uniform(100.0, 800.0)
                rec = make_record(burst_sender, burst_recipients[i], amt, "TRANSFER", burst_t, 1, "HIGH_FREQUENCY")
                fraud_records.append(rec)
                fraud_counts["HIGH_FREQUENCY"] += 1

        elif chosen_pattern == "UNUSUAL_AMOUNT":
            # Very large transaction draining 90% or spike amount
            sender = random.choice(accounts)
            receiver = random.choice(accounts)
            while receiver == sender:
                receiver = random.choice(accounts)
            spike_amt = random.uniform(45000.0, 95000.0)
            rec = make_record(sender, receiver, spike_amt, "TRANSFER", pattern_ts, 1, "UNUSUAL_AMOUNT")
            fraud_records.append(rec)
            fraud_counts["UNUSUAL_AMOUNT"] += 1

        elif chosen_pattern == "MULTI_HOP":
            # 3-hop transfer chain: A -> B -> C -> D
            chain_nodes = random.sample(accounts, 4)
            chain_len = min(3, target_fraud - len(fraud_records))
            base_amt = random.uniform(5000.0, 15000.0)
            for i in range(chain_len):
                hop_t = pattern_ts + timedelta(minutes=i * 2)
                hop_amt = base_amt * (0.97 ** i) # slight decrement (simulated laundering fee)
                rec = make_record(chain_nodes[i], chain_nodes[i+1], hop_amt, "TRANSFER", hop_t, 1, "MULTI_HOP")
                fraud_records.append(rec)
                fraud_counts["MULTI_HOP"] += 1

        elif chosen_pattern == "CIRCULAR":
            # Loop: A -> B -> C -> A
            loop_nodes = random.sample(accounts, 3)
            loop_len = min(3, target_fraud - len(fraud_records))
            base_amt = random.uniform(8000.0, 20000.0)
            edges = [
                (loop_nodes[0], loop_nodes[1]),
                (loop_nodes[1], loop_nodes[2]),
                (loop_nodes[2], loop_nodes[0])
            ]
            for i in range(loop_len):
                circ_t = pattern_ts + timedelta(minutes=i * 3)
                rec = make_record(edges[i][0], edges[i][1], base_amt, "TRANSFER", circ_t, 1, "CIRCULAR")
                fraud_records.append(rec)
                fraud_counts["CIRCULAR"] += 1

        elif chosen_pattern == "SHARED_BENEFICIARY":
            # Smurfing: 4-6 distinct senders sending to one beneficiary in close time
            beneficiary = random.choice(accounts)
            smurf_len = min(random.randint(4, 6), target_fraud - len(fraud_records))
            available_senders = [a for a in accounts if a != beneficiary]
            smurf_senders = random.sample(available_senders, smurf_len)
            for i in range(smurf_len):
                smurf_t = pattern_ts + timedelta(seconds=i * 15)
                amt = random.uniform(900.0, 2500.0)
                rec = make_record(smurf_senders[i], beneficiary, amt, "TRANSFER", smurf_t, 1, "SHARED_BENEFICIARY")
                fraud_records.append(rec)
                fraud_counts["SHARED_BENEFICIARY"] += 1

    # Generate normal transactions
    normal_records: List[Dict[str, Any]] = []
    normal_step = time_step_seconds
    for i in range(total_normal):
        current_time += timedelta(seconds=random.uniform(0.5 * normal_step, 1.5 * normal_step))
        sender, receiver = random.sample(accounts, 2)
        ttype = random.choice(transaction_types)
        
        # Realistic log-normal amount
        amt = math.exp(random.gauss(5.0, 1.2)) # mean ~$150-$300
        amt = min(max(5.0, amt), 8000.0)

        rec = make_record(sender, receiver, amt, ttype, current_time, 0, "NORMAL")
        normal_records.append(rec)

    # Combine and sort by timestamp
    all_records = normal_records + fraud_records
    all_records.sort(key=lambda r: r["timestamp"])

    # Re-assign sequential transaction_ids
    for idx, r in enumerate(all_records, start=1):
        r["transaction_id"] = f"TX_{idx:08d}"

    # Write to CSV
    fieldnames = [
        "transaction_id",
        "sender_id",
        "receiver_id",
        "amount",
        "transaction_type",
        "timestamp",
        "sender_balance_before",
        "sender_balance_after",
        "receiver_balance_before",
        "receiver_balance_after",
        "is_suspicious",
        "fraud_type"
    ]

    with open(output_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(all_records)

    total_amount = sum(r["amount"] for r in all_records)
    type_counts = {}
    for r in all_records:
        t = r["transaction_type"]
        type_counts[t] = type_counts.get(t, 0) + 1

    return {
        "file_path": str(output_path),
        "total_transactions": len(all_records),
        "normal_transactions": len(normal_records),
        "suspicious_transactions": len(fraud_records),
        "suspicious_pct": round(len(fraud_records) / len(all_records) * 100, 2),
        "total_amount": round(total_amount, 2),
        "fraud_distribution": fraud_counts,
        "type_distribution": type_counts,
        "accounts_count": num_accounts,
        "start_time": all_records[0]["timestamp"],
        "end_time": all_records[-1]["timestamp"]
    }


def get_dataset_summary(csv_path: Optional[Path] = None) -> Optional[Dict[str, Any]]:
    """Returns quick metadata and summary statistics for existing CSV."""
    if csv_path is None:
        csv_path = TRANSACTIONS_FILE

    csv_path = Path(csv_path)
    if not csv_path.exists():
        return None

    total = 0
    suspicious = 0
    total_amt = 0.0
    types: Dict[str, int] = {}
    frauds: Dict[str, int] = {}
    senders = set()
    receivers = set()
    first_ts = None
    last_ts = None

    with open(csv_path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            total += 1
            amt = float(row.get("amount", 0))
            total_amt += amt
            is_susp = int(row.get("is_suspicious", 0))
            if is_susp == 1:
                suspicious += 1
            ft = row.get("fraud_type", "NORMAL")
            frauds[ft] = frauds.get(ft, 0) + 1
            tt = row.get("transaction_type", "UNKNOWN")
            types[tt] = types.get(tt, 0) + 1
            senders.add(row.get("sender_id"))
            receivers.add(row.get("receiver_id"))
            ts = row.get("timestamp")
            if first_ts is None:
                first_ts = ts
            last_ts = ts

    file_size_kb = round(csv_path.stat().st_size / 1024, 2)
    return {
        "file_exists": True,
        "file_path": str(csv_path),
        "file_size_kb": file_size_kb,
        "total_transactions": total,
        "suspicious_transactions": suspicious,
        "normal_transactions": total - suspicious,
        "suspicious_pct": round((suspicious / total * 100), 2) if total > 0 else 0,
        "total_amount": round(total_amt, 2),
        "unique_senders": len(senders),
        "unique_receivers": len(receivers),
        "unique_accounts": len(senders.union(receivers)),
        "type_distribution": types,
        "fraud_distribution": frauds,
        "start_time": first_ts,
        "end_time": last_ts
    }


def preview_transactions(csv_path: Optional[Path] = None, limit: int = 50, offset: int = 0) -> Dict[str, Any]:
    """Returns a slice of transaction records for UI preview."""
    if csv_path is None:
        csv_path = TRANSACTIONS_FILE

    csv_path = Path(csv_path)
    if not csv_path.exists():
        return {"rows": [], "total": 0, "offset": offset, "limit": limit}

    rows = []
    total = 0
    with open(csv_path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for idx, row in enumerate(reader):
            total += 1
            if offset <= idx < (offset + limit):
                rows.append(row)

    return {
        "rows": rows,
        "total": total,
        "offset": offset,
        "limit": limit
    }


if __name__ == "__main__":
    print("Generating default synthetic dataset...")
    res = generate_synthetic_transactions(num_transactions=5000, num_accounts=500, suspicious_pct=5.0)
    print("Generated:", res)
