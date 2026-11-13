import json
import sqlite3
import time
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Any, Optional

from config import RESULTS_DB, PROCESSED_TXS_FILE, PROCESSED_EDGES_FILE
from influx_service import InfluxService
from neo4j_service import Neo4jService


class BenchmarkService:
    def __init__(self, db_path: Optional[Path] = None):
        self.db_path = Path(db_path) if db_path else RESULTS_DB
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self.init_db()

    def get_connection(self) -> sqlite3.Connection:
        conn = sqlite3.connect(str(self.db_path))
        conn.row_factory = sqlite3.Row
        return conn

    def init_db(self):
        """Initializes SQLite tables for experiment logging."""
        with self.get_connection() as conn:
            conn.execute("""
            CREATE TABLE IF NOT EXISTS experiments (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp TEXT NOT NULL,
                dataset_name TEXT DEFAULT 'elliptic',
                database_name TEXT NOT NULL,
                workload_type TEXT NOT NULL,
                workload_name TEXT NOT NULL,
                num_transactions INTEGER NOT NULL,
                execution_time_ms REAL NOT NULL,
                throughput_ops REAL NOT NULL,
                query_latency_ms REAL NOT NULL,
                details_json TEXT
            )
            """)
            # Check if dataset_name column exists (migration for existing table)
            cur = conn.execute("PRAGMA table_info(experiments)")
            cols = [r["name"] for r in cur.fetchall()]
            if "dataset_name" not in cols:
                try:
                    conn.execute("ALTER TABLE experiments ADD COLUMN dataset_name TEXT DEFAULT 'elliptic'")
                except Exception:
                    pass
            conn.commit()

    def log_experiment(
        self,
        database_name: str,
        workload_type: str,
        workload_name: str,
        num_transactions: int,
        execution_time_ms: float,
        throughput_ops: float,
        query_latency_ms: float,
        details: Optional[Dict[str, Any]] = None,
        dataset_name: str = "elliptic"
    ) -> int:
        """Stores a benchmark experiment run in SQLite."""
        ts = datetime.utcnow().isoformat() + "Z"
        details_str = json.dumps(details or {})
        with self.get_connection() as conn:
            cur = conn.execute("""
            INSERT INTO experiments (
                timestamp, dataset_name, database_name, workload_type, workload_name,
                num_transactions, execution_time_ms, throughput_ops, query_latency_ms, details_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                ts, dataset_name, database_name, workload_type, workload_name,
                num_transactions, execution_time_ms, throughput_ops, query_latency_ms, details_str
            ))
            conn.commit()
            return cur.lastrowid

    def get_all_experiments(self, limit: int = 100) -> List[Dict[str, Any]]:
        """Retrieves experiment run history."""
        with self.get_connection() as conn:
            cur = conn.execute("""
            SELECT id, timestamp, dataset_name, database_name, workload_type, workload_name,
                   num_transactions, execution_time_ms, throughput_ops, query_latency_ms, details_json
            FROM experiments
            ORDER BY id DESC
            LIMIT ?
            """, (limit,))
            rows = []
            for r in cur.fetchall():
                d = dict(r)
                try:
                    d["details"] = json.loads(d.get("details_json") or "{}")
                except Exception:
                    d["details"] = {}
                rows.append(d)
            return rows

    def get_comparative_summary(self) -> Dict[str, Any]:
        """
        Calculates comparative metrics between InfluxDB and Neo4j for Dashboard & Results views.
        """
        with self.get_connection() as conn:
            cur = conn.execute("""
            SELECT database_name, workload_type, workload_name,
                   COUNT(*) as run_count,
                   ROUND(AVG(execution_time_ms), 2) as avg_exec_time_ms,
                   ROUND(AVG(throughput_ops), 2) as avg_throughput,
                   ROUND(AVG(query_latency_ms), 2) as avg_latency_ms,
                   MAX(id) as latest_id
            FROM experiments
            GROUP BY database_name, workload_name
            ORDER BY workload_type, workload_name, database_name
            """)
            summary_rows = [dict(r) for r in cur.fetchall()]

        # Latest ingestion comparison
        ingestion_comp = {"InfluxDB": None, "Neo4j": None}
        with self.get_connection() as conn:
            for db in ["InfluxDB", "Neo4j"]:
                c = conn.execute("""
                SELECT throughput_ops, execution_time_ms, num_transactions, timestamp
                FROM experiments
                WHERE database_name = ? AND workload_type = 'INGESTION'
                ORDER BY id DESC LIMIT 1
                """, (db,))
                row = c.fetchone()
                if row:
                    ingestion_comp[db] = dict(row)

        return {
            "summary_rows": summary_rows,
            "latest_ingestion": ingestion_comp
        }

    def clear_experiments(self):
        """Clears all records in experiments table."""
        with self.get_connection() as conn:
            conn.execute("DELETE FROM experiments")
            conn.commit()

    def run_automated_benchmark(
        self,
        database: str = "BOTH",
        max_records: Optional[int] = 5000,
        batch_size: int = 1000
    ) -> Dict[str, Any]:
        """
        Executes an end-to-end benchmark suite using the Elliptic Bitcoin dataset across:
        1. Batch Ingestion
        2. Temporal Workloads (Time Step Aggregation, Illicit Trends, Feature Outliers)
        3. Graph Workloads (Degree Fan-Out, Multi-Hop Tracing, Illicit Contagion)
        """
        results: Dict[str, Any] = {"runs": []}
        targets = ["InfluxDB", "Neo4j"] if database.upper() == "BOTH" else [database]

        inf = InfluxService()
        neo = Neo4jService()

        for target in targets:
            # 1. Ingestion
            if target == "InfluxDB":
                ing_res = inf.ingest_transactions(batch_size=batch_size, max_records=max_records)
            else:
                ing_res = neo.ingest_transactions(batch_size=batch_size, max_records=max_records)

            if ing_res.get("success"):
                run_id = self.log_experiment(
                    database_name=target,
                    workload_type="INGESTION",
                    workload_name="Batch Ingestion",
                    num_transactions=ing_res["records_ingested"],
                    execution_time_ms=ing_res["execution_time_ms"],
                    throughput_ops=ing_res["throughput_ops"],
                    query_latency_ms=ing_res["execution_time_ms"],
                    details={"batch_size": batch_size, "records": ing_res["records_ingested"]}
                )
                results["runs"].append({"id": run_id, "db": target, "workload": "Batch Ingestion", "throughput": ing_res["throughput_ops"]})

            # 2. Temporal Workload: Volume by Time Step
            if target == "InfluxDB":
                t_vol = inf.query_count_by_timestep()
            else:
                t_vol = neo.query_count_by_timestep()

            run_id = self.log_experiment(
                database_name=target,
                workload_type="TEMPORAL",
                workload_name="Volume by Time Step",
                num_transactions=t_vol["results_count"],
                execution_time_ms=t_vol["latency_ms"],
                throughput_ops=round(1000.0 / max(0.1, t_vol["latency_ms"]), 2),
                query_latency_ms=t_vol["latency_ms"],
                details={"found": t_vol["results_count"]}
            )
            results["runs"].append({"id": run_id, "db": target, "workload": "Volume by Time Step", "latency_ms": t_vol["latency_ms"]})

            # 3. Temporal Workload: Illicit Trend
            if target == "InfluxDB":
                t_illicit = inf.query_illicit_distribution()
            else:
                t_illicit = neo.query_illicit_distribution()

            run_id = self.log_experiment(
                database_name=target,
                workload_type="TEMPORAL",
                workload_name="Illicit Trend Analysis",
                num_transactions=t_illicit["results_count"],
                execution_time_ms=t_illicit["latency_ms"],
                throughput_ops=round(1000.0 / max(0.1, t_illicit["latency_ms"]), 2),
                query_latency_ms=t_illicit["latency_ms"],
                details={"found": t_illicit["results_count"]}
            )
            results["runs"].append({"id": run_id, "db": target, "workload": "Illicit Trend Analysis", "latency_ms": t_illicit["latency_ms"]})

            # 4. Temporal Workload: Outlier Threshold
            if target == "InfluxDB":
                t_outlier = inf.query_feature_threshold(threshold=1.5)
            else:
                t_outlier = neo.query_feature_threshold(threshold=1.5)

            run_id = self.log_experiment(
                database_name=target,
                workload_type="TEMPORAL",
                workload_name="Outlier Feature Threshold",
                num_transactions=t_outlier["results_count"],
                execution_time_ms=t_outlier["latency_ms"],
                throughput_ops=round(1000.0 / max(0.1, t_outlier["latency_ms"]), 2),
                query_latency_ms=t_outlier["latency_ms"],
                details={"found": t_outlier["results_count"]}
            )
            results["runs"].append({"id": run_id, "db": target, "workload": "Outlier Feature Threshold", "latency_ms": t_outlier["latency_ms"]})

            # 5. Graph Workload: Multi-Hop Tracing
            if target == "InfluxDB":
                g_mh = inf.query_graph_simulation()
            else:
                g_mh = neo.query_multi_hop()

            run_id = self.log_experiment(
                database_name=target,
                workload_type="GRAPH",
                workload_name="Multi-Hop Flow Tracing",
                num_transactions=g_mh["results_count"],
                execution_time_ms=g_mh["latency_ms"],
                throughput_ops=round(1000.0 / max(0.1, g_mh["latency_ms"]), 2),
                query_latency_ms=g_mh["latency_ms"],
                details={"found": g_mh["results_count"]}
            )
            results["runs"].append({"id": run_id, "db": target, "workload": "Multi-Hop Flow Tracing", "latency_ms": g_mh["latency_ms"]})

            # 6. Graph Workload: Illicit Contagion / Neighborhood
            if target == "InfluxDB":
                g_cont = inf.query_feature_threshold(threshold=2.5)  # Influx equivalent high-risk feature search
            else:
                g_cont = neo.query_illicit_neighborhood()

            run_id = self.log_experiment(
                database_name=target,
                workload_type="GRAPH",
                workload_name="Illicit Neighborhood Contagion",
                num_transactions=g_cont["results_count"],
                execution_time_ms=g_cont["latency_ms"],
                throughput_ops=round(1000.0 / max(0.1, g_cont["latency_ms"]), 2),
                query_latency_ms=g_cont["latency_ms"],
                details={"found": g_cont["results_count"]}
            )
            results["runs"].append({"id": run_id, "db": target, "workload": "Illicit Neighborhood Contagion", "latency_ms": g_cont["latency_ms"]})

        return results
