import csv
import sys
import time
from typing import Dict, List, Any, Optional
from pathlib import Path

# Suppress broken environment pandas binary incompatibility for neo4j optional import
try:
    import pandas
except Exception:
    sys.modules["pandas"] = None

from neo4j import GraphDatabase

from config import (
    NEO4J_URI, NEO4J_USER, NEO4J_PASSWORD, NEO4J_DATABASE,
    PROCESSED_TXS_FILE, PROCESSED_EDGES_FILE
)


class Neo4jService:
    def __init__(self, uri: str = NEO4J_URI, user: str = NEO4J_USER, password: str = NEO4J_PASSWORD, database: str = NEO4J_DATABASE):
        self.uri = uri
        self.user = user
        self.password = password
        self.database = database
        self.driver = None

    def get_driver(self):
        if self.driver is None:
            self.driver = GraphDatabase.driver(self.uri, auth=(self.user, self.password))
        return self.driver

    def close(self):
        if self.driver:
            self.driver.close()
            self.driver = None

    def test_connection(self) -> Dict[str, Any]:
        """Tests connectivity to Neo4j instance and gets server info."""
        start_t = time.time()
        try:
            driver = self.get_driver()
            driver.verify_connectivity()
            with driver.session(database=self.database) as session:
                result = session.run("CALL dbms.components() YIELD name, versions, edition RETURN name, versions[0] AS version, edition")
                records = list(result)
                record = records[0] if records else None
                version = record["version"] if record else "Unknown"
                edition = record["edition"] if record else "Community"
                latency_ms = round((time.time() - start_t) * 1000, 2)
                return {
                    "connected": True,
                    "status": "Healthy",
                    "version": version,
                    "edition": edition,
                    "message": f"Connected to Neo4j {edition} {version}",
                    "latency_ms": latency_ms,
                    "uri": self.uri
                }
        except Exception as e:
            latency_ms = round((time.time() - start_t) * 1000, 2)
            return {
                "connected": False,
                "status": "Error",
                "message": str(e),
                "latency_ms": latency_ms,
                "uri": self.uri
            }

    def init_schema(self) -> Dict[str, Any]:
        """Creates unique constraints and indexes for Transaction nodes."""
        start_t = time.time()
        statements = [
            "CREATE CONSTRAINT transaction_id_unique IF NOT EXISTS FOR (t:Transaction) REQUIRE t.tx_id IS UNIQUE",
            "CREATE INDEX transaction_timestep IF NOT EXISTS FOR (t:Transaction) ON (t.time_step)",
            "CREATE INDEX transaction_class IF NOT EXISTS FOR (t:Transaction) ON (t.class)"
        ]
        driver = self.get_driver()
        with driver.session(database=self.database) as session:
            for stmt in statements:
                try:
                    session.run(stmt)
                except Exception:
                    pass

        latency_ms = round((time.time() - start_t) * 1000, 2)
        return {"success": True, "message": "Neo4j constraints and indexes ensured", "latency_ms": latency_ms}

    def get_transaction_count(self) -> Dict[str, int]:
        """Returns the total number of transactions (nodes) and directed relationships."""
        query = """
        CALL () {
            MATCH (t:Transaction) RETURN count(t) AS tx_count
        }
        CALL () {
            MATCH ()-[r:FLOWS_TO]->() RETURN count(r) AS flows_count
        }
        RETURN tx_count, flows_count
        """
        try:
            driver = self.get_driver()
            with driver.session(database=self.database) as session:
                res = session.run(query).single()
                if res:
                    return {
                        "transactions": res["tx_count"] or 0,
                        "relationships": res["flows_count"] or 0
                    }
        except Exception:
            pass
        return {"transactions": 0, "relationships": 0}

    def ingest_transactions(
        self,
        tx_csv_path: Optional[Path] = None,
        edge_csv_path: Optional[Path] = None,
        batch_size: int = 1000,
        max_records: Optional[int] = None
    ) -> Dict[str, Any]:
        """
        Streams Elliptic transactions and directed edges into Neo4j using parameterized UNWIND batches.
        Creates (:Transaction) nodes and [:FLOWS_TO] relationships.
        """
        if tx_csv_path is None:
            tx_csv_path = PROCESSED_TXS_FILE
        if edge_csv_path is None:
            edge_csv_path = PROCESSED_EDGES_FILE

        tx_csv_path = Path(tx_csv_path)
        edge_csv_path = Path(edge_csv_path)

        if not tx_csv_path.exists():
            return {
                "success": False,
                "error": f"Transactions CSV not found: {tx_csv_path}",
                "records_ingested": 0,
                "execution_time_ms": 0.0,
                "throughput_ops": 0.0
            }

        self.init_schema()

        cypher_nodes = """
        UNWIND $batch AS row
        MERGE (t:Transaction {tx_id: toInteger(row.tx_id)})
        SET t.time_step = toInteger(row.time_step),
            t.class = row.class,
            t.feat_1 = toFloat(row.feat_1),
            t.feat_2 = toFloat(row.feat_2),
            t.feat_3 = toFloat(row.feat_3),
            t.feat_4 = toFloat(row.feat_4),
            t.feat_5 = toFloat(row.feat_5)
        """

        cypher_edges = """
        UNWIND $batch AS row
        MATCH (t1:Transaction {tx_id: toInteger(row.tx_id_1)})
        MATCH (t2:Transaction {tx_id: toInteger(row.tx_id_2)})
        CREATE (t1)-[:FLOWS_TO]->(t2)
        """

        start_time = time.time()
        total_tx = 0
        total_edges = 0
        node_batch: List[Dict[str, Any]] = []

        driver = self.get_driver()
        with driver.session(database=self.database) as session:
            # 1. Ingest Transaction Nodes
            with open(tx_csv_path, "r", encoding="utf-8") as f:
                reader = csv.DictReader(f)
                for idx, row in enumerate(reader):
                    if max_records and idx >= max_records:
                        break
                    node_batch.append({
                        "tx_id": row["tx_id"],
                        "time_step": row.get("time_step", 1),
                        "class": row.get("class", "unknown"),
                        "feat_1": row.get("feat_1", 0.0),
                        "feat_2": row.get("feat_2", 0.0),
                        "feat_3": row.get("feat_3", 0.0),
                        "feat_4": row.get("feat_4", 0.0),
                        "feat_5": row.get("feat_5", 0.0)
                    })
                    if len(node_batch) >= batch_size:
                        session.run(cypher_nodes, batch=node_batch)
                        total_tx += len(node_batch)
                        node_batch = []

                if node_batch:
                    session.run(cypher_nodes, batch=node_batch)
                    total_tx += len(node_batch)

            # 2. Ingest Directed Edges (if edge CSV exists)
            if edge_csv_path.exists():
                edge_batch: List[Dict[str, Any]] = []
                with open(edge_csv_path, "r", encoding="utf-8") as f:
                    reader = csv.DictReader(f)
                    for idx, row in enumerate(reader):
                        if max_records and idx >= max_records:
                            break
                        edge_batch.append({
                            "tx_id_1": row["tx_id_1"],
                            "tx_id_2": row["tx_id_2"]
                        })
                        if len(edge_batch) >= batch_size:
                            session.run(cypher_edges, batch=edge_batch)
                            total_edges += len(edge_batch)
                            edge_batch = []

                    if edge_batch:
                        session.run(cypher_edges, batch=edge_batch)
                        total_edges += len(edge_batch)

        elapsed = time.time() - start_time
        total_records = total_tx + total_edges
        throughput = round(total_records / elapsed, 2) if elapsed > 0 else 0.0
        elapsed_ms = round(elapsed * 1000, 2)

        return {
            "success": True,
            "database": "Neo4j",
            "records_ingested": total_records,
            "transactions_ingested": total_tx,
            "edges_ingested": total_edges,
            "execution_time_ms": elapsed_ms,
            "execution_time_sec": round(elapsed, 4),
            "throughput_ops": throughput,
            "batch_size": batch_size
        }

    # Graph Workload 1: 1-Hop Neighbor / Degree Analysis (High Fan-Out)
    def query_degree_analysis(self, threshold: int = 2, limit: int = 50) -> Dict[str, Any]:
        """
        Finds transactions with out-degree >= threshold (high fan-out money distribution).
        """
        cypher = """
        MATCH (t:Transaction)-[:FLOWS_TO]->(m:Transaction)
        WITH t, count(m) AS out_degree
        WHERE out_degree >= $threshold
        RETURN t.tx_id AS tx_id, t.time_step AS time_step, t.class AS class, out_degree
        ORDER BY out_degree DESC
        LIMIT $limit
        """
        start_t = time.time()
        driver = self.get_driver()
        results = []
        with driver.session(database=self.database) as session:
            res = session.run(cypher, threshold=threshold, limit=limit)
            for record in res:
                results.append({
                    "tx_id": record["tx_id"],
                    "time_step": record["time_step"],
                    "class": record["class"],
                    "out_degree": record["out_degree"]
                })

        latency_ms = round((time.time() - start_t) * 1000, 2)
        return {
            "database": "Neo4j",
            "workload": "Graph - 1-Hop Degree Fan-Out",
            "latency_ms": latency_ms,
            "results_count": len(results),
            "results": results
        }

    # Graph Workload 2: Multi-Hop Money Flow Tracing (2-hop / 3-hop Chains)
    def query_multi_hop(self, limit: int = 50) -> Dict[str, Any]:
        """
        Traverses multi-hop transaction forwarding chains: (t1) -> (t2) -> (t3).
        Demonstrates Neo4j's index-free adjacency graph traversal efficiency.
        """
        cypher = """
        MATCH path = (t1:Transaction)-[:FLOWS_TO]->(t2:Transaction)-[:FLOWS_TO]->(t3:Transaction)
        WHERE t1 <> t2 AND t2 <> t3 AND t1 <> t3
        RETURN t1.tx_id AS source,
               t2.tx_id AS intermediary,
               t3.tx_id AS destination,
               t1.class AS source_class,
               t2.class AS inter_class,
               t3.class AS dest_class
        LIMIT $limit
        """
        start_t = time.time()
        driver = self.get_driver()
        results = []
        with driver.session(database=self.database) as session:
            res = session.run(cypher, limit=limit)
            for record in res:
                results.append({
                    "source": record["source"],
                    "intermediary": record["intermediary"],
                    "destination": record["destination"],
                    "source_class": record["source_class"],
                    "inter_class": record["inter_class"],
                    "dest_class": record["dest_class"]
                })

        latency_ms = round((time.time() - start_t) * 1000, 2)
        return {
            "database": "Neo4j",
            "workload": "Graph - Multi-Hop Tracing",
            "latency_ms": latency_ms,
            "results_count": len(results),
            "results": results
        }

    # Graph Workload 3: Illicit Neighborhood / Contagion Analysis
    def query_illicit_neighborhood(self, limit: int = 50) -> Dict[str, Any]:
        """
        Finds immediate downstream receivers of funds from known illicit transactions (class='1').
        """
        cypher = """
        MATCH (t:Transaction {class: '1'})-[:FLOWS_TO]->(downstream:Transaction)
        RETURN t.tx_id AS illicit_tx,
               t.time_step AS illicit_timestep,
               downstream.tx_id AS downstream_tx,
               downstream.class AS downstream_class,
               downstream.time_step AS downstream_timestep
        LIMIT $limit
        """
        start_t = time.time()
        driver = self.get_driver()
        results = []
        with driver.session(database=self.database) as session:
            res = session.run(cypher, limit=limit)
            for record in res:
                results.append({
                    "illicit_tx": record["illicit_tx"],
                    "illicit_timestep": record["illicit_timestep"],
                    "downstream_tx": record["downstream_tx"],
                    "downstream_class": record["downstream_class"],
                    "downstream_timestep": record["downstream_timestep"]
                })

        latency_ms = round((time.time() - start_t) * 1000, 2)
        return {
            "database": "Neo4j",
            "workload": "Graph - Illicit Neighborhood Contagion",
            "latency_ms": latency_ms,
            "results_count": len(results),
            "results": results
        }

    # Graph Workload 4: Convergent / Diamond Pattern Analysis
    def query_convergent_patterns(self, limit: int = 50) -> Dict[str, Any]:
        """
        Detects split-and-merge patterns: (a) flows to (b) and (c), which both flow into (d).
        """
        cypher = """
        MATCH (a:Transaction)-[:FLOWS_TO]->(b:Transaction)-[:FLOWS_TO]->(d:Transaction),
              (a)-[:FLOWS_TO]->(c:Transaction)-[:FLOWS_TO]->(d)
        WHERE b <> c
        RETURN a.tx_id AS source,
               b.tx_id AS path_1,
               c.tx_id AS path_2,
               d.tx_id AS destination
        LIMIT $limit
        """
        start_t = time.time()
        driver = self.get_driver()
        results = []
        with driver.session(database=self.database) as session:
            res = session.run(cypher, limit=limit)
            for record in res:
                results.append({
                    "source": record["source"],
                    "path_1": record["path_1"],
                    "path_2": record["path_2"],
                    "destination": record["destination"]
                })

        latency_ms = round((time.time() - start_t) * 1000, 2)
        return {
            "database": "Neo4j",
            "workload": "Graph - Convergent Flow Patterns",
            "latency_ms": latency_ms,
            "results_count": len(results),
            "results": results
        }

    # -------------------------------------------------------------
    # Temporal Cypher Workloads (Equivalent to InfluxDB for Direct Evaluation)
    # -------------------------------------------------------------

    def query_count_by_timestep(self) -> Dict[str, Any]:
        """
        Aggregates transaction counts across discrete time steps in Neo4j.
        """
        cypher = """
        MATCH (t:Transaction)
        RETURN t.time_step AS time_step, count(t) AS count
        ORDER BY time_step
        """
        start_t = time.time()
        driver = self.get_driver()
        results = []
        with driver.session(database=self.database) as session:
            res = session.run(cypher)
            for record in res:
                results.append({
                    "time_step": record["time_step"],
                    "count": record["count"]
                })

        latency_ms = round((time.time() - start_t) * 1000, 2)
        return {
            "database": "Neo4j",
            "workload": "Temporal - Volume by Time Step",
            "latency_ms": latency_ms,
            "results_count": len(results),
            "results": results
        }

    def query_illicit_distribution(self) -> Dict[str, Any]:
        """
        Aggregates count of illicit transactions (class='1') per time step in Neo4j.
        """
        cypher = """
        MATCH (t:Transaction {class: '1'})
        RETURN t.time_step AS time_step, count(t) AS illicit_count
        ORDER BY time_step
        """
        start_t = time.time()
        driver = self.get_driver()
        results = []
        with driver.session(database=self.database) as session:
            res = session.run(cypher)
            for record in res:
                results.append({
                    "time_step": record["time_step"],
                    "illicit_count": record["illicit_count"]
                })

        latency_ms = round((time.time() - start_t) * 1000, 2)
        return {
            "database": "Neo4j",
            "workload": "Temporal - Illicit Trend",
            "latency_ms": latency_ms,
            "results_count": len(results),
            "results": results
        }

    def query_feature_aggregation(self) -> Dict[str, Any]:
        """
        Computes mean of feat_1 across time steps in Neo4j.
        """
        cypher = """
        MATCH (t:Transaction)
        RETURN t.time_step AS time_step, round(avg(t.feat_1), 4) AS avg_feature_val
        ORDER BY time_step
        """
        start_t = time.time()
        driver = self.get_driver()
        results = []
        with driver.session(database=self.database) as session:
            res = session.run(cypher)
            for record in res:
                results.append({
                    "time_step": record["time_step"],
                    "avg_feature_val": record["avg_feature_val"]
                })

        latency_ms = round((time.time() - start_t) * 1000, 2)
        return {
            "database": "Neo4j",
            "workload": "Temporal - Feature Aggregation",
            "latency_ms": latency_ms,
            "results_count": len(results),
            "results": results
        }

    def query_feature_threshold(self, threshold: float = 2.0, limit: int = 50) -> Dict[str, Any]:
        """
        Outlier transaction search in Neo4j where feat_1 >= threshold.
        """
        cypher = """
        MATCH (t:Transaction)
        WHERE t.feat_1 >= $threshold
        RETURN t.time_step AS time_step, t.class AS class, t.feat_1 AS feat_1
        ORDER BY t.feat_1 DESC
        LIMIT $limit
        """
        start_t = time.time()
        driver = self.get_driver()
        results = []
        with driver.session(database=self.database) as session:
            res = session.run(cypher, threshold=threshold, limit=limit)
            for record in res:
                results.append({
                    "time_step": record["time_step"],
                    "class": record["class"],
                    "feat_1": record["feat_1"]
                })

        latency_ms = round((time.time() - start_t) * 1000, 2)
        return {
            "database": "Neo4j",
            "workload": "Temporal - Outlier Threshold",
            "latency_ms": latency_ms,
            "results_count": len(results),
            "results": results
        }

    def clear_database(self) -> Dict[str, Any]:
        """Deletes all nodes and relationships in the database."""
        start_t = time.time()
        driver = self.get_driver()
        try:
            with driver.session(database=self.database) as session:
                session.run("MATCH (n) DETACH DELETE n")
            latency_ms = round((time.time() - start_t) * 1000, 2)
            return {"success": True, "message": "Neo4j graph cleared", "latency_ms": latency_ms}
        except Exception as e:
            return {"success": False, "error": str(e)}
