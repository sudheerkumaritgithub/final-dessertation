import os
from pathlib import Path
from flask import Flask, render_template, request, jsonify, redirect, url_for

from config import (
    SECRET_KEY, FLASK_PORT, FLASK_DEBUG, DEFAULT_BATCH_SIZE, DEFAULT_SUBSET_SIZE
)
from dataset_loader import (
    get_dataset_summary, preview_transactions, preview_relationships, preprocess_elliptic_dataset
)
from influx_service import InfluxService
from neo4j_service import Neo4jService
from benchmark_service import BenchmarkService

app = Flask(__name__)
app.config["SECRET_KEY"] = SECRET_KEY

influx_svc = InfluxService()
neo4j_svc = Neo4jService()
benchmark_svc = BenchmarkService()


# -------------------------------------------------------------
# Frontend Page Routes
# -------------------------------------------------------------

@app.route("/")
def index():
    return redirect(url_for("dashboard"))


@app.route("/dashboard")
def dashboard():
    inf_status = influx_svc.test_connection()
    neo_status = neo4j_svc.test_connection()
    dataset_summary = get_dataset_summary()

    inf_count = influx_svc.get_transaction_count() if inf_status.get("connected") else 0
    neo_counts = neo4j_svc.get_transaction_count() if neo_status.get("connected") else {"transactions": 0, "relationships": 0}

    comparative = benchmark_svc.get_comparative_summary()
    recent_experiments = benchmark_svc.get_all_experiments(limit=8)

    return render_template(
        "dashboard.html",
        active_page="dashboard",
        inf_status=inf_status,
        neo_status=neo_status,
        dataset=dataset_summary,
        inf_count=inf_count,
        neo_counts=neo_counts,
        comparative=comparative,
        recent_experiments=recent_experiments
    )


@app.route("/dataset")
def dataset_page():
    dataset_summary = get_dataset_summary()
    tx_preview = preview_transactions(limit=20, offset=0)
    edge_preview = preview_relationships(limit=20, offset=0)
    return render_template(
        "dataset.html",
        active_page="dataset",
        dataset=dataset_summary,
        tx_preview=tx_preview,
        edge_preview=edge_preview
    )


@app.route("/experiments")
def experiments_page():
    inf_status = influx_svc.test_connection()
    neo_status = neo4j_svc.test_connection()
    dataset_summary = get_dataset_summary()

    return render_template(
        "experiments.html",
        active_page="experiments",
        inf_status=inf_status,
        neo_status=neo_status,
        dataset=dataset_summary,
        default_batch=DEFAULT_BATCH_SIZE,
        default_subset=DEFAULT_SUBSET_SIZE
    )


@app.route("/results")
def results_page():
    experiments = benchmark_svc.get_all_experiments(limit=100)
    comparative = benchmark_svc.get_comparative_summary()
    return render_template(
        "results.html",
        active_page="results",
        experiments=experiments,
        comparative=comparative
    )


# -------------------------------------------------------------
# REST API Endpoints
# -------------------------------------------------------------

@app.route("/api/db-status", methods=["GET"])
def api_db_status():
    return jsonify({
        "influxdb": influx_svc.test_connection(),
        "neo4j": neo4j_svc.test_connection()
    })


@app.route("/api/preprocess-dataset", methods=["POST"])
def api_preprocess_dataset():
    data = request.get_json() or {}
    force = bool(data.get("force", True))
    features = int(data.get("features", 15))
    res = preprocess_elliptic_dataset(feature_count=features, force=force)
    return jsonify({"success": True, "metadata": res})


@app.route("/api/dataset-preview", methods=["GET"])
def api_dataset_preview():
    kind = request.args.get("kind", "transactions")
    limit = int(request.args.get("limit", 20))
    offset = int(request.args.get("offset", 0))

    if kind == "relationships":
        preview = preview_relationships(limit=limit, offset=offset)
    else:
        preview = preview_transactions(limit=limit, offset=offset)

    return jsonify(preview)


@app.route("/api/run-ingestion", methods=["POST"])
def api_run_ingestion():
    data = request.get_json() or {}
    target_db = data.get("database", "BOTH").upper()
    batch_size = int(data.get("batch_size", DEFAULT_BATCH_SIZE))
    max_records = int(data.get("max_records", 0)) or None

    results = {}
    if target_db in ["INFLUXDB", "BOTH"]:
        res_inf = influx_svc.ingest_transactions(batch_size=batch_size, max_records=max_records)
        if res_inf.get("success"):
            benchmark_svc.log_experiment(
                database_name="InfluxDB",
                workload_type="INGESTION",
                workload_name="Batch Ingestion",
                num_transactions=res_inf["records_ingested"],
                execution_time_ms=res_inf["execution_time_ms"],
                throughput_ops=res_inf["throughput_ops"],
                query_latency_ms=res_inf["execution_time_ms"],
                details={"batch_size": batch_size, "records": res_inf["records_ingested"]}
            )
        results["InfluxDB"] = res_inf

    if target_db in ["NEO4J", "BOTH"]:
        res_neo = neo4j_svc.ingest_transactions(batch_size=batch_size, max_records=max_records)
        if res_neo.get("success"):
            benchmark_svc.log_experiment(
                database_name="Neo4j",
                workload_type="INGESTION",
                workload_name="Batch Ingestion",
                num_transactions=res_neo["records_ingested"],
                execution_time_ms=res_neo["execution_time_ms"],
                throughput_ops=res_neo["throughput_ops"],
                query_latency_ms=res_neo["execution_time_ms"],
                details={"batch_size": batch_size, "records": res_neo["records_ingested"]}
            )
        results["Neo4j"] = res_neo

    return jsonify({"success": True, "results": results})


@app.route("/api/run-temporal", methods=["POST"])
def api_run_temporal():
    data = request.get_json() or {}
    target_db = data.get("database", "BOTH").upper()
    workload = data.get("workload", "ALL").upper()
    threshold = float(data.get("threshold", 1.5))

    output = {}

    # 1. Volume by Time Step
    if workload in ["VOLUME_BY_STEP", "ALL"]:
        vol_out = {}
        if target_db in ["INFLUXDB", "BOTH"]:
            inf_res = influx_svc.query_count_by_timestep()
            benchmark_svc.log_experiment(
                database_name="InfluxDB",
                workload_type="TEMPORAL",
                workload_name="Volume by Time Step",
                num_transactions=inf_res["results_count"],
                execution_time_ms=inf_res["latency_ms"],
                throughput_ops=round(1000.0 / max(0.1, inf_res["latency_ms"]), 2),
                query_latency_ms=inf_res["latency_ms"]
            )
            vol_out["InfluxDB"] = inf_res

        if target_db in ["NEO4J", "BOTH"]:
            neo_res = neo4j_svc.query_count_by_timestep()
            benchmark_svc.log_experiment(
                database_name="Neo4j",
                workload_type="TEMPORAL",
                workload_name="Volume by Time Step",
                num_transactions=neo_res["results_count"],
                execution_time_ms=neo_res["latency_ms"],
                throughput_ops=round(1000.0 / max(0.1, neo_res["latency_ms"]), 2),
                query_latency_ms=neo_res["latency_ms"]
            )
            vol_out["Neo4j"] = neo_res
        output["volume_by_step"] = vol_out

    # 2. Illicit Trend
    if workload in ["ILLICIT_TREND", "ALL"]:
        ill_out = {}
        if target_db in ["INFLUXDB", "BOTH"]:
            inf_res = influx_svc.query_illicit_distribution()
            benchmark_svc.log_experiment(
                database_name="InfluxDB",
                workload_type="TEMPORAL",
                workload_name="Illicit Trend Analysis",
                num_transactions=inf_res["results_count"],
                execution_time_ms=inf_res["latency_ms"],
                throughput_ops=round(1000.0 / max(0.1, inf_res["latency_ms"]), 2),
                query_latency_ms=inf_res["latency_ms"]
            )
            ill_out["InfluxDB"] = inf_res

        if target_db in ["NEO4J", "BOTH"]:
            neo_res = neo4j_svc.query_illicit_distribution()
            benchmark_svc.log_experiment(
                database_name="Neo4j",
                workload_type="TEMPORAL",
                workload_name="Illicit Trend Analysis",
                num_transactions=neo_res["results_count"],
                execution_time_ms=neo_res["latency_ms"],
                throughput_ops=round(1000.0 / max(0.1, neo_res["latency_ms"]), 2),
                query_latency_ms=neo_res["latency_ms"]
            )
            ill_out["Neo4j"] = neo_res
        output["illicit_trend"] = ill_out

    # 3. Outlier Feature Threshold
    if workload in ["FEATURE_OUTLIER", "ALL"]:
        feat_out = {}
        if target_db in ["INFLUXDB", "BOTH"]:
            inf_res = influx_svc.query_feature_threshold(threshold=threshold)
            benchmark_svc.log_experiment(
                database_name="InfluxDB",
                workload_type="TEMPORAL",
                workload_name="Outlier Feature Threshold",
                num_transactions=inf_res["results_count"],
                execution_time_ms=inf_res["latency_ms"],
                throughput_ops=round(1000.0 / max(0.1, inf_res["latency_ms"]), 2),
                query_latency_ms=inf_res["latency_ms"],
                details={"threshold": threshold}
            )
            feat_out["InfluxDB"] = inf_res

        if target_db in ["NEO4J", "BOTH"]:
            neo_res = neo4j_svc.query_feature_threshold(threshold=threshold)
            benchmark_svc.log_experiment(
                database_name="Neo4j",
                workload_type="TEMPORAL",
                workload_name="Outlier Feature Threshold",
                num_transactions=neo_res["results_count"],
                execution_time_ms=neo_res["latency_ms"],
                throughput_ops=round(1000.0 / max(0.1, neo_res["latency_ms"]), 2),
                query_latency_ms=neo_res["latency_ms"],
                details={"threshold": threshold}
            )
            feat_out["Neo4j"] = neo_res
        output["feature_outlier"] = feat_out

    return jsonify({"success": True, "results": output})


@app.route("/api/run-graph", methods=["POST"])
def api_run_graph():
    data = request.get_json() or {}
    target_db = data.get("database", "BOTH").upper()
    workload = data.get("workload", "ALL").upper()
    limit = int(data.get("limit", 50))

    output = {}

    # 1. Multi-Hop Money Flow
    if workload in ["MULTI_HOP", "ALL"]:
        mh_out = {}
        if target_db in ["INFLUXDB", "BOTH"]:
            inf_res = influx_svc.query_graph_simulation(limit=limit)
            benchmark_svc.log_experiment(
                database_name="InfluxDB",
                workload_type="GRAPH",
                workload_name="Multi-Hop Flow Tracing",
                num_transactions=inf_res["results_count"],
                execution_time_ms=inf_res["latency_ms"],
                throughput_ops=round(1000.0 / max(0.1, inf_res["latency_ms"]), 2),
                query_latency_ms=inf_res["latency_ms"]
            )
            mh_out["InfluxDB"] = inf_res

        if target_db in ["NEO4J", "BOTH"]:
            neo_res = neo4j_svc.query_multi_hop(limit=limit)
            benchmark_svc.log_experiment(
                database_name="Neo4j",
                workload_type="GRAPH",
                workload_name="Multi-Hop Flow Tracing",
                num_transactions=neo_res["results_count"],
                execution_time_ms=neo_res["latency_ms"],
                throughput_ops=round(1000.0 / max(0.1, neo_res["latency_ms"]), 2),
                query_latency_ms=neo_res["latency_ms"]
            )
            mh_out["Neo4j"] = neo_res
        output["multi_hop"] = mh_out

    # 2. Illicit Neighborhood Contagion
    if workload in ["ILLICIT_CONTAGION", "ALL"]:
        ic_out = {}
        if target_db in ["INFLUXDB", "BOTH"]:
            inf_res = influx_svc.query_feature_threshold(threshold=2.5, limit=limit)
            benchmark_svc.log_experiment(
                database_name="InfluxDB",
                workload_type="GRAPH",
                workload_name="Illicit Neighborhood Contagion",
                num_transactions=inf_res["results_count"],
                execution_time_ms=inf_res["latency_ms"],
                throughput_ops=round(1000.0 / max(0.1, inf_res["latency_ms"]), 2),
                query_latency_ms=inf_res["latency_ms"]
            )
            ic_out["InfluxDB"] = inf_res

        if target_db in ["NEO4J", "BOTH"]:
            neo_res = neo4j_svc.query_illicit_neighborhood(limit=limit)
            benchmark_svc.log_experiment(
                database_name="Neo4j",
                workload_type="GRAPH",
                workload_name="Illicit Neighborhood Contagion",
                num_transactions=neo_res["results_count"],
                execution_time_ms=neo_res["latency_ms"],
                throughput_ops=round(1000.0 / max(0.1, neo_res["latency_ms"]), 2),
                query_latency_ms=neo_res["latency_ms"]
            )
            ic_out["Neo4j"] = neo_res
        output["illicit_contagion"] = ic_out

    # 3. 1-Hop Degree Fan-Out
    if workload in ["DEGREE_FANOUT", "ALL"]:
        df_out = {}
        if target_db in ["NEO4J", "BOTH"]:
            neo_res = neo4j_svc.query_degree_analysis(threshold=2, limit=limit)
            benchmark_svc.log_experiment(
                database_name="Neo4j",
                workload_type="GRAPH",
                workload_name="1-Hop Degree Fan-Out",
                num_transactions=neo_res["results_count"],
                execution_time_ms=neo_res["latency_ms"],
                throughput_ops=round(1000.0 / max(0.1, neo_res["latency_ms"]), 2),
                query_latency_ms=neo_res["latency_ms"]
            )
            df_out["Neo4j"] = neo_res
        output["degree_fanout"] = df_out

    return jsonify({"success": True, "results": output})


@app.route("/api/run-benchmark-suite", methods=["POST"])
def api_run_benchmark_suite():
    data = request.get_json() or {}
    database = data.get("database", "BOTH")
    batch_size = int(data.get("batch_size", DEFAULT_BATCH_SIZE))
    max_records = int(data.get("max_records", 5000))
    res = benchmark_svc.run_automated_benchmark(database=database, max_records=max_records, batch_size=batch_size)
    return jsonify({"success": True, "summary": res})


@app.route("/api/clear-db", methods=["POST"])
def api_clear_db():
    data = request.get_json() or {}
    target_db = data.get("database", "BOTH").upper()
    res = {}
    if target_db in ["INFLUXDB", "BOTH"]:
        res["InfluxDB"] = influx_svc.clear_database()
    if target_db in ["NEO4J", "BOTH"]:
        res["Neo4j"] = neo4j_svc.clear_database()
    return jsonify({"success": True, "results": res})


@app.route("/api/clear-history", methods=["POST"])
def api_clear_history():
    benchmark_svc.clear_experiments()
    return jsonify({"success": True, "message": "Experiment history cleared"})


@app.route("/api/chart-data", methods=["GET"])
def api_chart_data():
    comp = benchmark_svc.get_comparative_summary()
    return jsonify(comp)


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=FLASK_PORT, debug=FLASK_DEBUG)
