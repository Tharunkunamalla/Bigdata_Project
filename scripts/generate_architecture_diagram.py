"""
scripts/generate_architecture_diagram.py
========================================
Generates a publication-quality architecture diagram for CrisisGuard AI
displaying the complete 5-technology pipeline:
Kafka (9092) -> Spark Streaming -> HDFS Parquet (9000) -> Hive Metastore (9083) / Spark SQL -> PuLP MILP -> React Dashboard
Saves to docs/architecture_diagram.png
"""

import os
import matplotlib.pyplot as plt
import matplotlib.patches as patches

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DOCS_DIR = os.path.join(BASE_DIR, "docs")
os.makedirs(DOCS_DIR, exist_ok=True)
OUT_PATH = os.path.join(DOCS_DIR, "architecture_diagram.png")

def generate_diagram():
    fig, ax = plt.subplots(figsize=(14, 8), dpi=300)
    ax.set_xlim(0, 14)
    ax.set_ylim(0, 8)
    ax.axis("off")

    # Background
    fig.patch.set_facecolor('#0f172a')
    ax.set_facecolor('#0f172a')

    # Color Palette
    c_blue = '#38bdf8'
    c_green = '#4ade80'
    c_purple = '#c084fc'
    c_amber = '#fbbf24'
    c_red = '#f87171'
    c_box = '#1e293b'
    c_border = '#334155'
    c_text = '#f8fafc'
    c_sub = '#94a3b8'

    # Title
    ax.text(7, 7.5, "CrisisGuard AI: End-to-End Distributed Analytics Architecture",
            fontsize=18, fontweight='bold', color=c_text, ha='center', va='center')
    ax.text(7, 7.15, "Real-Time Detection of Synthetic Misinformation & Multi-Depot Fleet Optimization (CSE412)",
            fontsize=11, color=c_sub, ha='center', va='center')

    # Component Box Helper
    def draw_box(x, y, w, h, title, subtitle, port="", tech="", badge_color=c_blue):
        rect = patches.FancyBboxPatch((x, y), w, h, boxstyle="round,pad=0.1,rounding_size=0.15",
                                      facecolor=c_box, edgecolor=c_border, linewidth=1.5)
        ax.add_patch(rect)
        # Header / Port
        if port:
            ax.text(x + w - 0.15, y + h - 0.25, port, fontsize=8, color=badge_color, fontweight='bold', ha='right', va='top')
        if tech:
            ax.text(x + 0.15, y + h - 0.25, tech, fontsize=8, color=badge_color, fontweight='bold', ha='left', va='top')
        # Title
        ax.text(x + w/2, y + h/2 + 0.15, title, fontsize=12, fontweight='bold', color=c_text, ha='center', va='center')
        # Subtitle
        ax.text(x + w/2, y + h/2 - 0.25, subtitle, fontsize=8.5, color=c_sub, ha='center', va='center')

    # Layer 1: Ingestion
    draw_box(0.6, 4.4, 2.5, 1.8, "Apache Kafka", "KRaft Broker (Topic: disaster_raw_posts)\n2,420 events/sec sustained", "Port: 9092", "INGESTION", c_amber)
    draw_box(0.6, 1.8, 2.5, 1.8, "Raw Data Sources", "HumAID (84k) • CrisisLex (8.7k)\nHC3 AI QA • Hazard Hoax Gen", "99,568 Rows", "PROVENANCE", c_blue)

    # Layer 2: Stream Processing & MLlib
    draw_box(4.0, 4.4, 3.0, 1.8, "Spark Structured Streaming", "Micro-Batch (1.0s Trigger)\nDynamic Tokenizer & Scaler\n1.18s End-to-End Latency", "Engine", "PROCESSING", c_green)
    draw_box(4.0, 1.8, 3.0, 1.8, "Spark MLlib Classifiers", "Synthetic Detector (ROC 0.9155)\nRumour RF • Category LR\nMinHashLSH Similarity Graph", "Local[2]", "ANALYTICS", c_purple)

    # Layer 3: Storage & Metastore
    draw_box(7.9, 4.4, 2.7, 1.8, "Apache Hadoop HDFS", "Distributed NameNode & DataNode\nPartitioned Silver Parquet Blocks\nZero-Loss Write-Ahead Checkpoints", "Port: 9000", "STORAGE", c_amber)
    draw_box(7.9, 1.8, 2.7, 1.8, "Hive Metastore Catalog", "Thrift Catalog queried via Spark SQL\nTable: crisis_silver_real (93k rows)\nPartition Pruned Querying", "Port: 9083", "CATALOG", c_blue)

    # Layer 4: Optimization & Serving
    draw_box(11.3, 4.4, 2.3, 1.8, "FastAPI Backend", "Async REST Endpoints & SSE Stream\nCluster Health & Metrics APIs\nDriver Orchestrator", "Port: 8000", "SERVING", c_green)
    draw_box(11.3, 1.8, 2.3, 1.8, "PuLP MILP Solver & UI", "Trust-Aware Fleet Optimization\nReact 18 / Vite Live Map\n-35.7% Hoax Dispatches", "Port: 5173", "DISPATCH", c_red)

    # Arrow connections
    def draw_arrow(x1, y1, x2, y2, label=""):
        ax.annotate('', xy=(x2, y2), xytext=(x1, y1),
                    arrowprops=dict(arrowstyle="->", color=c_blue, lw=1.8, shrinkA=3, shrinkB=3))
        if label:
            mx, my = (x1 + x2) / 2, (y1 + y2) / 2
            ax.text(mx, my + 0.12, label, fontsize=7.5, color=c_text, ha='center', va='bottom',
                    bbox=dict(boxstyle="round,pad=0.2", facecolor=c_box, edgecolor="none", alpha=0.8))

    # Flow arrows
    draw_arrow(1.85, 3.6, 1.85, 4.4, "Replay Stream")
    draw_arrow(3.1, 5.3, 4.0, 5.3, "Consume Feed")
    draw_arrow(5.5, 4.4, 5.5, 3.6, "Featurize")
    draw_arrow(7.0, 5.3, 7.9, 5.3, "Append Parquet")
    draw_arrow(7.0, 2.7, 7.9, 2.7, "Scored Data")
    draw_arrow(9.25, 4.4, 9.25, 3.6, "Schema Sync")
    draw_arrow(10.6, 5.3, 11.3, 5.3, "Telemetry")
    draw_arrow(10.6, 2.7, 11.3, 2.7, "Filtered Demands")
    draw_arrow(12.45, 4.4, 12.45, 3.6, "Solve Dispatch")

    plt.tight_layout()
    plt.savefig(OUT_PATH, facecolor=fig.get_facecolor(), edgecolor='none')
    plt.close()
    print(f"[OK] Saved architecture diagram to {OUT_PATH}")

if __name__ == "__main__":
    generate_diagram()
