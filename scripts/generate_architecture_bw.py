import matplotlib.pyplot as plt
import matplotlib.patches as patches

# Set up IEEE publication-style high-resolution figure (Grayscale / Pure Black and White)
plt.rcParams['font.sans-serif'] = 'DejaVu Sans'
plt.rcParams['font.size'] = 9

fig, ax = plt.subplots(figsize=(12, 7.5), dpi=300)
ax.set_facecolor('white')
fig.patch.set_facecolor('white')
ax.set_xlim(0, 100)
ax.set_ylim(0, 100)
ax.axis('off')

# Box drawing helper
def draw_box(x, y, w, h, title, details=[], style='solid', lw=1.2, fill='white'):
    box = patches.FancyBboxPatch(
        (x, y), w, h,
        boxstyle="round,pad=0.5,rounding_size=1.2",
        linewidth=lw,
        linestyle=style,
        edgecolor='black',
        facecolor=fill
    )
    ax.add_patch(box)
    
    # Title
    ax.text(x + w/2, y + h - 2.8, title, ha='center', va='center',
            fontsize=9.5, fontweight='bold', color='black')
    
    # Details text
    if details:
        y_offset = y + h - 5.5
        spacing = (h - 6.0) / max(len(details), 1)
        for line in details:
            ax.text(x + w/2, y_offset, line, ha='center', va='center',
                    fontsize=8, color='black')
            y_offset -= spacing

# Helper to draw arrows
def draw_arrow(x1, y1, x2, y2, label="", label_pos=(0.5, 0.5), style='solid'):
    ax.annotate(
        '', xy=(x2, y2), xytext=(x1, y1),
        arrowprops=dict(facecolor='black', edgecolor='black', arrowstyle='-|>',
                        mutation_scale=12, lw=1.2, linestyle=style)
    )
    if label:
        lx = x1 + (x2 - x1) * label_pos[0]
        ly = y1 + (y2 - y1) * label_pos[1]
        ax.text(lx, ly, label, ha='center', va='center', fontsize=7.5,
                bbox=dict(boxstyle='square,pad=0.2', facecolor='white', edgecolor='none'),
                color='black')

# 1. Ingestion Layer
draw_box(4, 73, 24, 20, "1. Ingestion & Sources", [
    "Crisis Datasets (130,301 Records)",
    "HumAID, CrisisLex, PHEME, HC3",
    "Kafka Producer Replay Feed",
    "Kafka Broker (Port 9092, KRaft)",
    "Topic: 'disaster_raw_posts'"
])

# 2. Storage & Catalog
draw_box(4, 25, 24, 20, "2. Distributed Storage", [
    "Hadoop HDFS 3.3.6 (Port 9000)",
    "Silver Layer Partitioned Parquet",
    "Path: /user/hive/warehouse/...",
    "Apache Hive Metastore 3.1.3",
    "Thrift Service (Port 9083)"
])

# 3. Stream Engine & MLlib (Center)
draw_box(38, 55, 28, 40, "3. Spark Structured Streaming", [
    "Micro-batch Engine (Trigger: 1.0s)",
    "Driver: PySpark local[2] (v3.5.1)",
    "---------------------------------------",
    "[MLlib Inference Pipelines]",
    "* Synthetic Detector (AUC: 0.994)",
    "* Rumour Classifier (AUC: 0.892)",
    "* Humanitarian Categorizer (F1: 0.764)",
    "* Urgency Regressor (R2: 0.443)",
    "---------------------------------------",
    "MinHashLSH Coordination Graph",
    "Cluster Penalty: Delta T(Si)",
    "HDFS Checkpointing (/checkpoints/)"
])

# 4. Message Egress (Center bottom)
draw_box(38, 25, 28, 14, "4. Scored Event Buffer", [
    "Kafka Topic: 'disaster_scored_posts'",
    "Enriched Alert Tuples (Urgency, Veracity)",
    "Sustained 2,420 events/sec Throughput"
])

# 5. Optimization & MILP Solver (Right top)
draw_box(73, 58, 23, 27, "5. Emergency Allocation", [
    "FastAPI Gateway (Port 8000)",
    "OSM Overpass Authentic Depots",
    "(Boats, Engines, Ambulances)",
    "PuLP MILP Solver (Coin-OR CBC)",
    "Objective: Max Sum (Ui * Ti - Lambda * Dij)",
    "Trust-Aware Dispatch Plan"
])

# 6. Operations UI Dashboard (Right bottom)
draw_box(73, 15, 23, 27, "6. Operations Dashboard", [
    "React 18 / Vite Single Page App",
    "Leaflet GIS Map & Demand Clusters",
    "Real-Time Server-Sent Events (SSE)",
    "Model ROC/PR Curves & Metrics",
    "Streaming Pipeline Health Monitors",
    "URL: http://127.0.0.1:8000/"
])

# Connectors
# 1 -> 3
draw_arrow(28, 83, 38, 83, label="Alert Stream")
# 2 -> 3
draw_arrow(28, 38, 38, 62, label="Batch Training & Schemas")
# 3 -> 4
draw_arrow(52, 55, 52, 39, label="Scored Tuples")
# 4 -> 5
draw_arrow(66, 32, 73, 68, label="Enriched Feeds")
# 5 -> 6
draw_arrow(84.5, 58, 84.5, 42, label="Optimal Schedules")

# Add Section Label
plt.title("Fig. 1. CrisisGuard AI: Distributed System Architecture and Dataflow Pipeline",
          fontsize=10.5, fontweight='bold', pad=15, color='black')

plt.tight_layout()
output_path = "docs/architecture_diagram_bw.png"
plt.savefig(output_path, bbox_inches='tight', dpi=300)
print(f"Saved black and white architecture diagram to {output_path}")
