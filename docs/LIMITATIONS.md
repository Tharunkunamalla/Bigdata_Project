# Engineering Limitations, Assumptions, and Known Trade-Offs (`LIMITATIONS.md`)

**Course:** CSE412 — Big Data & Large-Scale Computing  
**Project:** CrisisGuard AI  
**Scope:** Honest Scoping, Known System Limitations, and Architectural Trade-Offs  
**Release Tag:** `v1.0-final-8`  

---

## 1. Core System Assumptions

1. **Adversarial Generative Model:** We assume automated adversarial hoaxes are generated using Large Language Models (LLMs) or rule-based templates that produce characteristic linguistic signals (elevated urgency claims, specific hazard terms) or rely on copy-paste botnet amplification. If an adversary meticulously fine-tunes an LLM directly on genuine local disaster tweets, text-only detectors may degrade.
2. **Deterministic Vehicle Capacities:** Fleet staging bases (depots) are assumed to have fixed, deterministic capacity during each optimization epoch ($C_j \in [10, 16]$ units). Dynamic vehicle refueling, crew fatigue, and mechanical breakdowns are not currently modeled in the static LP formulation.
3. **Great-Circle Distance Metric:** Transit distances between depots and demand coordinates are calculated using great-circle Haversine geometry with a constant transit penalty ($\lambda = 0.015$).

---

## 2. Technical Limitations & Known Trade-Offs

### 2.1 Driver-Side Mixed-Integer Linear Program (MILP) Solve
- **Trade-Off:** While ingestion, feature engineering, and stream scoring are fully distributed across Spark partitions, the final PuLP MILP optimization is executed on the driver node.
- **Justification:** Combinatorial Mixed-Integer Linear Programs with strict binary constraints ($x_{ij} \in \{0, 1\}$) cannot be decomposed into independent MapReduce stages because depot capacity constraints ($\sum_i x_{ij} \le C_j$) couple all decision variables globally. Spark aggregates and filters demands across partitions, and PuLP solves the resulting compact candidate matrix on the driver in under 100 milliseconds.
- **Boundary:** When scaling to $>10,000$ simultaneous demands, exact Branch-and-Cut becomes computationally intractable on a single thread. In such regimes, regional spatial partitioning must divide the global problem into disjoint sub-regions.

### 2.2 Out-of-Distribution Domain Shift & Generator Attribution
- **Empirical Finding:** As documented in `results/sanity_controls.json`, when a detector trained on Hello-SimpleAI HC3 (academic Q&A) was evaluated directly on held-out emergency hoaxes, accuracy dropped to 0.00%.
- **Hoax Generator Attribution (Item 2):** Generators A, B, and C are our own synthesized rule-based perturbation templates (hazard, infrastructure, and eyewitness distress calls) with slot perturbations. No external LLM API was used. The high ROC-AUC (0.9940) on held-out Gen C is an artifact of template phrasing ("evacuation", "warning", "rescue", "mayday") contrasting against messy social media tweets. In in-the-wild deployment with casual or adversarial styles, detector performance will be substantially lower.

### 2.3 Methodological Limitations & Empirical Disclaimers
1. **Random Split vs. Event Holdout (Optimistic Humanitarian & Urgency Results)**:
   - The primary 75/25 train/test partitioning is a **random stratified split within each dataset source**, not an event-held-out split.
   - Because tweets from the same disaster events appear in both the training set and the test set, the humanitarian categorization (in-distribution Macro-F1 = 0.7639) and urgency regression metrics ($R^2 = 0.4434$, MAE = 0.6624) are **optimistic** and are **not comparable to event-held-out numbers**.
   - Rigorous event-held-out evaluation across 60 HumAID events via 5-fold cross-validation (`results/event_held_out_evaluation.json`) demonstrates that humanitarian category performance drops to **Macro-F1 = 0.5587 ± 0.0631** (a 0.1977 generalization penalty), and urgency $R^2$ drops to **0.3838 ± 0.1672** when facing entirely unseen disaster events.
2. **Unconfirmed Cause for Rumour-Classifier Performance Drop**:
   - The rumour classifier experienced a performance drop when evaluated on multi-source data (CrisisLex + PHEME + LIAR).
   - This performance drop has **no confirmed cause**. While cross-source domain divergence (conversational crisis tweets in PHEME vs formal political claims in LIAR) is suspected (as PHEME alone achieves AUC 0.8620 vs composite 0.8920), no single root cause has been conclusively isolated.
3. **Disaster-Domain Hoax Detector Weakness at Fixed Threshold**:
   - On short crisis tweets (~110 characters), the synthetic text detector produces low raw sigmoid probabilities.
   - At a default fixed threshold ($\theta = 0.50$), the detector misses most short tweet hoaxes. Calibrated validation threshold tuning ($\theta^* = 0.1786$) prevents threshold starvation and yields modest, honest gains (+0.63 victims in Houston, +0.40 in California, +0.63 in Nepal and Miami).
4. **Circularity in Coordination Penalties (Item 3)**:
   - The real coordination graph revealed an edge density of 0.0 among injected hoaxes. Using real graph-derived cluster penalties on hoaxes yields +0.00 incremental gain over continuous trust.
   - The previously reported +1.93 gain came from applying `0.15 if is_fake else 0.0` using ground truth information. This policy is now strictly labeled as **Simulated Coordination (Upper Bound)** and removed from headline claims.
5. **Reconciliation between Real Detector Gain (+0.63) and Sweep (+22.48) (Item 1)**:
   - *Real Detector*: Under authentic HumAID disaster tweets, genuine rescue requests already have high urgency (4.0-5.0), and the depot capacity of 52 is sufficient to serve 28.00 genuine victims under Blind LP. The maximum possible oracle headroom is +24.0 victims. Real Trust-Aware Validation Threshold LP recovers +0.63 victims (2.6% of oracle headroom, $p = 0.00053$).
   - *Parametric Sweep*: In the parametric sweep, hoaxes were assigned an artificial priority advantage (4.5 vs Uniform[2.5, 4.5]), causing Blind LP to collapse down to 29.44. Under this severe urgency crowding, detecting hoaxes at AUC 0.99 restored all 22.48 victims (90.5% of Oracle headroom).
6. **Simulated Social Media Coordinates**:
   - Social media coordinates are simulated on regional bounding boxes (`coords_simulated: true`), because Twitter/X terms strip precise GPS for privacy. Depot locations are authentic facilities from OpenStreetMap Overpass.
7. **Big Data Infrastructure & Hive Status (Item 4, 5, 6)**:
   - HiveServer2 on port 10000 is closed. The catalog is Apache Hive Metastore (Thrift port 9083) queried through Spark SQL.
   - Live stream crash recovery completed in **2.83s** with 0 data loss and 0 duplicates (`results/stream_recovery_evidence.txt`).
   - Forced Parquet disk execution shows Pandas is faster at 1M rows (4.98s vs 16.80s), but Pandas crashes with OOM at 10M rows while Spark succeeds in 12.80s by spilling out-of-core.
