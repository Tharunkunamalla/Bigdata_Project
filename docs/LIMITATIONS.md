# Engineering Limitations, Assumptions, and Known Trade-Offs (`LIMITATIONS.md`)

**Course:** CSE412 — Big Data & Large-Scale Computing  
**Project:** CrisisGuard AI  
**Scope:** Honest Scoping, Known System Limitations, and Architectural Trade-Offs

---

## 1. Core System Assumptions

1. **Adversarial Generative Model:** We assume that automated adversarial hoaxes are generated using Large Language Models (LLMs) that produce characteristic linguistic signals (elevated lexical formality, distinctive punctuation density, and extreme alarmist terminology) or rely on synchronized copy-paste botnet amplification. If an adversary meticulously fine-tunes an LLM directly on genuine local disaster tweets, text-only detectors may degrade.
2. **Deterministic Vehicle Capacities:** Fleet staging bases (depots) are assumed to have fixed, deterministic capacity during each optimization epoch ($C_j \in [10, 20]$ units). Dynamic vehicle refueling, crew fatigue, and mechanical breakdowns are not currently modeled in the static LP formulation.
3. **Great-Circle Distance Metric:** Transit distances between depots and demand coordinates are calculated using great-circle Haversine geometry. We assume straight-line transit times with a constant transit penalty ($\lambda = 0.05$).

---

## 2. Technical Limitations & Known Trade-Offs

### 2.1 Driver-Side Mixed-Integer Linear Program (MILP) Solve
- **Trade-Off:** While ingestion, feature engineering, and stream scoring are fully distributed across Spark partitions, the final PuLP MILP optimization is executed on the driver node.
- **Justification:** Combinatorial Mixed-Integer Linear Programs with strict binary constraints ($x_{ij} \in \{0, 1\}$) cannot be decomposed into independent MapReduce stages because depot capacity constraints ($\sum_i x_{ij} \le C_j$) couple all decision variables globally. Spark aggregates and filters demands across partitions, and PuLP solves the resulting compact candidate matrix on the driver in under 100 milliseconds.
- **Boundary:** When scaling to $>10,000$ simultaneous demands, exact Branch-and-Cut becomes computationally intractable on a single thread. In such regimes, regional spatial partitioning (e.g., Geo-hashing or Voronoi clustering) must divide the global problem into disjoint sub-regions.

### 2.2 Out-of-Distribution Domain Shift
- **Empirical Finding:** As documented in `results/sanity_controls.json`, when our detector trained on Hello-SimpleAI HC3 (human vs. ChatGPT Q&A) was evaluated directly on held-out emergency hoaxes, accuracy dropped to **0.00%**.
- **Reason:** The HC3 corpus consists of verbose, grammatically pristine paragraphs answering academic questions, whereas crisis hoaxes are terse, urgent tweets with fragmented syntax.
- **Mitigation:** CrisisGuard combines stylometric signals with empirical disaster vocabularies from HumAID and relies on the MinHashLSH coordination graph as a network-level fallback.

### 2.3 Simulated Entities in Live Replay
- **Explicit Declaration:**
  - **Text, Labels & Locations:** 100% authentic, curated from QCRI HumAID, CrisisLex, and HC3.
  - **Live Websocket Stream & Vehicle Movement:** Simulated live playback replay through Kafka topic `disaster_raw_posts`.
  - **Social User Accounts:** Because public disaster datasets anonymize user IDs for privacy, user account IDs in the coordination graph are simulated approximations mapped onto authentic HumAID tweet texts and hashtags.

### 2.4 Physical Routing & Road Network Inundation
- **Limitation:** In actual flash floods (e.g., Hurricane Harvey in Houston), road segments and bayous become impassable. Haversine distance underestimates true travel times when bridges are washed out.
- **Future Direction:** Integrating OpenStreetMap (OSM) road networks with USGS river gauge sensors to compute real-time dynamic routing impedances.

### 2.5 Monolingual Linguistic Boundary
- **Limitation:** The current Spark MLlib TF-IDF and stylometric models are trained exclusively on English crisis corpora.
- **Impact:** Deploying CrisisGuard in non-English disaster theaters (e.g., Turkey earthquakes or Latin American cyclones) requires replacing unigram TF-IDF with multilingual sentence embeddings (e.g., XLM-RoBERTa or multilingual BERT).

### 2.6 Human-in-the-Loop Operational Scoping
- **Safety Boundary:** CrisisGuard AI is explicitly designed as an **emergency decision-support triaging tool** for human emergency managers, **not an autonomous physical actuator**.
- **Ethical Directive:** First responders must never automatically withhold life-saving aid from a citizen based solely on an algorithmic score. Algorithmic priority triages dispatch order during severe resource contention.
