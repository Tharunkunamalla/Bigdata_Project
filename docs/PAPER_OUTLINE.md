# Academic Paper Outline (IEEE / ACM Format)

**Title:** CrisisGuard: Mitigating Adversarial Generative AI Misinformation in Real-Time Emergency Resource Allocation via Distributed Big Data Pipelines  
**Target Venue:** IEEE Transactions on Big Data (TBD) / ACM SIGKDD Applied Data Science Track / ISCRAM (Information Systems for Crisis Response and Management)  
**Authors:** Team of 3 Authors (Department of Computer Science & Engineering)

---

## Abstract
Crowdsourced crisis informatics systems rely on incoming social media text to allocate physical emergency resources during natural disasters. With the advent of Large Language Models (LLMs), automated adversaries can synthesize high-urgency fictitious distress calls ("emergency hoaxes") at unprecedented scale, subverting classical dispatch optimizers and exhausting scarce rescue fleets away from genuine victims. 
In this paper, we present **CrisisGuard**, a distributed big data analytics and optimization architecture that immunizes emergency dispatch against adversarial synthetic text in real time. Deployed across Apache Kafka, Hadoop HDFS, Apache Hive Metastore, and Apache Spark MLlib, CrisisGuard streams high-velocity alert feeds, computes continuous veracity scores via ElasticNet text classifiers and MinHash Locality-Sensitive Hashing (LSH) coordination graphs, and solves a Trust-Aware Mixed-Integer Linear Program (MILP). 
Evaluated on an authentic corpus of **98,944 disaster records** (QCRI CrisisBench, HumAID, CrisisLex, and HC3) across 30 random seeds in four disaster theaters, our trust-aware formulation increased genuine victims rescued from $40.20 \pm 0.94$ to $51.80 \pm 0.15$ in Houston marine operations ($p = 1.45 \times 10^{-20}$) while reducing wasted hoax dispatches from 22.69% to 0.38%. In stress tests with crash injection at 100,000 events, the system demonstrated zero data loss and recovered within 7.72 seconds.

---

## Section Outline & Structure

### Section 1: Introduction & Related Work
- **1.1 The Emergency Allocation Dilemma:** Scarcity of physical resources (boats, fire units, ambulances) during catastrophic floods and earthquakes.
- **1.2 The Generative AI Threat:** How LLM text generation lowers the cost of fabricating plausible, urgent distress calls.
- **1.3 Prior Art:**
  - *Crisis Informatics:* Imran et al. (QCRI), Olteanu et al. (CrisisLex).
  - *Synthetic Text Detection:* Solaiman et al., Mitchell et al. (DetectGPT).
  - *Emergency Vehicle Routing:* Dynamic Dial-a-Ride and Facility Location Problems.
- **1.4 Core Contributions:**
  1. An authentic 130,301-record disaster corpus with zero-leakage random stratified splits and held-out event evaluation.
  2. Spark MLlib inference with anti-leakage sanity controls (shuffled-label validation).
  3. MinHashLSH coordination graph discovering 32.7x higher amplification among hoaxes.
  4. Multi-region Trust-Aware MILP formulation validated across 30 seeds with $p < 10^{-20}$ significance.
  5. 100,000-event streaming fault-tolerance and 1M-row scaling benchmarks.

### Section 2: Adversarial Threat Model & Problem Formulation
- **2.1 Attacker Capabilities:** Automated bots injecting fabricated distress tweets with extreme urgency claims ($U_i \to 1.0$).
- **2.2 Baseline Formulation Failure:** Proving why classical distance-and-urgency MIP objective functions ($\max \sum (U_i - \lambda D_{ij}) x_{ij}$) fail under adversarial contamination.
- **2.3 Proposed Trust-Aware MILP:**
  $$\max_{x_{ij}} \sum_{i=1}^{N} \sum_{j=1}^{M} \left[ U_i \cdot T_i - \lambda \left( \frac{D_{ij}}{100} \right) \right] x_{ij}$$
  subject to capacity and single-dispatch constraints.

### Section 3: Distributed System Architecture
- **3.1 Ingestion Layer:** Apache Kafka 3.7.0 (KRaft broker) handling partitioned alert topics.
- **3.2 Distributed Storage Layer:** Hadoop HDFS 3.3.6 storing silver Parquet blocks with atomic directory checkpointing.
- **3.3 Metadata Catalog:** Apache Hive Metastore 3.1.3 Thrift service exposing relational schemas.
- **3.4 Execution Engine:** Apache Spark 3.5.1 Structured Streaming and MLlib for distributed classification.
- **3.5 Solver Engine:** PuLP Branch-and-Cut solver executed on the driver node after distributed partition aggregation.

### Section 4: Empirical Data Curation & Spark MLlib Inference
- **4.1 Data Provenance:** Curation of QCRI HumAID, CrisisLex, and Hello-SimpleAI HC3.
- **4.2 Zero-Leakage Event Stratification:** Train on Hurricane Harvey & Kerala Floods (26,350 rows); test on Nepal Earthquake & California Wildfires (72,594 rows) with 0% text overlap.
- **4.3 Model Performance:**
  - Synthetic Detector: ROC-AUC = 0.7081, PR-AUC = 0.1585, Accuracy = 96.43%.
  - Rumour Classifier: ROC-AUC = 0.6785, F1 = 0.9036.
  - Humanitarian Classifier: Macro-F1 = 0.5994.
  - Urgency Regressor: RMSE = 1.2277.
- **4.4 Sanity Controls & Anti-Leakage Audit:**
  - Shuffled-label control yielding 0.5557 AUC (verifying zero latent leakage).
  - Out-of-distribution domain shift analysis (HC3 long-form Q&A to short crisis alerts yielding 0.00% accuracy).

### Section 5: MinHashLSH Botnet Coordination Graph
- **5.1 Formulation:** 5 hash tables, Jaccard distance threshold $\le 0.35$ over 5,000 TF-IDF features.
- **5.2 Empirical Graph Discovery:** 9,563 text similarity edges and 281,344 hashtag co-occurrence edges across 14,888 nodes.
- **5.3 Amplification Disparity:** Coordinated hoaxes exhibiting an average degree of 60.82 vs 1.86 for genuine citizens (32.74x amplification ratio).

### Section 6: Multi-Region Emergency Allocation Experiments
- **6.1 Multi-Theater Evaluation:** Comparative results across Houston (Boats), California (Wildfire), Nepal (SAR), and Miami (Ambulance).
- **6.2 Statistical Hypothesis Testing:** Paired t-tests ($t = 23.77, p = 1.45 \times 10^{-20}$).
- **6.3 Sensitivity Analysis:** Hoax contamination sweep (5% to 40%), proving gains scale to +46.93 genuine victims saved under high attack intensity.

### Section 7: Fault Tolerance & Scaling Benchmarks
- **7.1 100k Stream Crash-Recovery Test:** Ingestion of 100,000 events with hard driver kill at event 60,000; verified 0 events lost, 0 duplicates, and 7.72s recovery window.
- **7.2 Scalability Benchmark:** Honest empirical comparison of Pandas vs Spark across 10k, 100k, 500k, and 1,000,000 rows.

### Section 8: Discussion, Ethical Boundaries & Limitations
- **8.1 Human-in-the-Loop Triaging:** Why automated physical dispatch must be advisory to human incident commanders.
- **8.2 Adversarial Robustness:** Countering stylometric mimicry via graph coordination fallback.
- **8.3 Physical Routing:** From Haversine great-circle distances to inundated road network graphs.

### Section 9: Conclusion & Open Source Artifacts
- **9.1 Summary:** Closing remarks on reproducible big data pipelines for public safety.
- **9.2 Reproducibility Checklist:** Data repository URLs, HDFS setup scripts, and automated demo suite.

---

## Target References
1. F. Imran et al., "HumAID: Human-Annotated Disaster Dataset for Humanitarian Assistance," *Proc. ACM Web Conf.*, 2022.
2. A. Olteanu et al., "CrisisLex: A Lexicon for Collecting and Filtering Microblogged Information on Social Media," *ICWSM*, 2014.
3. B. Guo et al., "How Close is ChatGPT to Human Experts? Comparison Corpus (HC3)," *arXiv:2301.07597*, 2023.
4. M. Zaharia et al., "Apache Spark: A Unified Engine for Big Data Processing," *CACM*, 2016.
5. S. Mitchell et al., "DetectGPT: Zero-Shot Machine-Generated Text Detection using Probability Curvature," *ICML*, 2023.
