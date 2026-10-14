# Open Source Contribution Candidates & Upstream Proposals (`OSS_CONTRIBUTION.md`)

**Course:** CSE412 — Big Data & Large-Scale Computing  
**Project:** CrisisGuard AI  
**Scope:** 4 Technical Candidate Issues, Verified Upstream Links, and Concrete Draft Diffs  

> **Course Policy Requirement Note**: In accordance with course evaluation guidelines, open-source software contributions earn extra credit **ONLY if the pull request is officially reviewed, accepted, and MERGED upstream** into the target codebase. Simply opening a pull request, filing an issue, or drafting a diff does not earn credit.

During the architecture, deployment, and stress-testing of CrisisGuard AI across Apache Spark, Hadoop HDFS, and PuLP, our team identified four technical edge cases. Below are the verified upstream links, context, and concrete git diffs prepared for submission.

---

## 1. Apache Spark MLlib: Sparse Vector Handling in `StandardScaler(withMean=True)`

### Upstream Verification & Links
- **Repository:** Apache Spark (`apache/spark`)
- **Verified Tracker:** [Apache Spark JIRA (MLlib)](https://issues.apache.org/jira/browse/SPARK)
- **Module:** `mllib` / `pyspark.ml.feature`
- **Component:** `org.apache.spark.ml.feature.StandardScaler`

### Context & Problem Statement
When assembling numeric stylometric features with TF-IDF features using `VectorAssembler`, attempting to center dense features with `StandardScaler(withMean=True)` throws a runtime exception if applied after `VectorAssembler` has joined dense and sparse features into a single vector column:
```
IllegalArgumentException: StandardScaler does not support withMean=true for sparse vectors.
```
While mathematically valid (centering destroys vector sparsity), the documentation in `docs/ml-classification-regression.md` does not explain this interaction in combined multi-modal pipelines, nor does `StandardScaler` provide a `handleSparse="keep_uncentered"` fallback mode. Developers are forced to manually separate and scale features before assembly.

### Concrete Draft Diff
```diff
--- a/mllib/src/main/scala/org/apache/spark/ml/feature/StandardScaler.scala
+++ b/mllib/src/main/scala/org/apache/spark/ml/feature/StandardScaler.scala
@@ -72,6 +72,12 @@ class StandardScaler(override val uid: String)
   def setWithMean(value: Boolean): this.type = set(withMean, value)
 
+  /**
+   * Param for handling sparse vectors when withMean=true.
+   * Options: 'error' (default), 'ignoreMean' (center only dense components).
+   */
+  final val handleSparse: Param[String] = new Param[String](this, "handleSparse",
+    "How to handle sparse vectors with withMean: error or ignoreMean",
+    ParamValidators.inArray(Array("error", "ignoreMean")))
+
   override def fit(dataset: Dataset[_]): StandardScalerModel = {
     transformSchema(dataset.schema, logging = true)
     val input = dataset.select($(inputCol)).rdd.map {
```

---

## 2. Apache Hadoop HDFS: Unsanitized Partition Path Error Diagnostics

### Upstream Verification & Links
- **Repository:** Apache Hadoop (`apache/hadoop`)
- **Verified Tracker:** [Apache Hadoop JIRA (HDFS)](https://issues.apache.org/jira/browse/HDFS)
- **Module:** `hadoop-hdfs-project`
- **Component:** `org.apache.hadoop.hdfs.server.namenode.INodeDirectory`

### Context & Problem Statement
When ingesting crowdsourced crisis text into partitioned HDFS Parquet storage (`hdfs://127.0.0.1:9000/disaster_data/silver_real/`), posts containing unescaped newline characters or URL control symbols in dynamic partition columns triggered a NameNode crash:
```
org.apache.hadoop.ipc.RemoteException: PathComponentTooLongException: The maximum path component length was exceeded
```
The error failed to log the problematic row index, partition key value, or byte offset, making debugging distributed writes across 100k records unnecessarily difficult.

### Concrete Draft Diff
```diff
--- a/hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/namenode/INodeDirectory.java
+++ b/hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/namenode/INodeDirectory.java
@@ -310,7 +310,8 @@ public class INodeDirectory extends INodeWithAdditionalFields
     if (bytes.length > maxComponentLength) {
-      throw new PathComponentTooLongException("The maximum path component length was exceeded: "
-          + bytes.length + " > " + maxComponentLength);
+      String preview = DFSUtil.bytes2String(bytes, 0, Math.min(bytes.length, 64));
+      throw new PathComponentTooLongException(String.format(
+          "Path component length %d exceeds max %d. Offending prefix: '%s'",
+          bytes.length, maxComponentLength, preview));
     }
```

---

## 3. PuLP: Automatic Solver Binary Resolution on Windows / WSL

### Upstream Verification & Links
- **Repository:** COIN-OR PuLP (`coin-or/pulp`)
- **Verified Tracker:** [GitHub coin-or/pulp Issues](https://github.com/coin-or/pulp/issues)
- **Module:** `pulp.apis.coin_api`
- **Component:** `PULP_CBC_CMD` & `COIN_CMD`

### Context & Problem Statement
In PuLP 2.8+ on Python 3.11+, using `pulp.PULP_CBC_CMD(msg=False)` triggers deprecation warnings:
```
DeprecationWarning: PULP_CBC_CMD is deprecated and will be removed in PuLP 4.0. Install CBC with `pip install pulp[cbc]` and use COIN_CMD instead.
```
However, on Windows developer machines where `pulp[cbc]` wheel compilation fails without MSVC C++ tools, `COIN_CMD` fails to locate the system `cbc.exe` unless it is explicitly specified via an absolute path, whereas the deprecated `PULP_CBC_CMD` located it reliably via Windows PATH search.

### Concrete Draft Diff
```diff
--- a/pulp/apis/coin_api.py
+++ b/pulp/apis/coin_api.py
@@ -102,6 +102,12 @@ class COIN_CMD(LpSolver_CMD):
         if self.path is None:
             self.path = self.find_binary("cbc")
+            if self.path is None and sys.platform.startswith("win"):
+                # Check standard Windows Chocolatey, Conda, or local program files
+                fallback_paths = ["C:\\ProgramData\\chocolatey\\bin\\cbc.exe", shutil.which("cbc.exe")]
+                for p in fallback_paths:
+                    if p and os.path.exists(p):
+                        self.path = p
+                        break
         if self.path is None:
             raise PulpSolverError("Cannot find COIN-OR CBC solver binary in system PATH.")
```

---

## 4. Hugging Face Datasets: Community Loader for QCRI HumAID Benchmark

### Upstream Verification & Links
- **Repository:** Hugging Face Datasets (`huggingface/datasets`)
- **Verified Tracker:** [GitHub huggingface/datasets Pull Requests](https://github.com/huggingface/datasets/pulls)
- **Module:** `datasets/loading`

### Context & Problem Statement
QCRI HumAID is one of the standard benchmarks in crisis informatics (84k human-annotated disaster posts across 10 global crises). Currently, researchers must download raw tarballs and write custom CSV loaders because column headers vary across events (`tweet_id`, `tweet_text`, `class_label`). A canonical dataset builder script in Hugging Face Datasets standardizes ingestion for the entire community.

### Concrete Draft Loader Snippet
```python
# datasets/datasets/humaid/humaid.py
import csv
import datasets

class HumAID(datasets.GeneratorBasedBuilder):
    """QCRI HumAID: Human-Annotated Disaster Response Informatics Benchmark."""
    VERSION = datasets.Version("1.0.0")

    def _info(self):
        return datasets.DatasetInfo(
            description="QCRI HumAID Disaster Tweet Benchmark across 10 environmental events.",
            features=datasets.Features({
                "post_id": datasets.Value("string"),
                "text": datasets.Value("string"),
                "event_type": datasets.Value("string"),
                "disaster_type": datasets.Value("string"),
                "humanitarian_category": datasets.Value("string"),
            }),
            supervised_keys=None,
            homepage="https://crisisnlp.qcri.org/humaid_dataset.html",
        )

    def _generate_examples(self, filepath):
        with open(filepath, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for id_, row in enumerate(reader):
                yield id_, {
                    "post_id": row.get("tweet_id") or row.get("post_id"),
                    "text": row.get("tweet_text") or row.get("text"),
                    "event_type": row.get("event_type", "unknown"),
                    "disaster_type": row.get("disaster_type", "general"),
                    "humanitarian_category": row.get("class_label") or row.get("humanitarian_category"),
                }
```

---

## 5. Submission Execution Checklist
- [ ] Student submits PR #1 to `apache/spark` referencing Spark JIRA issue.
- [ ] Student submits PR #2 to `apache/hadoop` NameNode logging component.
- [ ] Student submits PR #3 to `coin-or/pulp` resolving Windows CBC resolution.
- [ ] Student publishes HumAID dataset script to Hugging Face Hub.
- [ ] Team tracks upstream reviewer feedback until pull requests achieve **MERGED** status.
