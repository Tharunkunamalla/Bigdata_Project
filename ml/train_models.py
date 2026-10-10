"""
CSE412 Big Data Project: Distributed Spark MLlib Training Pipeline
===================================================================
Trains 4 production MLlib models on the crisis dataset using out-of-distribution
event-wise cross-validation:
- Train Split: `hurricane` and `flood` (30,000 records)
- Test Split:  `earthquake` and `wildfire` (30,000 records)

Models Trained:
1. Synthetic Detector (Binary Classifier with calibrated probability)
2. Rumour / Hoax Classifier (Binary Classifier)
3. Humanitarian Need Categorizer (Multi-class Classifier)
4. Urgency Level Regressor (Continuous Regressor)

Exports serialized Spark MLlib PipelineModels to `ml/models/` and evaluation
metrics to `results/metrics.json`.
"""

import os
import json
import time
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.ml import Pipeline
from pyspark.ml.feature import (
    RegexTokenizer, StopWordsRemover, HashingTF, IDF,
    VectorAssembler, StandardScaler, StringIndexer
)
from pyspark.ml.classification import LogisticRegression, RandomForestClassifier
from pyspark.ml.regression import GBTRegressor, LinearRegression
from pyspark.ml.evaluation import (
    BinaryClassificationEvaluator, MulticlassClassificationEvaluator,
    RegressionEvaluator
)

MODEL_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "models")
RESULTS_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "results")
SILVER_DATA_PATH = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data", "curated", "disaster_posts")

os.makedirs(MODEL_DIR, exist_ok=True)
os.makedirs(RESULTS_DIR, exist_ok=True)

def build_spark():
    return (
        SparkSession.builder
        .appName("CSE412_MLlib_Training")
        .master("local[2]")
        .config("spark.driver.memory", "1g")
        .config("spark.executor.memory", "1g")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )

def train_and_evaluate_all():
    start_time = time.time()
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")

    print("==================================================================")
    print("      CSE412 Distributed Spark MLlib Multi-Model Training         ")
    print("==================================================================")

    # 1. Load Silver Dataset
    print(f"Loading silver dataset from: {SILVER_DATA_PATH}")
    df = spark.read.parquet(SILVER_DATA_PATH)
    total_records = df.count()
    print(f"Total records loaded: {total_records:,}")

    # 2. Event-Stratified Train / Test Split (Out-of-Distribution Generalization)
    train_df = df.filter(F.col("event_type").isin("hurricane", "flood")).cache()
    test_df = df.filter(F.col("event_type").isin("earthquake", "wildfire")).cache()
    
    n_train = train_df.count()
    n_test = test_df.count()
    print(f"Train Set (hurricane, flood)    : {n_train:,} records")
    print(f"Test Set  (earthquake, wildfire): {n_test:,} records")

    # -------------------------------------------------------------------------
    # Shared Feature Engineering Pipeline
    # -------------------------------------------------------------------------
    tokenizer = RegexTokenizer(inputCol="text", outputCol="words", pattern="\\W+")
    remover = StopWordsRemover(inputCol="words", outputCol="filtered_words")
    hashing_tf = HashingTF(inputCol="filtered_words", outputCol="raw_tf", numFeatures=5000)
    idf = IDF(inputCol="raw_tf", outputCol="tfidf_features")

    numeric_cols = [
        "char_length", "word_count", "avg_word_length", "uppercase_ratio",
        "exclamation_count", "burstiness_score", "lexical_diversity",
        "user_followers_count", "user_account_age_days", "retweet_count"
    ]
    num_assembler = VectorAssembler(inputCols=numeric_cols, outputCol="raw_num_features", handleInvalid="keep")
    scaler = StandardScaler(inputCol="raw_num_features", outputCol="scaled_num_features", withStd=True, withMean=False)

    final_assembler = VectorAssembler(
        inputCols=["tfidf_features", "scaled_num_features"],
        outputCol="features"
    )

    shared_feature_stages = [tokenizer, remover, hashing_tf, idf, num_assembler, scaler, final_assembler]

    metrics_output = {
        "dataset": {
            "total_records": total_records,
            "train_records": n_train,
            "test_records": n_test,
            "train_events": ["hurricane", "flood"],
            "test_events": ["earthquake", "wildfire"]
        },
        "models": {}
    }

    # -------------------------------------------------------------------------
    # Model 1: Synthetic Misinformation Detector
    # -------------------------------------------------------------------------
    print("\n--- [1/4] Training Synthetic Misinformation Detector ---")
    lr_synthetic = LogisticRegression(
        featuresCol="features",
        labelCol="is_synthetic",
        maxIter=25,
        regParam=0.01,
        elasticNetParam=0.2,
        probabilityCol="prob_synthetic"
    )
    pipeline_synth = Pipeline(stages=shared_feature_stages + [lr_synthetic])
    model_synth = pipeline_synth.fit(train_df)
    preds_synth = model_synth.transform(test_df).cache()

    bin_eval_roc = BinaryClassificationEvaluator(labelCol="is_synthetic", rawPredictionCol="prob_synthetic", metricName="areaUnderROC")
    bin_eval_pr = BinaryClassificationEvaluator(labelCol="is_synthetic", rawPredictionCol="prob_synthetic", metricName="areaUnderPR")
    multi_eval_f1 = MulticlassClassificationEvaluator(labelCol="is_synthetic", predictionCol="prediction", metricName="f1")
    multi_eval_acc = MulticlassClassificationEvaluator(labelCol="is_synthetic", predictionCol="prediction", metricName="accuracy")

    synth_roc = float(bin_eval_roc.evaluate(preds_synth))
    synth_pr = float(bin_eval_pr.evaluate(preds_synth))
    synth_f1 = float(multi_eval_f1.evaluate(preds_synth))
    synth_acc = float(multi_eval_acc.evaluate(preds_synth))

    print(f"Synthetic Detector -> ROC-AUC: {synth_roc:.4f} | PR-AUC: {synth_pr:.4f} | F1: {synth_f1:.4f} | Acc: {synth_acc:.4f}")
    synth_path = os.path.join(MODEL_DIR, "synthetic_detector_model")
    model_synth.write().overwrite().save(synth_path)
    metrics_output["models"]["synthetic_detector"] = {
        "roc_auc": round(synth_roc, 4),
        "pr_auc": round(synth_pr, 4),
        "f1": round(synth_f1, 4),
        "accuracy": round(synth_acc, 4),
        "model_type": "LogisticRegression_ElasticNet",
        "path": synth_path
    }

    # -------------------------------------------------------------------------
    # Model 2: Rumour / Hoax Classifier
    # -------------------------------------------------------------------------
    print("\n--- [2/4] Training Rumour / Hoax Classifier ---")
    rf_rumour = RandomForestClassifier(
        featuresCol="features",
        labelCol="is_rumour",
        numTrees=20,
        maxDepth=7,
        seed=412,
        probabilityCol="prob_rumour"
    )
    pipeline_rumour = Pipeline(stages=shared_feature_stages + [rf_rumour])
    model_rumour = pipeline_rumour.fit(train_df)
    preds_rumour = model_rumour.transform(test_df).cache()

    bin_eval_rumour_roc = BinaryClassificationEvaluator(labelCol="is_rumour", rawPredictionCol="prob_rumour", metricName="areaUnderROC")
    multi_eval_rumour_f1 = MulticlassClassificationEvaluator(labelCol="is_rumour", predictionCol="prediction", metricName="f1")

    rumour_roc = float(bin_eval_rumour_roc.evaluate(preds_rumour))
    rumour_f1 = float(multi_eval_rumour_f1.evaluate(preds_rumour))
    print(f"Rumour Classifier -> ROC-AUC: {rumour_roc:.4f} | F1: {rumour_f1:.4f}")

    rumour_path = os.path.join(MODEL_DIR, "rumour_classifier_model")
    model_rumour.write().overwrite().save(rumour_path)
    metrics_output["models"]["rumour_classifier"] = {
        "roc_auc": round(rumour_roc, 4),
        "f1": round(rumour_f1, 4),
        "model_type": "RandomForestClassifier",
        "path": rumour_path
    }

    # -------------------------------------------------------------------------
    # Model 3: Humanitarian Need Category Classifier (Multi-class)
    # -------------------------------------------------------------------------
    print("\n--- [3/4] Training Humanitarian Need Category Classifier ---")
    cat_indexer = StringIndexer(inputCol="humanitarian_category", outputCol="cat_indexed")
    lr_category = LogisticRegression(
        featuresCol="features",
        labelCol="cat_indexed",
        maxIter=25,
        regParam=0.02,
        family="multinomial"
    )
    pipeline_cat = Pipeline(stages=shared_feature_stages + [cat_indexer, lr_category])
    model_cat = pipeline_cat.fit(train_df)
    preds_cat = model_cat.transform(test_df).cache()

    multi_eval_cat_f1 = MulticlassClassificationEvaluator(labelCol="cat_indexed", predictionCol="prediction", metricName="f1")
    multi_eval_cat_acc = MulticlassClassificationEvaluator(labelCol="cat_indexed", predictionCol="prediction", metricName="accuracy")

    cat_f1 = float(multi_eval_cat_f1.evaluate(preds_cat))
    cat_acc = float(multi_eval_cat_acc.evaluate(preds_cat))
    print(f"Humanitarian Category Classifier -> Macro F1: {cat_f1:.4f} | Accuracy: {cat_acc:.4f}")

    cat_path = os.path.join(MODEL_DIR, "humanitarian_category_model")
    model_cat.write().overwrite().save(cat_path)
    metrics_output["models"]["humanitarian_category"] = {
        "macro_f1": round(cat_f1, 4),
        "accuracy": round(cat_acc, 4),
        "model_type": "MultinomialLogisticRegression",
        "path": cat_path
    }

    # -------------------------------------------------------------------------
    # Model 4: Urgency Level Continuous Regressor
    # -------------------------------------------------------------------------
    print("\n--- [4/4] Training Urgency Level Regressor ---")
    reg_urgency = LinearRegression(
        featuresCol="features",
        labelCol="urgency_level",
        maxIter=25,
        regParam=0.05,
        elasticNetParam=0.1
    )
    pipeline_urgency = Pipeline(stages=shared_feature_stages + [reg_urgency])
    model_urgency = pipeline_urgency.fit(train_df)
    preds_urgency = model_urgency.transform(test_df).cache()

    eval_rmse = RegressionEvaluator(labelCol="urgency_level", predictionCol="prediction", metricName="rmse")
    eval_r2 = RegressionEvaluator(labelCol="urgency_level", predictionCol="prediction", metricName="r2")
    eval_mae = RegressionEvaluator(labelCol="urgency_level", predictionCol="prediction", metricName="mae")

    urg_rmse = float(eval_rmse.evaluate(preds_urgency))
    urg_r2 = float(eval_r2.evaluate(preds_urgency))
    urg_mae = float(eval_mae.evaluate(preds_urgency))
    print(f"Urgency Regressor -> RMSE: {urg_rmse:.4f} | MAE: {urg_mae:.4f} | R2: {urg_r2:.4f}")

    urg_path = os.path.join(MODEL_DIR, "urgency_regressor_model")
    model_urgency.write().overwrite().save(urg_path)
    metrics_output["models"]["urgency_regressor"] = {
        "rmse": round(urg_rmse, 4),
        "mae": round(urg_mae, 4),
        "r2": round(urg_r2, 4),
        "model_type": "LinearRegression_ElasticNet",
        "path": urg_path
    }

    # Write metrics to results/metrics.json
    total_time = round(time.time() - start_time, 2)
    metrics_output["training_time_seconds"] = total_time
    metrics_file = os.path.join(RESULTS_DIR, "metrics.json")
    with open(metrics_file, "w") as f:
        json.dump(metrics_output, f, indent=2)
    print(f"\nSaved evaluation metrics to: {metrics_file}")
    print(f"Total training runtime: {total_time}s")
    print("==================================================================")
    print(">>> PHASE 4 MODEL TRAINING COMPLETED SUCCESSFULLY <<<")
    print("==================================================================")
    spark.stop()

if __name__ == "__main__":
    train_and_evaluate_all()
