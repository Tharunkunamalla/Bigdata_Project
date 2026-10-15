"""
scripts/tune_disaster_threshold.py
==================================
Tunes the disaster-domain operating threshold strictly on the training validation split
for disaster tweets & hoaxes (HumAID, CrisisLex, Hoax Generator A).
"""
import os
import json
import numpy as np
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.ml import Pipeline
from pyspark.ml.feature import RegexTokenizer, StopWordsRemover, HashingTF, IDF, VectorAssembler, StandardScaler
from pyspark.ml.classification import LogisticRegression
from sklearn.metrics import f1_score, precision_score, recall_score

def tune():
    spark = SparkSession.builder.master("local[2]").appName("TuneDisasterThreshold").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    from ml.train_models_real import add_computed_stylometrics

    curated_dir = os.path.join("data", "curated")
    train_raw = spark.read.parquet(os.path.join(curated_dir, "train_real.parquet"))
    train_df = add_computed_stylometrics(train_raw).cache()
    train_sub, val_df = train_df.randomSplit([0.80, 0.20], seed=412)

    disaster_sources = ["HumAID_CrisisBench", "CrisisLex_DisasterTweets", "hoax_generator_a_train"]
    val_disaster = val_df.filter(F.col("source").isin(disaster_sources)).cache()
    print(f"Disaster validation samples: {val_disaster.count():,}")

    tokenizer = RegexTokenizer(inputCol="text", outputCol="words", pattern="\\W+")
    remover = StopWordsRemover(inputCol="words", outputCol="filtered_words")
    hashing_tf = HashingTF(inputCol="filtered_words", outputCol="raw_tf", numFeatures=2000)
    idf = IDF(inputCol="raw_tf", outputCol="tfidf_features")
    stylo_cols = ["char_len", "word_count", "avg_word_len", "uppercase_ratio", "punct_ratio", "digit_ratio", "lexical_diversity"]
    num_assembler = VectorAssembler(inputCols=stylo_cols, outputCol="raw_stylometrics", handleInvalid="keep")
    scaler = StandardScaler(inputCol="raw_stylometrics", outputCol="scaled_stylometrics", withStd=True, withMean=True)
    final_assembler = VectorAssembler(inputCols=["tfidf_features", "scaled_stylometrics"], outputCol="features")
    lr_synth = LogisticRegression(featuresCol="features", labelCol="is_synthetic", probabilityCol="prob_synthetic", rawPredictionCol="raw_synthetic", predictionCol="pred_synthetic", maxIter=25, regParam=0.01, elasticNetParam=0.0)

    pipe = Pipeline(stages=[tokenizer, remover, hashing_tf, idf, num_assembler, scaler, final_assembler, lr_synth])
    model = pipe.fit(train_sub)
    preds_val = model.transform(val_disaster).cache()

    p_val = np.array([float(r[0][1]) for r in preds_val.select("prob_synthetic").collect()])
    y_val = np.array([int(r[0]) for r in preds_val.select("is_synthetic").collect()])

    print(f"y_val == 1 count: {np.sum(y_val):,}, y_val == 0 count: {len(y_val) - np.sum(y_val):,}")
    p_pos = p_val[y_val == 1]
    p_neg = p_val[y_val == 0]
    print(f"p_val (y=1): min={np.min(p_pos):.4f}, 25%={np.percentile(p_pos, 25):.4f}, median={np.median(p_pos):.4f}, 75%={np.percentile(p_pos, 75):.4f}, max={np.max(p_pos):.4f}")
    print(f"p_val (y=0): min={np.min(p_neg):.4f}, 25%={np.percentile(p_neg, 25):.4f}, median={np.median(p_neg,):.4f}, 75%={np.percentile(p_neg, 75):.4f}, 99%={np.percentile(p_neg, 99):.4f}, max={np.max(p_neg):.4f}")

    best_th, best_f1 = 0.5, 0.0
    for th in np.concatenate([np.linspace(0.001, 0.05, 50), np.linspace(0.05, 0.95, 100)]):
        pred = (p_val >= th).astype(int)
        f = f1_score(y_val, pred, zero_division=0)
        if f > best_f1:
            best_f1, best_th = f, th

    best_th = round(float(best_th), 4)
    prec = round(float(precision_score(y_val, (p_val >= best_th).astype(int), zero_division=0)), 4)
    rec = round(float(recall_score(y_val, (p_val >= best_th).astype(int), zero_division=0)), 4)
    best_f1 = round(float(best_f1), 4)

    print(f"[RESULT] Disaster Domain Validation Threshold: theta_disaster* = {best_th}")
    print(f"         Validation F1 = {best_f1}, Precision = {prec}, Recall = {rec}")

    spark.stop()
    return best_th, best_f1, prec, rec

if __name__ == "__main__":
    tune()
