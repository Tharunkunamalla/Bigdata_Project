"""
scripts/diagnose_item2.py
========================
Diagnoses Item 2:
1. Samples from Gen A, B, C, Real
2. Top 30 predictive n-grams
3. Leave-one-generator-out evaluation
4. Normalized-text evaluation
5. Length-matched evaluation
6. 20-shuffle control
"""

import re
import os
import json
import numpy as np
import pandas as pd
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import roc_auc_score, average_precision_score, brier_score_loss
from scipy import stats

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(BASE_DIR, "data", "curated")
RESULTS_DIR = os.path.join(BASE_DIR, "results")

# Load data
df_train = pd.read_csv(os.path.join(DATA_DIR, "train_real.csv"), low_memory=False)
df_test = pd.read_csv(os.path.join(DATA_DIR, "test_real.csv"), low_memory=False)

gen_a = df_train[df_train["source"] == "hoax_generator_a"].copy()
gen_b = df_train[df_train["source"] == "hoax_generator_b"].copy()
gen_c = df_test[df_test["source"] == "hoax_generator_c"].copy()
real_train = df_train[df_train["source"].isin(["HumAID_CrisisBench", "CrisisLex_DisasterTweets"])].copy()
real_test = df_test[df_test["source"].isin(["HumAID_CrisisBench", "CrisisLex_DisasterTweets"])].copy()

print("="*60)
print("1. SAMPLES (10 each)")
print("="*60)
print("\n--- Gen A (Authority / Hazard Perturbation) ---")
for s in gen_a["text"].sample(10, random_state=42):
    print(" -", s)

print("\n--- Gen B (Facility / Structural Collapse) ---")
for s in gen_b["text"].sample(10, random_state=42):
    print(" -", s)

print("\n--- Gen C (Civilian Eyewitness Distress) ---")
for s in gen_c["text"].sample(10, random_state=42):
    print(" -", s)

print("\n--- Real Tweets ---")
for s in real_train["text"].sample(10, random_state=42):
    print(" -", s)

# Text normalization function
def normalize_text(text):
    if not isinstance(text, str):
        return ""
    text = text.lower()
    text = re.sub(r"https?://\S+|www\.\S+", "", text)
    text = re.sub(r"[@#]\w+", "", text)  # strip mentions and hashtags
    text = re.sub(r"[\U00010000-\U0010ffff]", "", text)  # strip emoji
    text = re.sub(r"([!?.,;:]){2,}", r"\1", text)  # strip punctuation runs
    text = re.sub(r"\s+", " ", text).strip()
    return text

# Extract top 30 predictive n-grams on train (Gen A+B vs Real Train)
print("\n" + "="*60)
print("2. TOP 30 PREDICTIVE N-GRAMS (Gen A+B vs Real)")
print("="*60)
vec_raw = TfidfVectorizer(ngram_range=(1, 2), max_features=10000)
X_train_raw = vec_raw.fit_transform(pd.concat([gen_a["text"], gen_b["text"], real_train["text"].sample(10000, random_state=42)]))
y_train_raw = np.array([1]*len(gen_a) + [1]*len(gen_b) + [0]*10000)
clf_raw = LogisticRegression(max_iter=1000, C=1.0)
clf_raw.fit(X_train_raw, y_train_raw)

feature_names = np.array(vec_raw.get_feature_names_out())
coefs = clf_raw.coef_[0]
top_hoax_idx = np.argsort(coefs)[-30:][::-1]
print("Top 30 n-grams predictive of Hoax:")
for idx in top_hoax_idx:
    print(f"  {feature_names[idx]}: {coefs[idx]:.4f}")

# 3. Leave-one-generator-out evaluation
print("\n" + "="*60)
print("3. LEAVE-ONE-GENERATOR-OUT EVALUATION")
print("="*60)

def evaluate_split(train_hoaxes, test_hoaxes, train_real_df, test_real_df, normalize=False):
    if normalize:
        train_h_texts = train_hoaxes["text"].apply(normalize_text)
        test_h_texts = test_hoaxes["text"].apply(normalize_text)
        train_r_texts = train_real_df["text"].apply(normalize_text)
        test_r_texts = test_real_df["text"].apply(normalize_text)
    else:
        train_h_texts = train_hoaxes["text"]
        test_h_texts = test_hoaxes["text"]
        train_r_texts = train_real_df["text"]
        test_r_texts = test_real_df["text"]
        
    vec = TfidfVectorizer(ngram_range=(1, 2), max_features=10000)
    train_texts = pd.concat([train_h_texts, train_r_texts])
    train_y = np.array([1]*len(train_h_texts) + [0]*len(train_r_texts))
    
    test_texts = pd.concat([test_h_texts, test_r_texts])
    test_y = np.array([1]*len(test_h_texts) + [0]*len(test_r_texts))
    
    X_tr = vec.fit_transform(train_texts)
    X_te = vec.transform(test_texts)
    
    clf = LogisticRegression(max_iter=1000, C=1.0)
    clf.fit(X_tr, train_y)
    preds = clf.predict_proba(X_te)[:, 1]
    
    auc = roc_auc_score(test_y, preds)
    prauc = average_precision_score(test_y, preds)
    return auc, prauc, clf, vec

# Standard real sample size for train/test to balance execution time
r_tr = real_train.sample(15000, random_state=42)
r_te = real_test.sample(5000, random_state=42)

# Config 1: Train A+B, Test C (Current)
auc_raw_c, prauc_raw_c, _, _ = evaluate_split(pd.concat([gen_a, gen_b]), gen_c, r_tr, r_te, normalize=False)
auc_norm_c, prauc_norm_c, clf_norm, vec_norm = evaluate_split(pd.concat([gen_a, gen_b]), gen_c, r_tr, r_te, normalize=True)
print(f"Train A+B -> Test C: Raw AUC = {auc_raw_c:.4f} (PR: {prauc_raw_c:.4f}) | Normalized AUC = {auc_norm_c:.4f} (PR: {prauc_norm_c:.4f})")

# Config 2: Train A+C, Test B
auc_raw_b, _, _, _ = evaluate_split(pd.concat([gen_a, gen_c]), gen_b, r_tr, r_te, normalize=False)
auc_norm_b, _, _, _ = evaluate_split(pd.concat([gen_a, gen_c]), gen_b, r_tr, r_te, normalize=True)
print(f"Train A+C -> Test B: Raw AUC = {auc_raw_b:.4f} | Normalized AUC = {auc_norm_b:.4f}")

# Config 3: Train B+C, Test A
auc_raw_a, _, _, _ = evaluate_split(pd.concat([gen_b, gen_c]), gen_a, r_tr, r_te, normalize=False)
auc_norm_a, _, _, _ = evaluate_split(pd.concat([gen_b, gen_c]), gen_a, r_tr, r_te, normalize=True)
print(f"Train B+C -> Test A: Raw AUC = {auc_raw_a:.4f} | Normalized AUC = {auc_norm_a:.4f}")

# 4. Length-Matched Evaluation
print("\n" + "="*60)
print("4. LENGTH-MATCHED EVALUATION")
print("="*60)
# Match character lengths: sample real tweets whose character length matches the hoax distribution
gen_c_lengths = gen_c["text"].apply(normalize_text).apply(len)
r_te_matched = r_te.copy()
r_te_matched["char_len"] = r_te_matched["text"].apply(normalize_text).apply(len)
# Keep real tweets within [gen_c_lengths.quantile(0.10), gen_c_lengths.quantile(0.90)]
min_len, max_len = gen_c_lengths.quantile(0.10), gen_c_lengths.quantile(0.90)
r_te_len_sub = r_te_matched[(r_te_matched["char_len"] >= min_len) & (r_te_matched["char_len"] <= max_len)]

test_texts_lm = pd.concat([gen_c["text"].apply(normalize_text), r_te_len_sub["text"].apply(normalize_text)])
test_y_lm = np.array([1]*len(gen_c) + [0]*len(r_te_len_sub))
X_te_lm = vec_norm.transform(test_texts_lm)
preds_lm = clf_norm.predict_proba(X_te_lm)[:, 1]
auc_lm = roc_auc_score(test_y_lm, preds_lm)
print(f"Length-matched normalized AUC on Gen C: {auc_lm:.4f} (Real N={len(r_te_len_sub)}, Hoax N={len(gen_c)})")

# 5. 20-Shuffle Permutation Control on Normalized Model
print("\n" + "="*60)
print("5. 20-SHUFFLE PERMUTATION CONTROL ON NORMALIZED MODEL")
print("="*60)
perm_scores = []
y_eval = np.array([1]*len(gen_c) + [0]*len(r_te))
test_texts_eval = pd.concat([gen_c["text"].apply(normalize_text), r_te["text"].apply(normalize_text)])
X_te_eval = vec_norm.transform(test_texts_eval)

for s in range(20):
    y_shuffled = np.random.RandomState(s).permutation(y_eval)
    perm_scores.append(roc_auc_score(y_shuffled, preds_norm := clf_norm.predict_proba(X_te_eval)[:, 1]))

mean_perm = np.mean(perm_scores)
std_perm = np.std(perm_scores)
t_stat, p_val = stats.ttest_1samp(perm_scores, 0.50)
print(f"20-Shuffle Permutation AUC: {mean_perm:.4f} +/- {std_perm:.4f} (t={t_stat:.2f}, p={p_val:.4f})")

# Save diagnostic results
diag_results = {
    "leave_one_out": {
        "train_ab_test_c": {"raw_auc": round(auc_raw_c, 4), "normalized_auc": round(auc_norm_c, 4)},
        "train_ac_test_b": {"raw_auc": round(auc_raw_b, 4), "normalized_auc": round(auc_norm_b, 4)},
        "train_bc_test_a": {"raw_auc": round(auc_raw_a, 4), "normalized_auc": round(auc_norm_a, 4)},
    },
    "length_matched_auc": round(auc_lm, 4),
    "permutation_test": {"mean_auc": round(mean_perm, 4), "std": round(std_perm, 4), "p_val": round(p_val, 4)}
}
with open(os.path.join(RESULTS_DIR, "item2_diagnostics.json"), "w") as f:
    json.dump(diag_results, f, indent=2)
print("\nSaved diagnostics to results/item2_diagnostics.json")
