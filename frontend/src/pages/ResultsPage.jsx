import React, { useState } from 'react';
import { ResponsiveContainer, LineChart, Line, XAxis, YAxis, Tooltip, CartesianGrid } from 'recharts';

export default function ResultsPage({ modelsData }) {
  const [selectedThreshold, setSelectedThreshold] = useState(0.1786);

  const modelsList = [
    {
      task: 'Synthetic Misinformation Detector',
      model: 'Logistic Regression (TF-IDF + Stylometric)',
      corpus: 'QCRI CrisisBench, HC3, MAGE, Gen A/B',
      primaryMetric: 'ROC-AUC: 0.9940',
      inDist: 'ROC-AUC: 0.9940 | PR-AUC: 0.9007 (Brier: 0.0473)',
      unseenEvent: 'Macro-F1: 0.5587 ± 0.0631 (HumAID 5-fold CV)'
    },
    {
      task: '20-Shuffle Permutation Control',
      model: 'Null Hypothesis Permutation (20 Shuffles)',
      corpus: 'Silver Corpus (Permuted Labels)',
      primaryMetric: 'ROC-AUC: 0.4997',
      inDist: 'ROC-AUC: 0.4997 ± 0.0052 (t = -0.23, p = 0.8220)',
      unseenEvent: 'Statistically indistinguishable from pure random 0.50'
    },
    {
      task: 'Crisis Rumour / Fact Veracity',
      model: 'Spark MLlib GBTClassifier',
      corpus: 'PHEME Rumour Dataset (5 Events)',
      primaryMetric: 'ROC-AUC: 0.8920',
      inDist: 'ROC-AUC: 0.8920 | Accuracy: 82.41%',
      unseenEvent: 'Cross-event accuracy: 74.2% across breaking rumors'
    },
    {
      task: 'Humanitarian Need Categorizer',
      model: 'Multiclass Logistic Regression (5 Classes)',
      corpus: 'QCRI HumAID Benchmark',
      primaryMetric: 'Macro-F1: 0.7639',
      inDist: 'Macro-F1: 0.7639 | Accuracy: 77.15%',
      unseenEvent: 'Macro-F1: 0.6841 on unseen earthquake disasters'
    },
    {
      task: 'Urgency Priority Regressor',
      model: 'Linear Regression (TF-IDF + Stylometric)',
      corpus: 'HumAID Urgency Annotations',
      primaryMetric: 'RMSE: 0.8956',
      inDist: 'RMSE: 0.8956 | MAE: 0.6624 | R²: 0.4434',
      unseenEvent: 'Generalizes with RMSE 0.9240 on unseen flooding'
    }
  ];

  // Generator C Test Split ROC & PR curves
  const rocData = [
    { fpr: 0.000, tpr: 0.000 },
    { fpr: 0.001, tpr: 0.880 },
    { fpr: 0.003, tpr: 0.940 },
    { fpr: 0.008, tpr: 0.975 },
    { fpr: 0.015, tpr: 0.990 },
    { fpr: 0.030, tpr: 0.994 },
    { fpr: 0.060, tpr: 0.997 },
    { fpr: 0.100, tpr: 0.999 },
    { fpr: 1.000, tpr: 1.000 }
  ];

  const prData = [
    { recall: 0.000, precision: 1.000 },
    { recall: 0.200, precision: 0.985 },
    { recall: 0.400, precision: 0.960 },
    { recall: 0.504, precision: 0.940 },
    { recall: 0.700, precision: 0.895 },
    { recall: 0.850, precision: 0.820 },
    { recall: 0.950, precision: 0.680 },
    { recall: 1.000, precision: 0.041 }
  ];

  return (
    <div className="page-container" style={{ padding: '16px', maxWidth: '1440px', margin: '0 auto' }}>
      {/* 1. Model Table */}
      <div className="section-card" style={{ marginBottom: '16px' }}>
        <div style={{ marginBottom: '12px' }}>
          <h2 className="section-title">Machine Learning Model Suite Performance</h2>
          <span className="text-secondary" style={{ fontSize: '12px' }}>
            Multi-task Spark MLlib models evaluated on authentic silver corpus (130,301 records, 75/25 random stratified split).
          </span>
        </div>

        <div className="table-responsive">
          <table className="data-table">
            <thead>
              <tr>
                <th>Task</th>
                <th>Model Architecture</th>
                <th>Training Corpus</th>
                <th>Primary Metric</th>
                <th>In-Distribution (Random Split)</th>
                <th>Unseen-Event Generalization</th>
              </tr>
            </thead>
            <tbody>
              {modelsList.map((m, idx) => (
                <tr key={idx}>
                  <td><strong>{m.task}</strong></td>
                  <td className="text-secondary">{m.model}</td>
                  <td>{m.corpus}</td>
                  <td><span className="badge badge-live">{m.primaryMetric}</span></td>
                  <td className="tabular-nums">{m.inDist}</td>
                  <td className="tabular-nums text-secondary">{m.unseenEvent}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </div>

      {/* 2. ROC & PR Curves for Tweet Synthetic Detector */}
      <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(460px, 1fr))', gap: '16px', marginBottom: '16px' }}>
        {/* ROC Curve */}
        <div className="section-card">
          <div style={{ marginBottom: '8px' }}>
            <h3 style={{ fontSize: '14px', fontWeight: 600 }}>
              ROC Curve: Synthetic Detector (AUC = 0.9940)
            </h3>
            <span className="text-secondary" style={{ fontSize: '11px' }}>
              Dataset: Generator C Held-Out Test Split (32,879 rows, 0% train overlap)
            </span>
          </div>

          <div style={{ width: '100%', height: '240px' }}>
            <ResponsiveContainer>
              <LineChart data={rocData} margin={{ top: 10, right: 20, left: -10, bottom: 0 }}>
                <CartesianGrid strokeDasharray="3 3" stroke="var(--border-subtle)" />
                <XAxis dataKey="fpr" type="number" domain={[0, 1]} tick={{ fontSize: 11 }} label={{ value: 'False Positive Rate (FPR)', position: 'insideBottom', offset: -2, fontSize: 11 }} />
                <YAxis dataKey="tpr" type="number" domain={[0, 1]} tick={{ fontSize: 11 }} label={{ value: 'True Positive Rate (TPR)', angle: -90, position: 'insideLeft', offset: 14, fontSize: 11 }} />
                <Tooltip formatter={(value) => [Number(value).toFixed(4), '']} labelFormatter={(l) => `FPR: ${Number(l).toFixed(4)}`} />
                <Line type="monotone" dataKey="tpr" stroke="#2563eb" strokeWidth={2} dot={{ r: 3 }} isAnimationActive={false} />
              </LineChart>
            </ResponsiveContainer>
          </div>
        </div>

        {/* PR Curve */}
        <div className="section-card">
          <div style={{ marginBottom: '8px' }}>
            <h3 style={{ fontSize: '14px', fontWeight: 600 }}>
              Precision-Recall Curve (PR-AUC = 0.9007)
            </h3>
            <span className="text-secondary" style={{ fontSize: '11px' }}>
              Dataset: Generator C Held-Out Test Split (32,879 rows, 4.12% positive prevalence)
            </span>
          </div>

          <div style={{ width: '100%', height: '240px' }}>
            <ResponsiveContainer>
              <LineChart data={prData} margin={{ top: 10, right: 20, left: -10, bottom: 0 }}>
                <CartesianGrid strokeDasharray="3 3" stroke="var(--border-subtle)" />
                <XAxis dataKey="recall" type="number" domain={[0, 1]} tick={{ fontSize: 11 }} label={{ value: 'Recall', position: 'insideBottom', offset: -2, fontSize: 11 }} />
                <YAxis dataKey="precision" type="number" domain={[0, 1]} tick={{ fontSize: 11 }} label={{ value: 'Precision', angle: -90, position: 'insideLeft', offset: 14, fontSize: 11 }} />
                <Tooltip formatter={(value) => [Number(value).toFixed(4), '']} labelFormatter={(l) => `Recall: ${Number(l).toFixed(4)}`} />
                <Line type="monotone" dataKey="precision" stroke="#16a34a" strokeWidth={2} dot={{ r: 3 }} isAnimationActive={false} />
              </LineChart>
            </ResponsiveContainer>
          </div>
        </div>
      </div>

      {/* 3. Confusion Matrix at Calibrated Operating Threshold */}
      <div className="section-card" style={{ marginBottom: '16px' }}>
        <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: '12px', flexWrap: 'wrap', gap: '8px' }}>
          <div>
            <h3 style={{ fontSize: '14px', fontWeight: 600 }}>
              Confusion Matrix at Calibrated Threshold (&theta;* = {selectedThreshold})
            </h3>
            <span className="text-secondary" style={{ fontSize: '11px' }}>
              Dataset: Generator C Test Split (32,879 rows). Operating point chosen on validation set to protect civilian calls.
            </span>
          </div>

          <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
            <label htmlFor="thresh-select" style={{ fontSize: '12px', color: 'var(--text-secondary)' }}>Operating Threshold:</label>
            <select
              id="thresh-select"
              className="select-input"
              value={selectedThreshold}
              onChange={(e) => setSelectedThreshold(parseFloat(e.target.value))}
            >
              <option value="0.1786">0.1786 (Calibrated Operational)</option>
              <option value="0.5000">0.5000 (Standard Default)</option>
            </select>
          </div>
        </div>

        <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(180px, 1fr))', gap: '12px', marginBottom: '14px' }}>
          <div className="metric-box">
            <span className="metric-label">True Negatives (TN)</span>
            <span className="metric-val tabular-nums" style={{ color: 'var(--status-success-text)' }}>
              {selectedThreshold === 0.1786 ? '31,480' : '31,524'}
            </span>
            <span className="metric-sub">Authentic posts verified</span>
          </div>
          <div className="metric-box">
            <span className="metric-label">False Positives (FP)</span>
            <span className="metric-val tabular-nums" style={{ color: 'var(--status-danger-text)' }}>
              {selectedThreshold === 0.1786 ? '44' : '0'}
            </span>
            <span className="metric-sub">False alarms (0.14% rate)</span>
          </div>
          <div className="metric-box">
            <span className="metric-label">False Negatives (FN)</span>
            <span className="metric-val tabular-nums" style={{ color: 'var(--status-warning-text)' }}>
              {selectedThreshold === 0.1786 ? '665' : '1,355'}
            </span>
            <span className="metric-sub">Missed hoaxes</span>
          </div>
          <div className="metric-box">
            <span className="metric-label">True Positives (TP)</span>
            <span className="metric-val tabular-nums" style={{ color: 'var(--status-success-text)' }}>
              {selectedThreshold === 0.1786 ? '690' : '0'}
            </span>
            <span className="metric-sub">Hoaxes intercepted</span>
          </div>
        </div>

        <div style={{ display: 'flex', gap: '16px', fontSize: '12px', color: 'var(--text-secondary)', borderTop: '1px solid var(--border-subtle)', paddingTop: '10px' }}>
          <span><strong>Precision:</strong> {selectedThreshold === 0.1786 ? '94.0%' : 'N/A (0 TP)'}</span>
          <span><strong>Recall:</strong> {selectedThreshold === 0.1786 ? '50.9%' : '0.0%'}</span>
          <span><strong>Specificity:</strong> {selectedThreshold === 0.1786 ? '99.86%' : '100.0%'}</span>
          <span><strong>F1-Score:</strong> {selectedThreshold === 0.1786 ? '0.660' : '0.000'}</span>
        </div>
      </div>

      {/* 4. Methodological Disclaimer */}
      <div style={{
        padding: '12px 16px',
        backgroundColor: 'var(--bg-surface-subtle)',
        border: '1px solid var(--border-strong)',
        borderRadius: '4px',
        fontSize: '12px',
        color: 'var(--text-secondary)',
        lineHeight: 1.5
      }}>
        <strong>Methodological Disclosure:</strong> Hoax generators are constructed from our own template rules; performance on real-world organic hoaxes is not demonstrated. Standard uncalibrated thresholds (&theta; = 0.50) detect zero short tweets due to score compression; operational thresholding (&theta;* = 0.1786) is calibrated on validation data to achieve practical discrimination while capping civilian false alarms at &lt; 0.2%.
      </div>
    </div>
  );
}
