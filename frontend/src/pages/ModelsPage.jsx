import React, { useState, useEffect } from 'react';
import { api } from '../api/client';
import { formatDecimals, formatPercent } from '../utils/formatters';

export default function ModelsPage({ modelsData }) {
  const [selectedModelId, setSelectedModelId] = useState('tweet_detector');
  const [threshold, setThreshold] = useState(0.1786);
  const [thresholdMetrics, setThresholdMetrics] = useState(null);
  const [activeEvalView, setActiveEvalView] = useState('unseen_event');
  const [evalViews, setEvalViews] = useState(null);

  // Load evaluation views
  useEffect(() => {
    api.getEvaluationViews()
      .then(d => setEvalViews(d))
      .catch(err => console.error('Error fetching evaluation views:', err));
  }, []);

  // Fetch metrics when threshold slider moves
  useEffect(() => {
    api.getThresholdMetrics(threshold)
      .then(d => setThresholdMetrics(d))
      .catch(err => console.error('Error computing threshold metrics:', err));
  }, [threshold]);

  const modelsList = modelsData?.models || [];
  const selectedModel = modelsList.find(m => m.id === selectedModelId) || modelsList[0] || {};
  const curves = modelsData?.tweet_detector_curves || {};
  const rocPoints = curves.roc_curve || [];
  const prPoints = curves.pr_curve || [];

  return (
    <div className="models-page-container">
      {/* Top: Model Selector Bar */}
      <div className="section-card" style={{ marginBottom: '16px', padding: '12px 16px' }}>
        <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', flexWrap: 'wrap', gap: '12px' }}>
          <div>
            <h2 className="section-title" style={{ margin: 0 }}>Model Validation & Deception Diagnostics</h2>
            <span className="text-muted" style={{ fontSize: '12px' }}>
              Multi-task Spark MLlib inference suite evaluated against held-out generator hoaxes and unseen disaster events.
            </span>
          </div>

          <div className="btn-group">
            {modelsList.map(m => (
              <button
                key={m.id || m.task}
                className={`btn-toggle ${selectedModelId === (m.id || m.task) ? 'active' : ''}`}
                onClick={() => setSelectedModelId(m.id || m.task)}
              >
                {m.task.split(' ')[0]} {m.task.split(' ')[1]}
              </button>
            ))}
          </div>
        </div>
      </div>

      {/* Selected Model Card */}
      <div className="section-card" style={{ marginBottom: '16px' }}>
        <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'flex-start', flexWrap: 'wrap', gap: '12px' }}>
          <div>
            <h3 style={{ fontSize: '16px', fontWeight: 600 }}>{selectedModel.task}</h3>
            <span className="text-muted" style={{ fontSize: '12px' }}>Architecture: {selectedModel.model_type}</span>
          </div>
          <span className="badge badge-live" style={{ fontSize: '12px' }}>
            {selectedModel.primary_metric}
          </span>
        </div>

        <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(240px, 1fr))', gap: '12px', marginTop: '14px' }}>
          <div className="metric-card">
            <span className="metric-label">Training Corpus Provenance</span>
            <span className="metric-val" style={{ fontSize: '14px', fontWeight: 500 }}>
              {selectedModel.training_corpus}
            </span>
          </div>
          <div className="metric-card">
            <span className="metric-label">In-Distribution (Random Split)</span>
            <span className="metric-val" style={{ fontSize: '14px', fontWeight: 500, color: 'var(--status-warning-text)' }}>
              {selectedModel.in_distribution}
            </span>
          </div>
          <div className="metric-card">
            <span className="metric-label">Unseen-Event / Held-Out Generalization</span>
            <span className="metric-val" style={{ fontSize: '14px', fontWeight: 500, color: 'var(--status-success-text)' }}>
              {selectedModel.unseen_event_score}
            </span>
          </div>
        </div>
      </div>

      {/* Interactive Threshold Slider (For Tweet Misinformation Detector) */}
      {selectedModelId === 'tweet_detector' && (
        <div className="section-card" style={{ marginBottom: '16px' }}>
          <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: '12px', flexWrap: 'wrap', gap: '8px' }}>
            <div>
              <h3 style={{ fontSize: '15px', fontWeight: 600 }}>Interactive Operating Threshold Slider</h3>
              <span className="text-muted" style={{ fontSize: '12px' }}>
                Recomputes precision, recall, F1, and confusion matrix on the fly from the 32,879 test predictions.
              </span>
            </div>

            <button
              className="btn-secondary"
              style={{ fontSize: '12px', padding: '4px 10px' }}
              onClick={() => setThreshold(0.1786)}
            >
              Reset to Validation Threshold (&theta;* = 0.1786)
            </button>
          </div>

          <div style={{ margin: '14px 0' }}>
            <div style={{ display: 'flex', justifyContent: 'space-between', marginBottom: '6px' }}>
              <span style={{ fontSize: '13px', fontWeight: 500 }}>Decision Threshold (&theta;): <strong>{formatDecimals(threshold, 4)}</strong></span>
              {thresholdMetrics?.is_validation_selected && (
                <span className="badge badge-replay" style={{ fontSize: '11px' }}>
                  VALIDATION-OPTIMAL THRESHOLD (&theta;* = 0.1786)
                </span>
              )}
            </div>
            <input
              type="range"
              min="0.01"
              max="0.95"
              step="0.005"
              value={threshold}
              onChange={(e) => setThreshold(parseFloat(e.target.value))}
              style={{ width: '100%' }}
              aria-label="Model decision threshold slider"
            />
            <div style={{ display: 'flex', justifyContent: 'space-between', fontSize: '11px', color: 'var(--text-tertiary)', marginTop: '2px' }}>
              <span>0.01 (High Recall / Aggressive Alert)</span>
              <span>0.1786 (Calibrated Validation Optimal)</span>
              <span>0.50 (Default Logistic Threshold)</span>
              <span>0.95 (Ultra-Conservative)</span>
            </div>
          </div>

          {/* Recomputed Performance Metrics */}
          {thresholdMetrics && (
            <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(130px, 1fr))', gap: '10px', marginTop: '12px' }}>
              <div className="metric-card">
                <span className="metric-label">Precision</span>
                <span className="metric-val" style={{ color: 'var(--status-success-text)' }}>
                  {formatPercent(thresholdMetrics.precision * 100, 1)}
                </span>
              </div>
              <div className="metric-card">
                <span className="metric-label">Recall</span>
                <span className="metric-val">
                  {formatPercent(thresholdMetrics.recall * 100, 1)}
                </span>
              </div>
              <div className="metric-card">
                <span className="metric-label">Positive F1</span>
                <span className="metric-val">
                  {formatDecimals(thresholdMetrics.f1, 4)}
                </span>
              </div>
              <div className="metric-card">
                <span className="metric-label">Accuracy</span>
                <span className="metric-val">
                  {formatPercent(thresholdMetrics.accuracy * 100, 1)}
                </span>
              </div>
              <div className="metric-card">
                <span className="metric-label">Test Samples</span>
                <span className="metric-val">
                  {thresholdMetrics.total_test_samples?.toLocaleString()}
                </span>
              </div>
            </div>
          )}

          {/* Confusion Matrix Table */}
          {thresholdMetrics && (
            <div style={{ marginTop: '16px' }}>
              <span className="setting-label" style={{ marginBottom: '6px' }}>Confusion Matrix (Evaluated at &theta; = {formatDecimals(threshold, 4)})</span>
              <table className="confusion-matrix-table">
                <thead>
                  <tr>
                    <th>Ground Truth \ Predicted</th>
                    <th>Predicted Synthetic (Positive)</th>
                    <th>Predicted Credible (Negative)</th>
                  </tr>
                </thead>
                <tbody>
                  <tr>
                    <td><strong>Actual Synthetic Hoax</strong></td>
                    <td className="cm-cell cm-tp">
                      <strong>{thresholdMetrics.true_positives?.toLocaleString()}</strong>
                      <div className="cm-subtext">True Positives</div>
                    </td>
                    <td className="cm-cell cm-fn">
                      <strong>{thresholdMetrics.false_negatives?.toLocaleString()}</strong>
                      <div className="cm-subtext">False Negatives</div>
                    </td>
                  </tr>
                  <tr>
                    <td><strong>Actual Authentic Disaster Post</strong></td>
                    <td className="cm-cell cm-fp">
                      <strong>{thresholdMetrics.false_positives?.toLocaleString()}</strong>
                      <div className="cm-subtext">False Positives</div>
                    </td>
                    <td className="cm-cell cm-tn">
                      <strong>{thresholdMetrics.true_negatives?.toLocaleString()}</strong>
                      <div className="cm-subtext">True Negatives</div>
                    </td>
                  </tr>
                </tbody>
              </table>
              <div className="text-muted" style={{ fontSize: '11px', marginTop: '6px' }}>
                At &theta;* = 0.1786, the model maintains high positive precision ({formatPercent((thresholdMetrics.precision || 0) * 100, 1)}) to minimize false alarms on authentic civilian cries for help.
              </div>
            </div>
          )}
        </div>
      )}

      {/* SVG ROC and PR Curves */}
      {selectedModelId === 'tweet_detector' && (
        <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(320px, 1fr))', gap: '16px', marginBottom: '16px' }}>
          {/* ROC Curve */}
          <div className="chart-container">
            <div style={{ display: 'flex', justifyContent: 'space-between', marginBottom: '8px' }}>
              <span style={{ fontWeight: 600, fontSize: '13px' }}>Receiver Operating Characteristic (ROC)</span>
              <span className="badge badge-live">AUC = {curves.roc_auc || 0.9940}</span>
            </div>
            <svg viewBox="0 0 300 180" className="chart-svg">
              <rect x="35" y="15" width="250" height="135" fill="none" className="chart-grid" />
              {/* Diagonal reference */}
              <line x1="35" y1="150" x2="285" y2="15" stroke="var(--border-strong)" strokeDasharray="3,3" />
              {/* ROC Curve Path */}
              {rocPoints.length > 1 && (
                <polyline
                  fill="none"
                  stroke="var(--accent-primary)"
                  strokeWidth="2.5"
                  points={rocPoints.map(p => `${35 + p.fpr * 250},${150 - p.tpr * 135}`).join(' ')}
                />
              )}
              {/* Axes */}
              <line x1="35" y1="150" x2="285" y2="150" className="chart-axis" />
              <line x1="35" y1="15" x2="35" y2="150" className="chart-axis" />
              <text x="160" y="172" textAnchor="middle" className="chart-text">False Positive Rate (FPR)</text>
              <text x="12" y="85" textAnchor="middle" transform="rotate(-90 12,85)" className="chart-text">True Positive Rate</text>
            </svg>
          </div>

          {/* Precision-Recall Curve */}
          <div className="chart-container">
            <div style={{ display: 'flex', justifyContent: 'space-between', marginBottom: '8px' }}>
              <span style={{ fontWeight: 600, fontSize: '13px' }}>Precision-Recall (PR) Curve</span>
              <span className="badge badge-live">PR-AUC = {curves.pr_auc || 0.9007}</span>
            </div>
            <svg viewBox="0 0 300 180" className="chart-svg">
              <rect x="35" y="15" width="250" height="135" fill="none" className="chart-grid" />
              {/* PR Curve Path */}
              {prPoints.length > 1 && (
                <polyline
                  fill="none"
                  stroke="#10b981"
                  strokeWidth="2.5"
                  points={prPoints.map(p => `${35 + p.recall * 250},${150 - p.precision * 135}`).join(' ')}
                />
              )}
              {/* Axes */}
              <line x1="35" y1="150" x2="285" y2="150" className="chart-axis" />
              <line x1="35" y1="15" x2="35" y2="150" className="chart-axis" />
              <text x="160" y="172" textAnchor="middle" className="chart-text">Recall</text>
              <text x="12" y="85" textAnchor="middle" transform="rotate(-90 12,85)" className="chart-text">Precision</text>
            </svg>
          </div>
        </div>
      )}

      {/* Evaluation View Selector Slice */}
      <div className="section-card" style={{ marginBottom: '16px' }}>
        <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: '12px', flexWrap: 'wrap', gap: '8px' }}>
          <h3 style={{ fontSize: '15px', fontWeight: 600 }}>Empirical Evaluation Slices</h3>
          <div className="btn-group">
            {['unseen_event', 'in_distribution', 'generator_ablation', 'shuffled_control'].map(viewKey => (
              <button
                key={viewKey}
                className={`btn-toggle ${activeEvalView === viewKey ? 'active' : ''}`}
                onClick={() => setActiveEvalView(viewKey)}
              >
                {viewKey.replace('_', ' ').toUpperCase()}
              </button>
            ))}
          </div>
        </div>

        {evalViews && evalViews[activeEvalView] && (
          <div style={{ backgroundColor: 'var(--bg-surface-subtle)', padding: '14px', borderRadius: '4px', border: '1px solid var(--border-subtle)' }}>
            <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: '6px' }}>
              <strong>{evalViews[activeEvalView].title}</strong>
              <span className="badge badge-replay">{evalViews[activeEvalView].status}</span>
            </div>
            <p className="text-muted" style={{ fontSize: '13px', marginBottom: '10px' }}>
              {evalViews[activeEvalView].description}
            </p>
            <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(200px, 1fr))', gap: '8px' }}>
              {Object.entries(evalViews[activeEvalView].metrics || {}).map(([k, v]) => (
                <div key={k} style={{ padding: '8px', background: 'var(--bg-surface)', borderRadius: '4px', border: '1px solid var(--border-subtle)' }}>
                  <span className="metric-label">{k.replace(/_/g, ' ').toUpperCase()}</span>
                  <div style={{ fontWeight: 600, fontSize: '13px', marginTop: '2px' }}>{String(v)}</div>
                </div>
              ))}
            </div>
          </div>
        )}
      </div>

      {/* Permanent Known Limits Callout Box */}
      <div className="callout-box" style={{ marginTop: '16px' }}>
        <strong>Known Methodological Limits & Academic Disclaimers:</strong>
        <ul style={{ paddingLeft: '18px', marginTop: '6px', fontSize: '12px', lineHeight: '1.6' }}>
          <li>
            <strong>Own Hoax Generators:</strong> Generators A, B, and C are our own synthesized rule-based perturbation templates with slot perturbations (no external LLM API was used in the streaming pipeline). High AUC (0.9940) is an artifact of template phrasing contrasting against casual tweets; performance in the wild will be lower.
          </li>
          <li>
            <strong>Random Stratified Split Optimism:</strong> The 75/25 split partitions inside each source dataset, allowing posts from identical disasters into train and test. Strictly holding out disaster events reduces humanitarian category Macro-F1 from 0.7564 to 0.5587 (-0.198 penalty).
          </li>
          <li>
            <strong>Length Shortcut Sanity Checks:</strong> Length-only baseline achieves AUC 0.6685, stylometric baseline achieves 0.7820, proving separation does not rely solely on character length.
          </li>
        </ul>
      </div>
    </div>
  );
}
