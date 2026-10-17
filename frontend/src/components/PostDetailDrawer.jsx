import React from 'react';
import { formatDecimals } from '../utils/formatters';

export default function PostDetailDrawer({ post, onClose, isReplayMode = false }) {
  if (!post) return null;

  const isFlagged = post.is_flagged_misinformation === 1 || (post.prob_synthetic_score >= 0.1786);

  return (
    <div className="drawer-overlay" onClick={onClose} role="dialog" aria-modal="true" aria-labelledby="post-detail-title">
      <div className="drawer-panel" onClick={(e) => e.stopPropagation()} style={{ maxWidth: '440px' }}>
        <div className="drawer-header">
          <div>
            <h2 id="post-detail-title" className="drawer-title">Post Inspection & Feature Attribution</h2>
            <span className="text-muted" style={{ fontSize: '11px' }}>ID: {post.post_id} | Arrived: {post.timestamp || 'Live'}</span>
          </div>
          <button className="btn-close" onClick={onClose} aria-label="Close detail drawer">✕</button>
        </div>

        <div className="drawer-body">
          {/* Verdict Banner */}
          <div style={{
            padding: '10px 14px',
            borderRadius: '4px',
            marginBottom: '14px',
            border: '1px solid',
            borderColor: isFlagged ? 'var(--status-danger-text)' : 'var(--status-success-text)',
            backgroundColor: isFlagged ? 'var(--status-danger-bg)' : 'var(--status-success-bg)',
            display: 'flex',
            justifyContent: 'space-between',
            alignItems: 'center'
          }}>
            <div>
              <strong style={{ color: isFlagged ? 'var(--status-danger-text)' : 'var(--status-success-text)' }}>
                {isFlagged ? 'Suspected Synthetic Deception' : 'Credible Civilian Report'}
              </strong>
              <div style={{ fontSize: '11px', color: 'var(--text-secondary)' }}>
                Calibrated Threshold: &theta;* = 0.1786
              </div>
            </div>
            <span className="badge" style={{
              backgroundColor: isFlagged ? 'var(--status-danger-text)' : 'var(--status-success-text)',
              color: '#ffffff'
            }}>
              {isFlagged ? 'FLAGGED' : 'VERIFIED'}
            </span>
          </div>

          {/* Replay Mode: Ground Truth Disclosure */}
          {isReplayMode && (
            <div style={{
              padding: '8px 12px',
              backgroundColor: 'var(--bg-surface-subtle)',
              border: '1px dashed var(--border-strong)',
              borderRadius: '4px',
              marginBottom: '14px',
              fontSize: '12px'
            }}>
              <span className="text-muted">Injected (Simulated Ground Truth): </span>
              <strong>{post.injected_ground_truth || (post.is_injected ? 'Injected Hoax' : 'Authentic Disaster Post')}</strong>
            </div>
          )}

          {/* Full Message Text */}
          <div className="setting-group">
            <label className="setting-label">Emergency Message Text</label>
            <div style={{
              padding: '10px 12px',
              backgroundColor: 'var(--bg-surface-subtle)',
              borderRadius: '4px',
              border: '1px solid var(--border-subtle)',
              fontSize: '13px',
              lineHeight: '1.45',
              color: 'var(--text-primary)'
            }}>
              {post.text}
            </div>
          </div>

          {/* Metadata Grid */}
          <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: '10px', margin: '14px 0' }}>
            <div className="metric-card" style={{ padding: '8px 12px' }}>
              <span className="metric-label">p(Synthetic Deception)</span>
              <span className="metric-val" style={{ fontSize: '18px', color: isFlagged ? 'var(--status-danger-text)' : 'var(--text-primary)' }}>
                {formatDecimals(post.prob_synthetic_score, 3)}
              </span>
            </div>
            <div className="metric-card" style={{ padding: '8px 12px' }}>
              <span className="metric-label">Calibrated Trust Score</span>
              <span className="metric-val" style={{ fontSize: '18px', color: isFlagged ? 'var(--status-danger-text)' : 'var(--status-success-text)' }}>
                {formatDecimals(post.trust_score, 3)}
              </span>
            </div>
            <div className="metric-card" style={{ padding: '8px 12px' }}>
              <span className="metric-label">Urgency Priority</span>
              <span className="metric-val" style={{ fontSize: '18px' }}>
                {formatDecimals(post.urgency_level, 1)} / 5.0
              </span>
            </div>
            <div className="metric-card" style={{ padding: '8px 12px' }}>
              <span className="metric-label">Event Domain</span>
              <span className="metric-val" style={{ fontSize: '15px' }}>
                {post.event_type || 'Disaster Event'}
              </span>
            </div>
          </div>

          {/* Top Contributing Factors */}
          <div className="setting-group">
            <label className="setting-label">Top Predictive Scoring Factors</label>
            <ul style={{ paddingLeft: '18px', color: 'var(--text-secondary)', fontSize: '12px', lineHeight: '1.6' }}>
              {(post.top_factors || []).map((factor, idx) => (
                <li key={idx}>{factor}</li>
              ))}
              <li>MinHash near-duplicate cluster size: <strong>{post.cluster_size || 1} post(s)</strong></li>
            </ul>
          </div>

          {/* Geolocation Notice */}
          <div className="callout-box" style={{ marginTop: '12px', fontSize: '11px' }}>
            <strong>Geospatial Notice:</strong> Coordinates ({post.latitude?.toFixed(4)}, {post.longitude?.toFixed(4)}) are simulated on the {post.region || 'regional'} bounding box in compliance with Twitter developer privacy terms. Depot facilities are authentic OpenStreetMap nodes.
          </div>
        </div>

        <div className="drawer-footer">
          <button className="btn-secondary" onClick={onClose} style={{ width: '100%' }}>Close Drawer</button>
        </div>
      </div>
    </div>
  );
}
