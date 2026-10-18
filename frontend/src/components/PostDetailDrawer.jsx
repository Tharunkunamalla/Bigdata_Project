import React from 'react';
import { X, AlertCircle, CheckCircle2, Clock, MapPin, Tag } from 'lucide-react';

export default function PostDetailDrawer({ post, onClose }) {
  if (!post) return null;

  const isFlagged = Boolean(post.is_flagged || post.is_flagged_misinformation === 1 || post.prob_synthetic_score >= 0.1786);
  const isUrgent = Boolean(post.is_urgent || (post.urgency_level && post.urgency_level >= 4.0));

  const reasons = (post.why_flagged && post.why_flagged.length > 0)
    ? post.why_flagged
    : (isFlagged
      ? ['Very uniform wording typical of generated text', 'Unusual phrasing patterns']
      : ['Consistent with authentic civilian emergency reports']);

  return (
    <div className="drawer-overlay" onClick={onClose} role="dialog" aria-modal="true">
      <div className="detail-panel" onClick={(e) => e.stopPropagation()}>
        <div className="panel-header">
          <div>
            <h3 className="panel-title">Report detail</h3>
            <span className="panel-subtitle">Received at {post.timestamp || 'Recent'}</span>
          </div>
          <button className="btn-close" onClick={onClose} aria-label="Close detail panel">
            <X size={18} />
          </button>
        </div>

        <div className="panel-body">
          {/* Status summary banner */}
          {isFlagged ? (
            <div className="flagged-banner">
              <AlertCircle size={16} className="banner-icon" />
              <div>
                <strong>Flagged as unreliable</strong>
                <p>This report matches patterns commonly seen in automated or synthetic content.</p>
              </div>
            </div>
          ) : (
            <div className="normal-banner">
              <CheckCircle2 size={16} className="banner-icon" />
              <div>
                <strong>Standard civilian report</strong>
                <p>No synthetic or automated patterns detected.</p>
              </div>
            </div>
          )}

          {/* Full Post Text */}
          <div className="detail-section">
            <label className="section-label">Report text</label>
            <div className="report-text-box">
              {post.text}
            </div>
          </div>

          {/* Metadata chips */}
          <div className="detail-meta-row">
            <div className="meta-chip">
              <Tag size={13} className="chip-icon" />
              <span>Event: <strong>{post.event_type || 'Disaster'}</strong></span>
            </div>
            {isUrgent && (
              <div className="meta-chip urgent">
                <span>Priority: <strong>Urgent</strong></span>
              </div>
            )}
            <div className="meta-chip">
              <Clock size={13} className="chip-icon" />
              <span>Time: <strong>{post.timestamp || '—'}</strong></span>
            </div>
          </div>

          {/* Plain language reason */}
          <div className="detail-section">
            <label className="section-label">
              {isFlagged ? 'Why it was flagged' : 'Assessment'}
            </label>
            <ul className="reasons-list">
              {reasons.map((r, i) => (
                <li key={i} className="reason-item">
                  <span className="reason-bullet" />
                  <span>{r}</span>
                </li>
              ))}
            </ul>
          </div>

          {/* Location note */}
          <div className="location-note">
            <MapPin size={13} className="note-icon" />
            <span>Locations are simulated inside the region.</span>
          </div>
        </div>
      </div>
    </div>
  );
}
