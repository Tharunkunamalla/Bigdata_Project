import React from 'react';

export default function SettingsDrawer({
  isOpen,
  onClose,
  settings,
  onUpdateSettings
}) {
  if (!isOpen) return null;

  const handleToggleColumn = (colId) => {
    const current = settings.visibleColumns || [];
    const next = current.includes(colId)
      ? current.filter(c => c !== colId)
      : [...current, colId];
    onUpdateSettings({ ...settings, visibleColumns: next });
  };

  const columnsList = [
    { id: 'time', label: 'Timestamp' },
    { id: 'event', label: 'Event Type' },
    { id: 'text', label: 'Message Content' },
    { id: 'urgency', label: 'Urgency Rating' },
    { id: 'p_synth', label: 'p(Synthetic)' },
    { id: 'trust', label: 'Calibrated Trust' },
    { id: 'verdict', label: 'Model Verdict' }
  ];

  return (
    <div className="drawer-overlay" onClick={onClose} role="dialog" aria-modal="true" aria-labelledby="settings-title">
      <div className="drawer-panel" onClick={(e) => e.stopPropagation()}>
        <div className="drawer-header">
          <h2 id="settings-title" className="drawer-title">Settings & Display Preferences</h2>
          <button className="btn-close" onClick={onClose} aria-label="Close settings">✕</button>
        </div>

        <div className="drawer-body">
          {/* Theme */}
          <div className="setting-group">
            <label className="setting-label">Theme</label>
            <div className="setting-control-row">
              {['dark', 'light'].map((t) => (
                <button
                  key={t}
                  className={`btn-toggle ${settings.theme === t ? 'active' : ''}`}
                  onClick={() => onUpdateSettings({ ...settings, theme: t })}
                >
                  {t.charAt(0).toUpperCase() + t.slice(1)}
                </button>
              ))}
            </div>
          </div>

          {/* Row Density */}
          <div className="setting-group">
            <label className="setting-label">Feed Table Density</label>
            <div className="setting-control-row">
              {['comfortable', 'compact'].map((d) => (
                <button
                  key={d}
                  className={`btn-toggle ${settings.density === d ? 'active' : ''}`}
                  onClick={() => onUpdateSettings({ ...settings, density: d })}
                >
                  {d.charAt(0).toUpperCase() + d.slice(1)}
                </button>
              ))}
            </div>
          </div>

          {/* Timezone */}
          <div className="setting-group">
            <label className="setting-label">Timestamp Timezone</label>
            <div className="setting-control-row">
              {['local', 'utc'].map((tz) => (
                <button
                  key={tz}
                  className={`btn-toggle ${settings.timezone === tz ? 'active' : ''}`}
                  onClick={() => onUpdateSettings({ ...settings, timezone: tz })}
                >
                  {tz.toUpperCase()}
                </button>
              ))}
            </div>
          </div>

          {/* Units */}
          <div className="setting-group">
            <label className="setting-label">Distance Units</label>
            <div className="setting-control-row">
              {['km', 'miles'].map((u) => (
                <button
                  key={u}
                  className={`btn-toggle ${settings.units === u ? 'active' : ''}`}
                  onClick={() => onUpdateSettings({ ...settings, units: u })}
                >
                  {u === 'km' ? 'Kilometers (km)' : 'Miles (mi)'}
                </button>
              ))}
            </div>
          </div>

          {/* Replay Streaming Rate */}
          <div className="setting-group">
            <label className="setting-label">Replay Ingestion Interval</label>
            <div className="setting-control-row">
              {[0.5, 1.0, 2.0].map((r) => (
                <button
                  key={r}
                  className={`btn-toggle ${settings.streamRate === r ? 'active' : ''}`}
                  onClick={() => onUpdateSettings({ ...settings, streamRate: r })}
                >
                  {r}s / event
                </button>
              ))}
            </div>
          </div>

          {/* Visible Columns */}
          <div className="setting-group">
            <label className="setting-label">Visible Feed Columns</label>
            <div className="checkbox-list">
              {columnsList.map((col) => (
                <label key={col.id} className="checkbox-item">
                  <input
                    type="checkbox"
                    checked={(settings.visibleColumns || []).includes(col.id)}
                    onChange={() => handleToggleColumn(col.id)}
                  />
                  <span>{col.label}</span>
                </label>
              ))}
            </div>
          </div>
        </div>

        <div className="drawer-footer">
          <span className="text-muted" style={{ fontSize: '11px' }}>Preferences stored safely in local browser storage.</span>
          <button className="btn-primary" onClick={onClose}>Done</button>
        </div>
      </div>
    </div>
  );
}
