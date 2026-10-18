import React from 'react';
import { X, CheckCircle2, XCircle, Clock, Zap } from 'lucide-react';

export default function StatusDrawer({
  isOpen,
  onClose,
  health,
  dataState = 'REPLAY',
  lastMsgTime,
  replayRate,
  onReplayRateChange
}) {
  if (!isOpen) return null;

  const services = [
    {
      name: 'Kafka',
      running: health?.services?.kafka === 'Running (service reachable)' || health?.services?.kafka?.includes('Running'),
      note: health?.services?.kafka || 'Checking...'
    },
    {
      name: 'Spark streaming',
      running: health?.services?.spark_streaming === 'Running (service reachable)' || health?.services?.spark_streaming?.includes('Running'),
      note: health?.services?.spark_streaming || 'Checking...'
    },
    {
      name: 'HDFS',
      running: health?.services?.hdfs === 'Running (service reachable)' || health?.services?.hdfs?.includes('Running'),
      note: health?.services?.hdfs || 'Checking...'
    },
    {
      name: 'Hive metastore',
      running: health?.services?.hive_metastore === 'Running (service reachable)' || health?.services?.hive_metastore?.includes('Running'),
      note: health?.services?.hive_metastore || 'Checking...'
    },
    {
      name: 'API',
      running: health?.services?.api === 'Running' || Boolean(health),
      note: health?.services?.api || 'Running'
    }
  ];

  return (
    <div className="status-drawer-overlay" onClick={onClose}>
      <div className="status-drawer" onClick={(e) => e.stopPropagation()}>
        <div className="drawer-header">
          <div className="drawer-title-group">
            <h2 className="drawer-title">System status</h2>
            <span className={`status-pill pill-${dataState.toLowerCase()}`}>
              {dataState === 'LIVE' ? 'Live stream' : dataState === 'REPLAY' ? 'Replay of recorded posts' : 'Offline'}
            </span>
          </div>
          <button className="btn-close" onClick={onClose} aria-label="Close status drawer">
            <X size={18} />
          </button>
        </div>

        <div className="drawer-body">
          <div className="service-list">
            {services.map((s) => (
              <div key={s.name} className="service-row">
                <div className="service-name-group">
                  {s.running ? (
                    <CheckCircle2 size={16} className="text-accent" />
                  ) : (
                    <XCircle size={16} className="text-muted" />
                  )}
                  <span className="service-name">{s.name}</span>
                </div>
                <div className="service-state">
                  <span className={s.running ? 'state-running' : 'state-not-running'}>
                    {s.running ? 'Running' : 'Not running'}
                  </span>
                  {s.name === 'Kafka' && lastMsgTime && (
                    <span className="last-seen">last message {lastMsgTime}</span>
                  )}
                </div>
              </div>
            ))}
          </div>

          <div className="drawer-section">
            <label className="section-label">
              <Zap size={14} className="icon-inline" /> Replay speed
            </label>
            <div className="speed-buttons">
              {[
                { label: '1x (Normal)', rate: 1.0 },
                { label: '2x (Fast)', rate: 0.5 },
                { label: '5x (Rapid)', rate: 0.2 }
              ].map((sp) => (
                <button
                  key={sp.rate}
                  className={`btn-speed ${replayRate === sp.rate ? 'active' : ''}`}
                  onClick={() => onReplayRateChange && onReplayRateChange(sp.rate)}
                >
                  {sp.label}
                </button>
              ))}
            </div>
            <p className="drawer-help-text">
              Adjusts arrival interval for the replayed disaster feed.
            </p>
          </div>
        </div>
      </div>
    </div>
  );
}
