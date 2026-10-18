import React from 'react';
import { X, CheckCircle2, XCircle } from 'lucide-react';

export default function StatusDrawer({
  isOpen,
  onClose,
  health,
  dataState = 'REPLAY',
  lastMsgTime
}) {
  if (!isOpen) return null;

  const isServiceRunning = (srv) => {
    if (!srv) return false;
    if (typeof srv === 'string') return srv.toLowerCase().includes('running');
    if (srv.status) return srv.status.toLowerCase().includes('running');
    return Boolean(srv.service_reachable);
  };

  const services = [
    {
      name: 'Kafka',
      running: isServiceRunning(health?.services?.kafka),
      lastMsg: health?.services?.kafka?.last_message || lastMsgTime
    },
    {
      name: 'Spark streaming',
      running: isServiceRunning(health?.services?.spark),
      lastMsg: null
    },
    {
      name: 'HDFS',
      running: isServiceRunning(health?.services?.hdfs),
      lastMsg: null
    },
    {
      name: 'Hive metastore',
      running: isServiceRunning(health?.services?.hive),
      lastMsg: null
    },
    {
      name: 'API',
      running: isServiceRunning(health?.services?.api) || Boolean(health),
      lastMsg: null
    }
  ];

  return (
    <div className="status-drawer-overlay" onClick={onClose}>
      <div className="status-drawer" onClick={(e) => e.stopPropagation()}>
        <div className="drawer-header">
          <div className="drawer-title-group">
            <h2 className="drawer-title">System status</h2>
            <span className={`status-pill pill-${dataState.toLowerCase()}`}>
              {dataState === 'LIVE' ? 'Live' : dataState === 'REPLAY' ? 'Replay of recorded posts' : 'Offline'}
            </span>
          </div>
          <button className="btn-close" onClick={onClose} aria-label="Close status drawer">
            <X size={18} />
          </button>
        </div>

        <div className="drawer-body">
          {dataState === 'OFFLINE' && (
            <div className="drawer-offline-alert">
              <strong>API offline.</strong> The API is not running. Start it with <code>make api</code>
            </div>
          )}

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
                  {s.name === 'Kafka' && s.running && s.lastMsg && (
                    <span className="last-seen">last message {s.lastMsg}</span>
                  )}
                </div>
              </div>
            ))}
          </div>
        </div>
      </div>
    </div>
  );
}
