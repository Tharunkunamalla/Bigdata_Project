import React from 'react';

export default function Header({
  activeTab,
  setActiveTab,
  streamState, // 'LIVE', 'REPLAY', 'STALE', 'OFFLINE'
  isSimulated = true,
  onOpenSettings,
  theme,
  onToggleTheme
}) {
  const tabs = [
    { id: 'live', label: 'Live Operations' },
    { id: 'models', label: 'Model Validation' },
    { id: 'allocation', label: 'Fleet Allocation' },
    { id: 'pipeline', label: 'Big Data Pipeline' },
    { id: 'demo', label: 'End-to-End Demo' }
  ];

  const getBadgeClass = (state) => {
    switch (state) {
      case 'LIVE': return 'badge-live';
      case 'REPLAY': return 'badge-replay';
      case 'STALE': return 'badge-stale';
      case 'OFFLINE': default: return 'badge-offline';
    }
  };

  return (
    <header className="app-header">
      <div className="brand-section">
        <span className="brand-title">CrisisGuard AI</span>
        <div className="badge-group">
          <span className={`badge ${getBadgeClass(streamState)}`} title={`Current ingestion feed state: ${streamState}`}>
            <span className="status-dot"></span>
            {streamState}
          </span>
          {isSimulated && (
            <span className="badge badge-simulated" title="Coordinates or simulated replay data active">
              SIMULATED
            </span>
          )}
        </div>
      </div>

      <nav className="tab-nav" aria-label="Main application sections">
        {tabs.map((tab) => (
          <button
            key={tab.id}
            id={`nav-tab-${tab.id}`}
            className={`tab-btn ${activeTab === tab.id ? 'active' : ''}`}
            onClick={() => setActiveTab(tab.id)}
          >
            {tab.label}
          </button>
        ))}
      </nav>

      <div className="header-actions">
        <button
          className="btn-icon"
          onClick={onToggleTheme}
          title={`Switch to ${theme === 'dark' ? 'light' : 'dark'} mode`}
          aria-label="Toggle theme"
        >
          {theme === 'dark' ? 'Light' : 'Dark'}
        </button>
        <button
          className="btn-icon"
          onClick={onOpenSettings}
          title="Open application settings"
          aria-label="Settings"
        >
          Settings
        </button>
      </div>
    </header>
  );
}
