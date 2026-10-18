import React from 'react';
import { Sun, Moon } from 'lucide-react';

export default function Header({
  activeTab,
  setActiveTab,
  streamState, // 'LIVE', 'REPLAY', 'STALE', 'OFFLINE'
  isSimulated = true,
  theme,
  onToggleTheme
}) {
  const tabs = [
    { id: 'live', label: 'Live' },
    { id: 'allocation', label: 'Allocation' },
    { id: 'pipeline', label: 'Pipeline' },
    { id: 'results', label: 'Results' }
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
          <span className={`badge ${getBadgeClass(streamState)}`} title={`Ingestion feed state: ${streamState}`}>
            <span className="status-dot"></span>
            {streamState}
          </span>
          {isSimulated && (
            <span className="badge badge-simulated" title="Simulated data: Social coordinates & replay feed active">
              Simulated data
            </span>
          )}
        </div>
      </div>

      <nav className="tab-nav" aria-label="Main navigation">
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
          id="theme-toggle-btn"
          className="btn-icon"
          onClick={onToggleTheme}
          title={`Switch to ${theme === 'dark' ? 'light' : 'dark'} mode`}
          aria-label="Toggle visual theme"
        >
          {theme === 'dark' ? <Sun size={15} /> : <Moon size={15} />}
        </button>
      </div>
    </header>
  );
}
