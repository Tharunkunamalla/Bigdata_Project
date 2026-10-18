import React from 'react';
import { Sun, Moon, Radio } from 'lucide-react';

export default function Header({
  activeTab,
  setActiveTab,
  dataState = 'REPLAY', // 'LIVE', 'REPLAY', 'OFFLINE'
  onOpenStatus,
  theme,
  onToggleTheme
}) {
  const tabs = [
    { id: 'live', label: 'Live' },
    { id: 'dispatch', label: 'Dispatch' },
    { id: 'about', label: 'About' }
  ];

  const getStatusLabel = () => {
    if (dataState === 'LIVE') return 'Live';
    if (dataState === 'REPLAY') return 'Replay';
    return 'Offline';
  };

  const getStatusDotClass = () => {
    if (dataState === 'LIVE') return 'dot-live';
    if (dataState === 'REPLAY') return 'dot-replay';
    return 'dot-offline';
  };

  return (
    <header className="app-header">
      <div className="header-left">
        <div className="brand" onClick={() => setActiveTab('live')} role="button" tabIndex={0}>
          <span className="brand-name">CrisisGuard</span>
        </div>

        <button
          id="status-drawer-toggle"
          className="status-button"
          onClick={onOpenStatus}
          title="Open service status details"
          aria-label="Service status drawer"
        >
          <span className={`status-dot ${getStatusDotClass()}`} />
          <span className="status-text">{getStatusLabel()}</span>
        </button>
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

      <div className="header-right">
        <button
          id="theme-toggle-btn"
          className="btn-theme"
          onClick={onToggleTheme}
          title={`Switch to ${theme === 'dark' ? 'light' : 'dark'} mode`}
          aria-label="Toggle theme"
        >
          {theme === 'dark' ? <Sun size={15} /> : <Moon size={15} />}
        </button>
      </div>
    </header>
  );
}
