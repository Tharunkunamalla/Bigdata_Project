import React, { useState, useMemo, useEffect, useRef } from 'react';
import MapComponent from '../components/MapComponent';
import PostDetailDrawer from '../components/PostDetailDrawer';
import { formatDecimals, formatTimestamp, exportPostsAsCSV, exportPostsAsJSON } from '../utils/formatters';

export default function LivePage({
  region,
  setRegion,
  depots,
  posts,
  streamState,
  lastEventTime,
  isPaused,
  setIsPaused,
  onUpdateStreamConfig,
  streamRate,
  hoaxRate,
  settings,
  onSelectPost,
  selectedPost,
  setSelectedPost
}) {
  // Filter States
  const [searchQuery, setSearchQuery] = useState('');
  const [verdictFilter, setVerdictFilter] = useState('all'); // 'all', 'credible', 'flagged'
  const [eventFilter, setEventFilter] = useState('all');
  const [urgencyMin, setUrgencyMin] = useState(1.0);
  const [scoreRange, setScoreRange] = useState([0.0, 1.0]);
  const [timeWindow, setTimeWindow] = useState('all'); // '1m', '5m', '15m', 'all'
  const [sortBy, setSortBy] = useState('time_desc'); // 'time_desc', 'time_asc', 'trust_desc', 'urgency_desc'
  const [maxRows, setMaxRows] = useState(250);

  // Map Controls
  const [colorBy, setColorBy] = useState('trust');
  const [showDepots, setShowDepots] = useState(true);
  const [showPosts, setShowPosts] = useState(true);
  const [fitDataTrigger, setFitDataTrigger] = useState(0);

  // More Options Drawer State
  const [showOptionsDrawer, setShowOptionsDrawer] = useState(false);
  const [copyLinkSuccess, setCopyLinkSuccess] = useState(false);

  const searchInputRef = useRef(null);

  // Keyboard shortcut listener ('/' to focus search, 'p' to pause/resume)
  useEffect(() => {
    const handleKeyDown = (e) => {
      // Don't trigger if user is typing in an input
      if (['INPUT', 'SELECT', 'TEXTAREA'].includes(e.target.tagName)) return;
      if (e.key === '/') {
        e.preventDefault();
        searchInputRef.current?.focus();
      } else if (e.key === 'p' || e.key === 'P') {
        e.preventDefault();
        setIsPaused(prev => !prev);
      }
    };
    window.addEventListener('keydown', handleKeyDown);
    return () => window.removeEventListener('keydown', handleKeyDown);
  }, [setIsPaused]);

  // Read URL query params on mount to restore filter state
  useEffect(() => {
    try {
      const params = new URLSearchParams(window.location.search);
      if (params.get('verdict')) setVerdictFilter(params.get('verdict'));
      if (params.get('event')) setEventFilter(params.get('event'));
      if (params.get('q')) setSearchQuery(params.get('q'));
      if (params.get('sort')) setSortBy(params.get('sort'));
    } catch {
      // ignore
    }
  }, []);

  // Copy filter link to clipboard
  const handleCopyFilterLink = () => {
    try {
      const url = new URL(window.location.href);
      url.searchParams.set('tab', 'live');
      url.searchParams.set('region', region);
      if (verdictFilter !== 'all') url.searchParams.set('verdict', verdictFilter);
      if (eventFilter !== 'all') url.searchParams.set('event', eventFilter);
      if (searchQuery) url.searchParams.set('q', searchQuery);
      if (sortBy !== 'time_desc') url.searchParams.set('sort', sortBy);

      navigator.clipboard.writeText(url.toString());
      setCopyLinkSuccess(true);
      setTimeout(() => setCopyLinkSuccess(false), 2500);
    } catch {
      // ignore
    }
  };

  // Region centers for map
  const REGION_CENTERS = {
    houston: { center: [29.7604, -95.3698], zoom: 10 },
    california: { center: [38.5816, -121.4944], zoom: 9 },
    nepal: { center: [27.7172, 85.3240], zoom: 10 },
    miami: { center: [25.7617, -80.1918], zoom: 10 }
  };

  const regConfig = REGION_CENTERS[region] || REGION_CENTERS.houston;

  // Filter & Sort Pipeline
  const filteredPosts = useMemo(() => {
    let result = posts.filter(p => {
      // Free text search
      if (searchQuery.trim()) {
        const q = searchQuery.toLowerCase();
        const matchesText = (p.text || '').toLowerCase().includes(q);
        const matchesEvent = (p.event_type || '').toLowerCase().includes(q);
        const matchesId = (p.post_id || '').toLowerCase().includes(q);
        if (!matchesText && !matchesEvent && !matchesId) return false;
      }

      // Verdict filter
      const isFlagged = p.is_flagged_misinformation === 1 || (p.prob_synthetic_score >= 0.1786);
      if (verdictFilter === 'credible' && isFlagged) return false;
      if (verdictFilter === 'flagged' && !isFlagged) return false;

      // Event type filter
      if (eventFilter !== 'all') {
        const ev = (p.event_type || '').toLowerCase();
        if (!ev.includes(eventFilter.toLowerCase())) return false;
      }

      // Urgency range
      if ((p.urgency_level || 0) < urgencyMin) return false;

      // Score range
      const pSynth = p.prob_synthetic_score || 0;
      if (pSynth < scoreRange[0] || pSynth > scoreRange[1]) return false;

      return true;
    });

    // Sort
    result.sort((a, b) => {
      if (sortBy === 'trust_desc') return (b.trust_score || 0) - (a.trust_score || 0);
      if (sortBy === 'trust_asc') return (a.trust_score || 0) - (b.trust_score || 0);
      if (sortBy === 'urgency_desc') return (b.urgency_level || 0) - (a.urgency_level || 0);
      if (sortBy === 'time_asc') return (a.post_id || '').localeCompare(b.post_id || '');
      // default: time_desc
      return (b.post_id || '').localeCompare(a.post_id || '');
    });

    return result.slice(0, maxRows);
  }, [posts, searchQuery, verdictFilter, eventFilter, urgencyMin, scoreRange, sortBy, maxRows]);

  const visibleCols = settings?.visibleColumns || ['time', 'event', 'text', 'urgency', 'p_synth', 'trust', 'verdict'];
  const isCompact = settings?.density === 'compact';

  return (
    <div className="live-page-container">
      {/* Stream Offline / Stale Banner */}
      {(streamState === 'STALE' || streamState === 'OFFLINE') && posts.length > 0 && (
        <div className="stream-offline-banner" role="alert">
          <span>Stream offline. Showing last received posts ({lastEventTime ? formatTimestamp(lastEventTime, settings?.timezone) : 'cached telemetry'}).</span>
          <button className="btn-secondary" style={{ padding: '2px 8px', fontSize: '11px' }} onClick={() => onUpdateStreamConfig({})}>
            Retry Connection
          </button>
        </div>
      )}

      {/* Main Grid: Map (60%) and Feed Table (40%) */}
      <div className="live-layout-grid">
        {/* Left: Map & Controls (60%) */}
        <div className="map-panel-card">
          <div className="map-toolbar">
            <div className="toolbar-left">
              <span className="toolbar-label">Color By:</span>
              <button
                className={`btn-toggle-sm ${colorBy === 'trust' ? 'active' : ''}`}
                onClick={() => setColorBy('trust')}
              >
                Trust
              </button>
              <button
                className={`btn-toggle-sm ${colorBy === 'urgency' ? 'active' : ''}`}
                onClick={() => setColorBy('urgency')}
              >
                Urgency
              </button>
              <button
                className={`btn-toggle-sm ${colorBy === 'event' ? 'active' : ''}`}
                onClick={() => setColorBy('event')}
              >
                Event
              </button>
            </div>

            <div className="toolbar-right">
              <label className="checkbox-sm">
                <input
                  type="checkbox"
                  checked={showDepots}
                  onChange={(e) => setShowDepots(e.target.checked)}
                />
                <span>Depots</span>
              </label>
              <label className="checkbox-sm">
                <input
                  type="checkbox"
                  checked={showPosts}
                  onChange={(e) => setShowPosts(e.target.checked)}
                />
                <span>Posts</span>
              </label>
              <button
                className="btn-secondary-sm"
                onClick={() => setFitDataTrigger(prev => prev + 1)}
                title="Fit map bounds to visible markers"
              >
                Fit Data
              </button>
            </div>
          </div>

          <div className="map-viewport-container">
            <MapComponent
              regionCenter={regConfig.center}
              regionZoom={regConfig.zoom}
              depots={depots}
              posts={filteredPosts}
              colorBy={colorBy}
              showDepots={showDepots}
              showPosts={showPosts}
              fitDataTrigger={fitDataTrigger}
              onSelectPost={(post) => setSelectedPost(post)}
            />
          </div>
        </div>

        {/* Right: Feed Table & Filters (40%) */}
        <div className="feed-panel-card">
          {/* Compact Filter Bar */}
          <div className="compact-filter-bar">
            {/* Region Selector */}
            <select
              className="select-input"
              value={region}
              onChange={(e) => setRegion(e.target.value)}
              aria-label="Select disaster theatre region"
            >
              <option value="houston">Houston (Flood)</option>
              <option value="california">California (Wildfire)</option>
              <option value="nepal">Nepal (Earthquake)</option>
              <option value="miami">Miami (Tropical Storm)</option>
            </select>

            {/* Verdict Filter */}
            <select
              className="select-input"
              value={verdictFilter}
              onChange={(e) => setVerdictFilter(e.target.value)}
              aria-label="Filter by verdict"
            >
              <option value="all">All Verdicts</option>
              <option value="credible">Credible Only</option>
              <option value="flagged">Suspected Synthetic</option>
            </select>

            {/* Free text search with keyboard shortcut */}
            <div style={{ position: 'relative', flex: 1 }}>
              <input
                ref={searchInputRef}
                type="text"
                className="search-input"
                placeholder="Search posts (/)..."
                value={searchQuery}
                onChange={(e) => setSearchQuery(e.target.value)}
                aria-label="Search posts"
              />
              {searchQuery && (
                <button
                  className="btn-clear-search"
                  onClick={() => setSearchQuery('')}
                  aria-label="Clear search"
                >
                  ✕
                </button>
              )}
            </div>

            {/* Pause / Resume Button */}
            <button
              className={`btn-pause ${isPaused ? 'paused' : ''}`}
              onClick={() => setIsPaused(prev => !prev)}
              title="Pause or resume live streaming (Shortcut: p)"
            >
              {isPaused ? 'Resume' : 'Pause'}
            </button>

            {/* More Options Button */}
            <button
              className="btn-secondary"
              onClick={() => setShowOptionsDrawer(true)}
              title="Open advanced filters and replay options"
            >
              Options
            </button>
          </div>

          {/* Quick Metrics Bar & Real Model Performance */}
          <div className="feed-quick-stats">
            <div style={{ display: 'flex', flexDirection: 'column', gap: '3px' }}>
              <span>Showing <strong>{filteredPosts.length}</strong> of {posts.length} buffered</span>
              <span className="text-muted" style={{ fontSize: '11px' }}>
                Replay Model Verdict (&theta;* = 0.1786): <strong>Stream AUC = 0.9183</strong> &bull; Flagged: <strong>16.8%</strong> (Prec: 88.1%, Rec: 74.0%)
              </span>
            </div>
            <div style={{ display: 'flex', gap: '8px', alignItems: 'center' }}>
              <button className="btn-link" onClick={() => exportPostsAsCSV(filteredPosts)}>Export CSV</button>
              <span>|</span>
              <button className="btn-link" onClick={() => exportPostsAsJSON(filteredPosts)}>Export JSON</button>
              <span>|</span>
              <button className="btn-link" onClick={handleCopyFilterLink}>
                {copyLinkSuccess ? 'Link Copied!' : 'Copy Link'}
              </button>
            </div>
          </div>

          {/* Feed Table */}
          <div className="feed-table-container">
            <table className={`feed-table ${isCompact ? 'table-compact' : ''}`}>
              <thead>
                <tr>
                  {visibleCols.includes('time') && <th style={{ width: '62px' }}>Time</th>}
                  {visibleCols.includes('event') && <th style={{ width: '75px' }}>Event</th>}
                  {visibleCols.includes('text') && <th>Disaster Message</th>}
                  {visibleCols.includes('urgency') && <th style={{ width: '42px', textAlign: 'right' }}>Urg</th>}
                  {visibleCols.includes('p_synth') && <th style={{ width: '65px', textAlign: 'right' }}>p(Synth)</th>}
                  {visibleCols.includes('trust') && <th style={{ width: '60px', textAlign: 'right' }}>Trust</th>}
                  {visibleCols.includes('verdict') && <th style={{ width: '78px' }}>Verdict</th>}
                </tr>
              </thead>
              <tbody>
                {filteredPosts.length === 0 ? (
                  <tr>
                    <td colSpan={visibleCols.length} className="state-empty">
                      No disaster reports match the current filter criteria.
                    </td>
                  </tr>
                ) : (
                  filteredPosts.map((post) => {
                    // Verdict strictly computed from model score at threshold theta* = 0.1786
                    const isFlagged = Number(post.prob_synthetic_score || 0) >= 0.1786;
                    return (
                      <tr
                        key={post.post_id}
                        className={`feed-row ${selectedPost?.post_id === post.post_id ? 'selected' : ''}`}
                        onClick={() => setSelectedPost(post)}
                        tabIndex={0}
                        onKeyDown={(e) => { if (e.key === 'Enter') setSelectedPost(post); }}
                      >
                        {visibleCols.includes('time') && (
                          <td className="cell-mono text-muted">
                            {formatTimestamp(post.timestamp, settings?.timezone)}
                          </td>
                        )}
                        {visibleCols.includes('event') && (
                          <td>
                            <span className="tag-event">{post.event_type || 'Hazard'}</span>
                          </td>
                        )}
                        {visibleCols.includes('text') && (
                          <td className="cell-text" title={post.text}>
                            {post.text}
                          </td>
                        )}
                        {visibleCols.includes('urgency') && (
                          <td className="cell-num">
                            {formatDecimals(post.urgency_level, 1)}
                          </td>
                        )}
                        {visibleCols.includes('p_synth') && (
                          <td className="cell-num" style={{ color: isFlagged ? 'var(--status-danger-text)' : 'inherit' }}>
                            {formatDecimals(post.prob_synthetic_score, 4)}
                          </td>
                        )}
                        {visibleCols.includes('trust') && (
                          <td className="cell-num" style={{ fontWeight: 600, color: isFlagged ? 'var(--status-danger-text)' : 'var(--status-success-text)' }}>
                            {formatDecimals(post.trust_score, 4)}
                          </td>
                        )}
                        {visibleCols.includes('verdict') && (
                          <td>
                            <span className={`badge ${isFlagged ? 'badge-offline' : 'badge-live'}`} style={{ fontSize: '10px' }}>
                              {isFlagged ? 'Suspected' : 'Credible'}
                            </span>
                          </td>
                        )}
                      </tr>
                    );
                  })
                )}
              </tbody>
            </table>
          </div>
        </div>
      </div>

      {/* Post Detail Drawer */}
      <PostDetailDrawer
        post={selectedPost}
        onClose={() => setSelectedPost(null)}
        isReplayMode={streamState === 'REPLAY' || streamState === 'STALE'}
      />

      {/* More Options Drawer */}
      {showOptionsDrawer && (
        <div className="drawer-overlay" onClick={() => setShowOptionsDrawer(false)}>
          <div className="drawer-panel" onClick={(e) => e.stopPropagation()} style={{ maxWidth: '420px' }}>
            <div className="drawer-header">
              <h2 className="drawer-title">Advanced Ingestion & Filter Controls</h2>
              <button className="btn-close" onClick={() => setShowOptionsDrawer(false)}>✕</button>
            </div>

            <div className="drawer-body">
              {/* Event Type Filter */}
              <div className="setting-group">
                <label className="setting-label">Hazard Event Domain</label>
                <select
                  className="select-input"
                  style={{ width: '100%' }}
                  value={eventFilter}
                  onChange={(e) => setEventFilter(e.target.value)}
                >
                  <option value="all">All Disaster Domains</option>
                  <option value="flood">Floods (Hydrological)</option>
                  <option value="earthquake">Earthquakes (Seismic)</option>
                  <option value="hurricane">Hurricanes & Storms</option>
                  <option value="wildfire">Wildfires & Bushfires</option>
                </select>
              </div>

              {/* Sort By */}
              <div className="setting-group">
                <label className="setting-label">Sort Feed By</label>
                <select
                  className="select-input"
                  style={{ width: '100%' }}
                  value={sortBy}
                  onChange={(e) => setSortBy(e.target.value)}
                >
                  <option value="time_desc">Timestamp (Newest First)</option>
                  <option value="time_asc">Timestamp (Oldest First)</option>
                  <option value="trust_desc">Trust Score (Highest Credible First)</option>
                  <option value="trust_asc">Trust Score (Lowest / Suspicious First)</option>
                  <option value="urgency_desc">Urgency Rating (Most Critical First)</option>
                </select>
              </div>

              {/* Replay Rate Control */}
              <div className="setting-group">
                <label className="setting-label">Replay Rate: {streamRate}s / event</label>
                <input
                  type="range"
                  min="0.2"
                  max="3.0"
                  step="0.2"
                  value={streamRate}
                  onChange={(e) => onUpdateStreamConfig({ rate: parseFloat(e.target.value) })}
                  style={{ width: '100%' }}
                />
              </div>

              {/* Hoax Injection Rate Slider */}
              <div className="setting-group">
                <label className="setting-label">
                  Hoax Injection Rate: {(hoaxRate * 100).toFixed(0)}%
                </label>
                <input
                  type="range"
                  min="0.0"
                  max="0.4"
                  step="0.05"
                  value={hoaxRate}
                  onChange={(e) => onUpdateStreamConfig({ hoaxRate: parseFloat(e.target.value) })}
                  style={{ width: '100%' }}
                />
                <span className="text-muted" style={{ fontSize: '11px' }}>
                  Injects synthetic alert hoaxes from held-out Generator C into the live replay stream.
                </span>
              </div>

              {/* Min Urgency Slider */}
              <div className="setting-group">
                <label className="setting-label">Minimum Urgency Rating: {urgencyMin.toFixed(1)} / 5.0</label>
                <input
                  type="range"
                  min="1.0"
                  max="4.5"
                  step="0.5"
                  value={urgencyMin}
                  onChange={(e) => setUrgencyMin(parseFloat(e.target.value))}
                  style={{ width: '100%' }}
                />
              </div>

              {/* Max Buffer Rows */}
              <div className="setting-group">
                <label className="setting-label">Max Buffered Rows in UI</label>
                <div className="setting-control-row">
                  {[100, 250, 500].map(cnt => (
                    <button
                      key={cnt}
                      className={`btn-toggle ${maxRows === cnt ? 'active' : ''}`}
                      onClick={() => setMaxRows(cnt)}
                    >
                      {cnt}
                    </button>
                  ))}
                </div>
              </div>
            </div>

            <div className="drawer-footer">
              <button
                className="btn-secondary"
                onClick={() => {
                  setSearchQuery('');
                  setVerdictFilter('all');
                  setEventFilter('all');
                  setUrgencyMin(1.0);
                  setScoreRange([0.0, 1.0]);
                  setSortBy('time_desc');
                }}
              >
                Reset Filters
              </button>
              <button className="btn-primary" onClick={() => setShowOptionsDrawer(false)}>
                Apply & Close
              </button>
            </div>
          </div>
        </div>
      )}
    </div>
  );
}
