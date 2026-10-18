import React, { useState, useMemo } from 'react';
import MapComponent from '../components/MapComponent';
import PostDetailDrawer from '../components/PostDetailDrawer';
import { Search, Pause, Play, AlertTriangle, ShieldAlert } from 'lucide-react';

const REGION_CENTERS = {
  houston: { center: [29.7604, -95.3698], zoom: 11, label: 'Houston (Hurricane Harvey)' },
  california: { center: [38.5816, -121.4944], zoom: 10, label: 'Northern California (Wildfires)' },
  nepal: { center: [27.7172, 85.3240], zoom: 11, label: 'Kathmandu Valley (Earthquake)' },
  miami: { center: [25.7617, -80.1918], zoom: 11, label: 'Miami / South Florida (Hurricane Irma)' }
};

export default function LivePage({
  region,
  setRegion,
  depots,
  posts,
  dataState,
  isPaused,
  setIsPaused,
  selectedPost,
  setSelectedPost
}) {
  const [searchQuery, setSearchQuery] = useState('');
  const [showFilter, setShowFilter] = useState('all'); // 'all', 'flagged', 'urgent'

  const currentRegionConfig = REGION_CENTERS[region] || REGION_CENTERS.houston;

  // Filter posts based on controls
  const filteredPosts = useMemo(() => {
    return posts.filter((post) => {
      const isFlagged = Boolean(
        post.is_flagged ||
        post.is_flagged_misinformation === 1 ||
        (post.prob_synthetic_score !== undefined && post.prob_synthetic_score >= 0.1786)
      );
      const isUrgent = Boolean(post.is_urgent || (post.urgency_level && post.urgency_level >= 4.0));

      if (showFilter === 'flagged' && !isFlagged) return false;
      if (showFilter === 'urgent' && !isUrgent) return false;

      if (searchQuery.trim()) {
        const q = searchQuery.toLowerCase();
        const textMatch = post.text && post.text.toLowerCase().includes(q);
        const eventMatch = post.event_type && post.event_type.toLowerCase().includes(q);
        if (!textMatch && !eventMatch) return false;
      }
      return true;
    });
  }, [posts, showFilter, searchQuery]);

  // Three live counts computed directly from current buffer (no hardcoded numbers!)
  const stats = useMemo(() => {
    let flagged = 0;
    let urgent = 0;
    posts.forEach((p) => {
      if (p.is_flagged || p.is_flagged_misinformation === 1 || (p.prob_synthetic_score !== undefined && p.prob_synthetic_score >= 0.1786)) {
        flagged++;
      }
      if (p.is_urgent || (p.urgency_level && p.urgency_level >= 4.0)) {
        urgent++;
      }
    });
    return {
      total: posts.length,
      flagged,
      urgent
    };
  }, [posts]);

  // Clean time formatter: guarantees HH:mm:ss
  const formatTime = (ts) => {
    if (!ts) return '--:--:--';
    if (/^\d{2}:\d{2}:\d{2}$/.test(ts)) return ts;
    try {
      const d = new Date(ts);
      if (isNaN(d.getTime())) return '--:--:--';
      const hh = String(d.getHours()).padStart(2, '0');
      const mm = String(d.getMinutes()).padStart(2, '0');
      const ss = String(d.getSeconds()).padStart(2, '0');
      return `${hh}:${mm}:${ss}`;
    } catch {
      return '--:--:--';
    }
  };

  return (
    <div className="live-page-layout">
      {/* Offline banner if stream is down */}
      {dataState === 'OFFLINE' && (
        <div className="offline-banner">
          <ShieldAlert size={15} />
          <span>Stream is offline. Replay paused until connectivity is restored.</span>
        </div>
      )}

      {/* Main 2-column view */}
      <div className="live-view-grid">
        {/* Left Column: Map */}
        <section className="map-panel" aria-label="Disaster region map">
          <MapComponent
            regionCenter={currentRegionConfig.center}
            regionZoom={currentRegionConfig.zoom}
            depots={depots}
            posts={filteredPosts}
            showDepots={true}
            showPosts={true}
            showLines={false}
            onSelectPost={setSelectedPost}
            fitDataTrigger={posts.length}
          />
        </section>

        {/* Right Column: Live Feed */}
        <section className="feed-panel" aria-label="Incoming incident feed">
          {/* Above feed: 3 live counts from current buffer */}
          <div className="feed-stats-strip">
            <div className="stat-item">
              <span className="stat-label">Posts received</span>
              <span className="stat-value tabular-nums">{stats.total}</span>
            </div>
            <div className="stat-item">
              <span className="stat-label">Flagged</span>
              <span className="stat-value stat-flagged tabular-nums">{stats.flagged}</span>
            </div>
            <div className="stat-item">
              <span className="stat-label">Urgent</span>
              <span className="stat-value stat-urgent tabular-nums">{stats.urgent}</span>
            </div>
          </div>

          {/* Controls toolbar */}
          <div className="feed-controls-bar">
            {/* Region selector */}
            <select
              id="region-selector"
              className="ctrl-select"
              value={region}
              onChange={(e) => setRegion(e.target.value)}
              aria-label="Select disaster region"
            >
              <option value="houston">Houston</option>
              <option value="california">Northern California</option>
              <option value="nepal">Kathmandu Valley</option>
              <option value="miami">Miami / South Florida</option>
            </select>

            {/* Show Filter: All / Flagged / Urgent */}
            <div className="filter-segmented-group" role="group" aria-label="Filter posts">
              <button
                className={`seg-btn ${showFilter === 'all' ? 'active' : ''}`}
                onClick={() => setShowFilter('all')}
              >
                All
              </button>
              <button
                className={`seg-btn ${showFilter === 'flagged' ? 'active' : ''}`}
                onClick={() => setShowFilter('flagged')}
              >
                Flagged
              </button>
              <button
                className={`seg-btn ${showFilter === 'urgent' ? 'active' : ''}`}
                onClick={() => setShowFilter('urgent')}
              >
                Urgent
              </button>
            </div>

            {/* Search */}
            <div className="search-input-wrapper">
              <Search size={14} className="search-icon" />
              <input
                id="search-posts-input"
                type="text"
                placeholder="Search..."
                value={searchQuery}
                onChange={(e) => setSearchQuery(e.target.value)}
                className="ctrl-search-input"
              />
            </div>

            {/* Pause / Resume */}
            <button
              id="pause-resume-btn"
              className={`btn-pause ${isPaused ? 'paused' : ''}`}
              onClick={() => setIsPaused(!isPaused)}
              title={isPaused ? 'Resume stream' : 'Pause stream'}
              aria-label={isPaused ? 'Resume stream' : 'Pause stream'}
            >
              {isPaused ? <Play size={14} /> : <Pause size={14} />}
              <span>{isPaused ? 'Resume' : 'Pause'}</span>
            </button>
          </div>

          {/* Feed List */}
          <div className="feed-list-container">
            {filteredPosts.length === 0 ? (
              <div className="feed-empty-state">
                <p>No matching reports found in current buffer.</p>
              </div>
            ) : (
              <div className="feed-rows">
                {filteredPosts.map((post) => {
                  const isFlagged = Boolean(
                    post.is_flagged ||
                    post.is_flagged_misinformation === 1 ||
                    (post.prob_synthetic_score !== undefined && post.prob_synthetic_score >= 0.1786)
                  );
                  const isUrgent = Boolean(post.is_urgent || (post.urgency_level && post.urgency_level >= 4.0));

                  return (
                    <article
                      key={post.post_id}
                      className={`feed-row ${isFlagged ? 'row-flagged' : ''} ${selectedPost?.post_id === post.post_id ? 'row-selected' : ''}`}
                      onClick={() => setSelectedPost(post)}
                      tabIndex={0}
                      role="button"
                    >
                      <div className="feed-row-top">
                        <time className="post-time tabular-nums">{formatTime(post.timestamp)}</time>
                        <span className="post-event-tag">{post.event_type || 'Disaster'}</span>

                        {/* Thin urgency indicator */}
                        {isUrgent && (
                          <span className="badge-urgent">Urgent</span>
                        )}

                        {/* Small Flagged label ONLY on flagged posts; normal posts have NO badge */}
                        {isFlagged && (
                          <span className="badge-flagged">Flagged</span>
                        )}
                      </div>

                      <p className="feed-row-text">
                        {post.text}
                      </p>
                    </article>
                  );
                })}
              </div>
            )}
          </div>
        </section>
      </div>

      {/* Post Detail Drawer on row click */}
      <PostDetailDrawer
        post={selectedPost}
        onClose={() => setSelectedPost(null)}
      />
    </div>
  );
}
