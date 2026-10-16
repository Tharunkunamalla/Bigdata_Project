import React, { useState, useEffect, useRef } from 'react';
import L from 'leaflet';
import 'leaflet/dist/leaflet.css';

// Regional coordinates and initial setup
const REGIONS = {
  houston: { name: 'Houston Metro', disaster: 'Hurricane Harvey (Flood)', center: [29.7604, -95.3698], zoom: 10, resource: 'rescue_boats' },
  california: { name: 'Northern California', disaster: 'California Wildfires', center: [38.5816, -121.4944], zoom: 9, resource: 'wildfire_units' },
  nepal: { name: 'Kathmandu Valley', disaster: 'Nepal Earthquake', center: [27.7172, 85.3240], zoom: 10, resource: 'earthquake_rescue_teams' },
  miami: { name: 'South Florida', disaster: 'Hurricane & Tropical Storm', center: [25.7617, -80.1918], zoom: 10, resource: 'ambulances' }
};

export default function App() {
  const params = typeof window !== 'undefined' ? new URLSearchParams(window.location.search) : null;
  const initialTab = params?.get('tab') || 'live';
  const initialTheme = params?.get('theme') || 'dark';

  const [activeTab, setActiveTab] = useState(initialTab); // 'live', 'models', 'allocation', 'pipeline', 'demo'
  const [theme, setTheme] = useState(initialTheme);
  const [region, setRegion] = useState('houston');
  const [sourceFilter, setSourceFilter] = useState('all');
  const [isPaused, setIsPaused] = useState(false);
  
  // Data states
  const [health, setHealth] = useState(null);
  const [summary, setSummary] = useState(null);
  const [depots, setDepots] = useState([]);
  const [posts, setPosts] = useState([]);
  const [selectedPost, setSelectedPost] = useState(null);
  const [streamStatus, setStreamStatus] = useState('CONNECTING'); // 'LIVE', 'REPLAY', 'OFFLINE', 'CONNECTING'
  
  // Models State
  const [modelsData, setModelsData] = useState(null);

  // Allocation State
  const [allocPolicy, setAllocPolicy] = useState('trust_aware_validation_thresh');
  const [allocResult, setAllocResult] = useState(null);
  const [allocLoading, setAllocLoading] = useState(false);

  // Demo State
  const [demoOutputs, setDemoOutputs] = useState({});
  const [demoLoading, setDemoLoading] = useState({});
  const [exportStatus, setExportStatus] = useState(null);

  // Map references
  const mapContainerRef = useRef(null);
  const mapInstanceRef = useRef(null);
  const markersGroupRef = useRef(null);
  const depotsGroupRef = useRef(null);
  const linesGroupRef = useRef(null);

  // SSE ref
  const sseRef = useRef(null);
  const reconnectTimeoutRef = useRef(null);

  // Toggle theme
  const toggleTheme = () => {
    const next = theme === 'dark' ? 'light' : 'dark';
    setTheme(next);
    document.documentElement.setAttribute('data-theme', next);
  };

  // 1. Initial Health and Summary fetch
  useEffect(() => {
    document.documentElement.setAttribute('data-theme', theme);

    fetch('/api/health')
      .then(res => res.json())
      .then(d => setHealth(d))
      .catch(() => setHealth({ status: 'OFFLINE', services: {} }));

    fetch('/api/summary')
      .then(res => res.json())
      .then(d => setSummary(d))
      .catch(err => console.error('Summary error:', err));

    fetch('/api/models')
      .then(res => res.json())
      .then(d => setModelsData(d))
      .catch(err => console.error('Models error:', err));
  }, []);

  // 2. Fetch Depots when region changes
  useEffect(() => {
    fetch(`/api/depots?region=${region}`)
      .then(res => res.json())
      .then(d => setDepots(d.depots || []))
      .catch(err => console.error('Depots error:', err));
  }, [region]);

  // 3. Leaflet Map Lifecycle
  useEffect(() => {
    if (activeTab !== 'live' && activeTab !== 'allocation') return;
    if (!mapContainerRef.current) return;

    if (!mapInstanceRef.current) {
      const reg = REGIONS[region];
      const map = L.map(mapContainerRef.current, {
        center: reg.center,
        zoom: reg.zoom,
        zoomControl: true
      });

      L.tileLayer('https://{s}.basemaps.cartocdn.com/rastertiles/voyager/{z}/{x}/{y}{r}.png', {
        attribution: '&copy; OpenStreetMap contributors &copy; CARTO',
        maxZoom: 18
      }).addTo(map);

      depotsGroupRef.current = L.layerGroup().addTo(map);
      markersGroupRef.current = L.layerGroup().addTo(map);
      linesGroupRef.current = L.layerGroup().addTo(map);

      mapInstanceRef.current = map;
    } else {
      const reg = REGIONS[region];
      mapInstanceRef.current.setView(reg.center, reg.zoom);
    }
  }, [activeTab, region]);

  // Render Depots on Map
  useEffect(() => {
    if (!depotsGroupRef.current) return;
    depotsGroupRef.current.clearLayers();

    depots.forEach(depot => {
      const icon = L.divIcon({
        className: 'depot-pin',
        html: `<div style="background:#2563eb; color:white; border:2px solid #ffffff; width:22px; height:22px; border-radius:4px; display:flex; align-items:center; justify-content:center; font-size:10px; font-weight:bold;">D</div>`,
        iconSize: [22, 22]
      });

      const cap = depot.capacities ? Object.values(depot.capacities)[0] : 15;
      const marker = L.marker([depot.latitude, depot.longitude], { icon })
        .bindPopup(`<strong>${depot.name}</strong><br/>Capacity: ${cap} units`);
      depotsGroupRef.current.addLayer(marker);
    });
  }, [depots]);

  // Render Posts on Map
  useEffect(() => {
    if (!markersGroupRef.current) return;
    markersGroupRef.current.clearLayers();

    const filtered = posts.filter(p => {
      if (sourceFilter === 'live') return !p.coords_simulated;
      if (sourceFilter === 'replay') return p.coords_simulated;
      return true;
    });

    filtered.slice(0, 100).forEach(post => {
      const isSynth = post.is_flagged_misinformation === 1;
      const color = isSynth ? '#ef4444' : '#22c55e';
      const icon = L.divIcon({
        className: 'post-pin',
        html: `<div style="background:${color}; width:10px; height:10px; border-radius:50%; border:1px solid #ffffff;"></div>`,
        iconSize: [10, 10]
      });

      const marker = L.marker([post.latitude, post.longitude], { icon })
        .bindPopup(`<strong>Post ${post.post_id}</strong><br/>Status: ${post.status_label}<br/>Urgency: ${post.urgency_level}<br/>Trust: ${post.trust_score}`);
      marker.on('click', () => setSelectedPost(post));
      markersGroupRef.current.addLayer(marker);
    });
  }, [posts, sourceFilter]);

  // Render Allocation Lines on Map
  useEffect(() => {
    if (!linesGroupRef.current) return;
    linesGroupRef.current.clearLayers();

    if (activeTab === 'allocation' && allocResult && allocResult.assignments) {
      allocResult.assignments.forEach(asgn => {
        const isSynth = asgn.is_synthetic === 1;
        const color = isSynth ? '#ef4444' : '#2563eb';
        const polyline = L.polyline(
          [[asgn.depot_lat, asgn.depot_lon], [asgn.demand_lat, asgn.demand_lon]],
          { color: color, weight: 2, opacity: 0.75, dashArray: isSynth ? '4, 4' : null }
        ).bindPopup(`Dispatch to ${asgn.demand_id} (${asgn.distance_km} km) - ${asgn.status}`);
        linesGroupRef.current.addLayer(polyline);
      });
    }
  }, [activeTab, allocResult]);

  // 4. Real-time SSE Stream Connection with Backoff and Deduplication
  useEffect(() => {
    let reconnectAttempts = 0;

    const connectSSE = () => {
      if (isPaused) return;

      try {
        const sse = new EventSource('/api/stream');
        sseRef.current = sse;

        sse.onopen = () => {
          setStreamStatus('LIVE');
          reconnectAttempts = 0;
        };

        sse.onmessage = (event) => {
          if (isPaused) return;
          try {
            const data = JSON.parse(event.data);
            setPosts(prev => {
              // Deduplicate by post_id and keep max 500
              if (prev.some(p => p.post_id === data.post_id)) {
                return prev;
              }
              const updated = [data, ...prev];
              return updated.slice(0, 500);
            });
          } catch (e) {
            console.error('Error parsing SSE event data:', e);
          }
        };

        sse.onerror = () => {
          setStreamStatus('OFFLINE');
          if (sseRef.current) {
            sseRef.current.close();
            sseRef.current = null;
          }
          reconnectAttempts += 1;
          const delay = Math.min(1000 * Math.pow(2, reconnectAttempts), 10000);
          reconnectTimeoutRef.current = setTimeout(connectSSE, delay);
        };
      } catch (err) {
        setStreamStatus('OFFLINE');
      }
    };

    // Initial seed posts
    fetch(`/api/sample-posts?region=${region}&limit=40`)
      .then(res => res.json())
      .then(d => {
        if (d.posts) {
          setPosts(d.posts);
          if (d.posts.length > 0) setSelectedPost(d.posts[0]);
        }
      })
      .catch(err => console.error('Sample posts error:', err));

    connectSSE();

    return () => {
      if (sseRef.current) sseRef.current.close();
      if (reconnectTimeoutRef.current) clearTimeout(reconnectTimeoutRef.current);
    };
  }, [isPaused, region]);

  // 5. Trigger Real Solver Allocation
  const handleRunAllocation = () => {
    setAllocLoading(true);
    fetch('/api/allocate', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        demands: posts.slice(0, 50),
        region: region,
        resource_type: REGIONS[region].resource,
        policy: allocPolicy
      })
    })
      .then(res => res.json())
      .then(data => {
        setAllocResult(data);
        setAllocLoading(false);
      })
      .catch(err => {
        console.error('Allocation error:', err);
        setAllocLoading(false);
      });
  };

  // 6. Demo Runner
  const handleRunDemoStep = (stepNumber) => {
    setDemoLoading(prev => ({ ...prev, [stepNumber]: true }));
    fetch(`/api/demo/step?step=${stepNumber}`, { method: 'POST' })
      .then(res => res.json())
      .then(data => {
        setDemoOutputs(prev => ({ ...prev, [stepNumber]: data }));
        setDemoLoading(prev => ({ ...prev, [stepNumber]: false }));
      })
      .catch(err => {
        setDemoOutputs(prev => ({
          ...prev,
          [stepNumber]: { status: 'FAIL', output: String(err), command: 'make demo-step' }
        }));
        setDemoLoading(prev => ({ ...prev, [stepNumber]: false }));
      });
  };

  const handleExportEvidence = () => {
    fetch('/api/demo/export', { method: 'POST' })
      .then(res => res.json())
      .then(d => setExportStatus(`Exported: ${d.evidence_file}`))
      .catch(err => setExportStatus(`Export failed: ${String(err)}`));
  };

  // Compute status badge
  const kafkaUp = health?.services?.kafka?.status === 'OPERATIONAL';
  const dataState = !kafkaUp ? 'OFFLINE' : streamStatus === 'LIVE' ? (isPaused ? 'STALE' : 'LIVE') : 'REPLAY';

  return (
    <div>
      {/* Header Bar */}
      <header className="app-header">
        <div className="brand-section">
          <span className="brand-title">CrisisGuard AI</span>
          <div className="badge-group">
            <span className={`badge badge-${dataState.toLowerCase()}`}>
              {dataState}
            </span>
            <span className="badge badge-simulated" title="Coordinates mapped to regional bounding box; synthetic hoaxes generated via template perturbation.">
              SIMULATED
            </span>
          </div>
        </div>

        <div className="header-controls">
          <nav className="nav-tabs" role="tablist">
            {['live', 'models', 'allocation', 'pipeline', 'demo'].map(tab => (
              <button
                key={tab}
                className={`nav-tab-btn ${activeTab === tab ? 'active' : ''}`}
                onClick={() => setActiveTab(tab)}
              >
                {tab.charAt(0).toUpperCase() + tab.slice(1)}
              </button>
            ))}
          </nav>

          <button className="theme-toggle-btn" onClick={toggleTheme}>
            {theme === 'dark' ? 'Light mode' : 'Dark mode'}
          </button>
        </div>
      </header>

      {/* Main Container */}
      <main className="main-container">
        {/* ========================================================= */}
        {/* TAB 1: LIVE MONITOR */}
        {/* ========================================================= */}
        {activeTab === 'live' && (
          <div>
            <div className="page-header">
              <h1 className="page-title">Live Disaster Operations Feed</h1>
              <p className="page-desc">
                Continuous ingestion of social distress messages with automated MLlib synthetic classification and trust scoring.
              </p>
            </div>

            <div className="live-layout">
              {/* Map Left (60%) */}
              <div className="map-container-wrapper">
                <div className="map-toolbar">
                  <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                    <label style={{ fontSize: '12px', color: 'var(--text-secondary)' }}>Region:</label>
                    <select
                      className="select-control"
                      value={region}
                      onChange={e => setRegion(e.target.value)}
                    >
                      <option value="houston">Houston (Hurricane Harvey)</option>
                      <option value="california">Northern California (Wildfires)</option>
                      <option value="nepal">Kathmandu Valley (Earthquake)</option>
                      <option value="miami">South Florida (Tropical Storm)</option>
                    </select>
                  </div>
                  <div style={{ fontSize: '12px', color: 'var(--text-secondary)' }}>
                    Active depots: {depots.length} | Displayed posts: {posts.length}
                  </div>
                </div>
                <div ref={mapContainerRef} className="map-element" id="live-map"></div>
              </div>

              {/* Feed Right (40%) */}
              <div className="feed-container-wrapper">
                <div className="feed-toolbar">
                  <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                    <button
                      className="btn-secondary"
                      onClick={() => setIsPaused(!isPaused)}
                    >
                      {isPaused ? 'Resume stream' : 'Pause stream'}
                    </button>
                    <select
                      className="select-control"
                      value={sourceFilter}
                      onChange={e => setSourceFilter(e.target.value)}
                    >
                      <option value="all">All sources</option>
                      <option value="live">Live only</option>
                      <option value="replay">Replay only</option>
                    </select>
                  </div>
                  <span style={{ fontSize: '12px', color: 'var(--text-secondary)' }}>
                    Buffer: {posts.length}/500
                  </span>
                </div>

                <div className="feed-table-scroll">
                  <table className="data-table">
                    <thead>
                      <tr>
                        <th>Time</th>
                        <th>Text</th>
                        <th>Status</th>
                        <th>Trust</th>
                        <th>Urg</th>
                      </tr>
                    </thead>
                    <tbody>
                      {posts.length === 0 ? (
                        <tr>
                          <td colSpan="5" className="state-empty">No posts in buffer.</td>
                        </tr>
                      ) : (
                        posts.map(p => (
                          <tr
                            key={p.post_id}
                            className={selectedPost?.post_id === p.post_id ? 'selected' : ''}
                            onClick={() => setSelectedPost(p)}
                          >
                            <td style={{ whiteSpace: 'nowrap', fontSize: '11px', color: 'var(--text-secondary)' }}>
                              {p.timestamp}
                            </td>
                            <td style={{ maxWidth: '180px', overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>
                              {p.text}
                            </td>
                            <td>
                              <span className={`status-chip ${p.is_flagged_misinformation === 1 ? 'status-chip-synthetic' : 'status-chip-credible'}`}>
                                {p.is_flagged_misinformation === 1 ? 'Suspected' : 'Credible'}
                              </span>
                            </td>
                            <td>{Number(p.trust_score).toFixed(3)}</td>
                            <td>{Number(p.urgency_level).toFixed(1)}</td>
                          </tr>
                        ))
                      )}
                    </tbody>
                  </table>
                </div>

                {/* Side Drawer for Details */}
                {selectedPost && (
                  <div className="side-drawer">
                    <div className="side-drawer-header">
                      <span>Post inspection: {selectedPost.post_id}</span>
                      <span className={`status-chip ${selectedPost.is_flagged_misinformation === 1 ? 'status-chip-synthetic' : 'status-chip-credible'}`}>
                        {selectedPost.status_label}
                      </span>
                    </div>
                    <div className="drawer-text">"{selectedPost.text}"</div>
                    <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: '8px', fontSize: '12px', marginBottom: '8px' }}>
                      <div><strong>Urgency:</strong> {selectedPost.urgency_level} / 5.0</div>
                      <div><strong>Synthetic prob:</strong> {Number(selectedPost.prob_synthetic_score).toFixed(4)}</div>
                      <div><strong>Category:</strong> {selectedPost.humanitarian_category || 'Rescue Need'}</div>
                      <div><strong>Source:</strong> {selectedPost.source}</div>
                    </div>
                    <div>
                      <strong style={{ fontSize: '12px' }}>Score factors:</strong>
                      <ul className="factors-list">
                        {(selectedPost.top_factors || []).map((factor, idx) => (
                          <li key={idx}>{factor}</li>
                        ))}
                      </ul>
                    </div>
                  </div>
                )}
              </div>
            </div>
          </div>
        )}

        {/* ========================================================= */}
        {/* TAB 2: MODELS */}
        {/* ========================================================= */}
        {activeTab === 'models' && (
          <div>
            <div className="page-header">
              <h1 className="page-title">Model Specifications & Validation Metrics</h1>
              <p className="page-desc">
                Offline evaluation metrics across 75/25 random stratified split and held-out cross-validation partitions.
              </p>
            </div>

            {/* Model Catalog Table */}
            <div className="panel">
              <div className="panel-header">
                <span className="panel-title">Production Models Catalog</span>
              </div>
              <div style={{ overflowX: 'auto' }}>
                <table className="data-table">
                  <thead>
                    <tr>
                      <th>Task</th>
                      <th>Architecture</th>
                      <th>Training Data</th>
                      <th>Primary Metric</th>
                      <th>In-Distribution</th>
                      <th>Unseen-Event / Generalization</th>
                    </tr>
                  </thead>
                  <tbody>
                    {modelsData?.models?.map((m, idx) => (
                      <tr key={idx}>
                        <td><strong>{m.task}</strong></td>
                        <td>{m.model_type}</td>
                        <td>{m.training_corpus}</td>
                        <td>{m.primary_metric}</td>
                        <td>{m.in_distribution}</td>
                        <td>{m.unseen_event_score}</td>
                      </tr>
                    )) || (
                      <tr><td colSpan="6" className="state-loading">Loading model definitions...</td></tr>
                    )}
                  </tbody>
                </table>
              </div>
            </div>

            {/* Two-Column: Charts & Confusion Matrix */}
            <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: '16px', marginBottom: '16px' }}>
              {/* ROC & PR Curve Visualizations */}
              <div className="panel">
                <div className="panel-header">
                  <span className="panel-title">Tweet Misinformation Detector (ROC & PR)</span>
                </div>
                <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: '16px' }}>
                  {/* ROC Curve */}
                  <div>
                    <div style={{ fontSize: '12px', fontWeight: 600, marginBottom: '6px', textAlign: 'center' }}>
                      ROC Curve (AUC = 0.9940)
                    </div>
                    <svg viewBox="0 0 160 120" className="chart-svg">
                      <line x1="20" y1="100" x2="150" y2="100" className="chart-axis" />
                      <line x1="20" y1="10" x2="20" y2="100" className="chart-axis" />
                      {/* Grid lines */}
                      <line x1="20" y1="55" x2="150" y2="55" className="chart-grid" />
                      <line x1="85" y1="10" x2="85" y2="100" className="chart-grid" />
                      {/* Diagonal chance */}
                      <line x1="20" y1="100" x2="150" y2="10" stroke="#64748b" strokeWidth="1" strokeDasharray="3,3" />
                      {/* ROC Curve */}
                      <polyline
                        points="20,100 22,40 25,25 35,15 50,11 150,10"
                        className="chart-line"
                      />
                      <text x="25" y="112" className="chart-text">0.0 FPR</text>
                      <text x="130" y="112" className="chart-text">1.0</text>
                      <text x="5" y="18" className="chart-text">1.0</text>
                      <text x="5" y="98" className="chart-text">0.0</text>
                    </svg>
                    <div style={{ fontSize: '11px', color: 'var(--text-secondary)', textAlign: 'center', marginTop: '4px' }}>
                      Recall @ 1% FPR = 76.75% | Recall @ 5% FPR = 100%
                    </div>
                  </div>

                  {/* PR Curve */}
                  <div>
                    <div style={{ fontSize: '12px', fontWeight: 600, marginBottom: '6px', textAlign: 'center' }}>
                      Precision-Recall Curve (PR-AUC = 0.9007)
                    </div>
                    <svg viewBox="0 0 160 120" className="chart-svg">
                      <line x1="20" y1="100" x2="150" y2="100" className="chart-axis" />
                      <line x1="20" y1="10" x2="20" y2="100" className="chart-axis" />
                      <line x1="20" y1="55" x2="150" y2="55" className="chart-grid" />
                      <line x1="85" y1="10" x2="85" y2="100" className="chart-grid" />
                      <polyline
                        points="20,15 70,18 100,22 135,28 145,95 150,95"
                        className="chart-line"
                      />
                      <text x="25" y="112" className="chart-text">0.0 Recall</text>
                      <text x="130" y="112" className="chart-text">1.0</text>
                      <text x="5" y="18" className="chart-text">1.0</text>
                      <text x="5" y="98" className="chart-text">0.0</text>
                    </svg>
                    <div style={{ fontSize: '11px', color: 'var(--text-secondary)', textAlign: 'center', marginTop: '4px' }}>
                      Precision @ Threshold 0.1786 = 92.00%
                    </div>
                  </div>
                </div>
              </div>

              {/* Confusion Matrix */}
              <div className="panel">
                <div className="panel-header">
                  <span className="panel-title">Confusion Matrix (Calibrated Threshold &theta;* = 0.1786)</span>
                </div>
                <table className="data-table" style={{ marginTop: '8px' }}>
                  <thead>
                    <tr>
                      <th>Actual \ Predicted</th>
                      <th>Predicted Synthetic</th>
                      <th>Predicted Genuine</th>
                    </tr>
                  </thead>
                  <tbody>
                    <tr>
                      <td><strong>Actual Synthetic (Gen C)</strong></td>
                      <td><span style={{ color: 'var(--status-success-text)', fontWeight: 600 }}>23 (TP)</span></td>
                      <td>1,332 (FN)</td>
                    </tr>
                    <tr>
                      <td><strong>Actual Genuine</strong></td>
                      <td><span style={{ color: 'var(--status-danger-text)', fontWeight: 600 }}>2 (FP)</span></td>
                      <td>31,500 (TN)</td>
                    </tr>
                  </tbody>
                </table>
                <div style={{ fontSize: '12px', color: 'var(--text-secondary)', marginTop: '12px' }}>
                  <strong>Operational Operating Point:</strong> Calibrated threshold &theta;* = 0.1786 prioritizes ultra-high precision (0.9200) to protect civilian distress calls from wrongful suppression, tolerating false negatives over false positives.
                </div>
              </div>
            </div>

            {/* Known Limits Box */}
            <div className="panel">
              <div className="panel-header">
                <span className="panel-title">Known Limitations & Robustness Controls</span>
              </div>
              <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr 1fr', gap: '16px', fontSize: '13px' }}>
                <div className="callout-box" style={{ margin: 0 }}>
                  <strong>Own Hoax Generators:</strong><br />
                  Generators A, B, and C are our own synthesized rule-based perturbation templates with slot perturbations (no external LLM was used). High AUC is an artifact of template phrasing contrasting against casual tweets.
                </div>
                <div className="callout-box" style={{ margin: 0 }}>
                  <strong>Random-Split Optimism:</strong><br />
                  The corpus split is a random stratified split inside each source dataset. Tweets from the same disaster appear in both train and test partitions. Generalization to entirely unseen disasters exhibits an honest -0.198 Macro-F1 penalty.
                </div>
                <div className="callout-box" style={{ margin: 0 }}>
                  <strong>Length Shortcut Checks:</strong><br />
                  A length-only baseline achieves an AUC of 0.6685, and stylometrics alone achieve 0.7820, confirming that the full TF-IDF model (0.9940) does not merely discriminate based on character count.
                </div>
              </div>
            </div>
          </div>
        )}

        {/* ========================================================= */}
        {/* TAB 3: ALLOCATION */}
        {/* ========================================================= */}
        {activeTab === 'allocation' && (
          <div>
            <div className="page-header">
              <h1 className="page-title">Trust-Aware Emergency Resource Allocation</h1>
              <p className="page-desc">
                Linear and Mixed-Integer Linear Programming dispatch solver comparing trust-blind against trust-aware objective weighting.
              </p>
            </div>

            {/* Controls Bar */}
            <div className="panel" style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', gap: '16px', flexWrap: 'wrap' }}>
              <div style={{ display: 'flex', alignItems: 'center', gap: '12px' }}>
                <label style={{ fontSize: '13px', fontWeight: 600 }}>Region:</label>
                <select
                  className="select-control"
                  value={region}
                  onChange={e => setRegion(e.target.value)}
                >
                  <option value="houston">Houston (52 Rescue Boats)</option>
                  <option value="california">Northern California (52 Wildfire Units)</option>
                  <option value="nepal">Kathmandu (52 Rescue Teams)</option>
                  <option value="miami">South Florida (54 Ambulances)</option>
                </select>

                <label style={{ fontSize: '13px', fontWeight: 600 }}>Dispatch Policy:</label>
                <select
                  className="select-control"
                  value={allocPolicy}
                  onChange={e => setAllocPolicy(e.target.value)}
                >
                  <option value="blind_lp">Blind LP (Urgency Only, Trust-Blind)</option>
                  <option value="trust_aware_continuous">Trust-Aware Continuous (1 - p_synth)</option>
                  <option value="trust_aware_validation_thresh">Trust-Aware Validation Threshold (&theta;* = 0.1786)</option>
                  <option value="oracle">Oracle Upper Bound (Ground Truth Knowledge)</option>
                </select>
              </div>

              <button
                className="btn-primary"
                onClick={handleRunAllocation}
                disabled={allocLoading}
              >
                {allocLoading ? 'Solving MILP...' : 'Run allocation'}
              </button>
            </div>

            {/* Solver Real-Time Result */}
            {allocResult && (
              <div className="panel">
                <div className="panel-header">
                  <span className="panel-title">Solver Output (Current Window)</span>
                  <span style={{ fontSize: '12px', color: 'var(--text-secondary)' }}>
                    Solved in {allocResult.solve_time_ms} ms via PuLP CBC Optimizer
                  </span>
                </div>
                <div style={{ display: 'grid', gridTemplateColumns: 'repeat(5, 1fr)', gap: '12px', textAlign: 'center', marginBottom: '14px' }}>
                  <div className="panel" style={{ margin: 0, padding: '10px' }}>
                    <div style={{ fontSize: '11px', color: 'var(--text-secondary)' }}>Genuine victims served</div>
                    <div style={{ fontSize: '18px', fontWeight: 600, color: 'var(--status-success-text)' }}>
                      {allocResult.genuine_served}
                    </div>
                  </div>
                  <div className="panel" style={{ margin: 0, padding: '10px' }}>
                    <div style={{ fontSize: '11px', color: 'var(--text-secondary)' }}>Wasted dispatches to hoaxes</div>
                    <div style={{ fontSize: '18px', fontWeight: 600, color: allocResult.wasted_dispatches > 0 ? 'var(--status-danger-text)' : 'var(--text-primary)' }}>
                      {allocResult.wasted_dispatches}
                    </div>
                  </div>
                  <div className="panel" style={{ margin: 0, padding: '10px' }}>
                    <div style={{ fontSize: '11px', color: 'var(--text-secondary)' }}>Wasted dispatch rate</div>
                    <div style={{ fontSize: '18px', fontWeight: 600 }}>
                      {allocResult.wasted_pct}%
                    </div>
                  </div>
                  <div className="panel" style={{ margin: 0, padding: '10px' }}>
                    <div style={{ fontSize: '11px', color: 'var(--text-secondary)' }}>Total transit distance</div>
                    <div style={{ fontSize: '18px', fontWeight: 600 }}>
                      {allocResult.total_distance_km} km
                    </div>
                  </div>
                  <div className="panel" style={{ margin: 0, padding: '10px' }}>
                    <div style={{ fontSize: '11px', color: 'var(--text-secondary)' }}>Total assigned units</div>
                    <div style={{ fontSize: '18px', fontWeight: 600 }}>
                      {allocResult.total_served}
                    </div>
                  </div>
                </div>

                <div style={{ maxHeight: '240px', overflowY: 'auto' }}>
                  <table className="data-table">
                    <thead>
                      <tr>
                        <th>Demand ID</th>
                        <th>Assigned Depot</th>
                        <th>Distance (km)</th>
                        <th>Urgency</th>
                        <th>Trust Weight</th>
                        <th>Outcome Status</th>
                      </tr>
                    </thead>
                    <tbody>
                      {allocResult.assignments.map((a, idx) => (
                        <tr key={idx}>
                          <td>{a.demand_id}</td>
                          <td>{a.depot_name}</td>
                          <td>{a.distance_km} km</td>
                          <td>{a.urgency}</td>
                          <td>{Number(a.trust).toFixed(3)}</td>
                          <td>
                            <span className={`status-chip ${a.is_synthetic === 1 ? 'status-chip-synthetic' : 'status-chip-credible'}`}>
                              {a.status}
                            </span>
                          </td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              </div>
            )}

            {/* Offline 30-Seed Comparison Study Bar Chart */}
            <div className="panel">
              <div className="panel-header">
                <span className="panel-title">Offline 30-Seed Policy Evaluation (Houston Flood)</span>
                <span style={{ fontSize: '12px', color: 'var(--text-secondary)' }}>
                  Offline experiment: 30 random seeds, 120 demands/seed, 52 rescue boat capacity
                </span>
              </div>

              <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: '24px', alignItems: 'center' }}>
                {/* SVG Bar Chart with 95% CIs */}
                <div>
                  <svg viewBox="0 0 320 180" className="chart-svg">
                    <line x1="40" y1="150" x2="300" y2="150" className="chart-axis" />
                    <line x1="40" y1="20" x2="40" y2="150" className="chart-axis" />
                    {/* Y-axis grid lines */}
                    <line x1="40" y1="124" x2="300" y2="124" className="chart-grid" />
                    <text x="15" y="128" className="chart-text">10</text>
                    <line x1="40" y1="98" x2="300" y2="98" className="chart-grid" />
                    <text x="15" y="102" className="chart-text">20</text>
                    <line x1="40" y1="72" x2="300" y2="72" className="chart-grid" />
                    <text x="15" y="76" className="chart-text">30</text>
                    <line x1="40" y1="46" x2="300" y2="46" className="chart-grid" />
                    <text x="15" y="50" className="chart-text">40</text>
                    <line x1="40" y1="20" x2="300" y2="20" className="chart-grid" />
                    <text x="15" y="24" className="chart-text">50</text>

                    {/* Bar 1: Blind LP (28.00 victims, height = 28 * 2.6 = 72.8, y = 150 - 72.8 = 77.2) */}
                    <rect x="65" y="77" width="55" height="73" fill="#64748b" rx="2" />
                    <text x="75" y="70" className="chart-text" fill="var(--text-primary)" fontWeight="bold">28.00</text>
                    <line x1="92" y1="77" x2="92" y2="77" stroke="#ffffff" strokeWidth="2" />

                    {/* Bar 2: Trust-Aware Val Thresh (28.63 victims, height = 28.63 * 2.6 = 74.4, y = 75.6) */}
                    <rect x="145" y="75" width="55" height="75" fill="#2563eb" rx="2" />
                    <text x="155" y="68" className="chart-text" fill="var(--text-primary)" fontWeight="bold">28.63</text>
                    {/* 95% CI error bar: 28.63 ± 0.32 (y = 74.8 to 76.4) */}
                    <line x1="172" y1="73" x2="172" y2="78" stroke="#ffffff" strokeWidth="2" />
                    <line x1="168" y1="73" x2="176" y2="73" stroke="#ffffff" strokeWidth="2" />
                    <line x1="168" y1="78" x2="176" y2="78" stroke="#ffffff" strokeWidth="2" />

                    {/* Bar 3: Oracle Upper Bound (52.00 victims, height = 52 * 2.6 = 135.2, y = 14.8) */}
                    <rect x="225" y="15" width="55" height="135" fill="#15803d" rx="2" />
                    <text x="235" y="10" className="chart-text" fill="var(--text-primary)" fontWeight="bold">52.00</text>

                    {/* Labels */}
                    <text x="65" y="165" className="chart-text">Blind LP</text>
                    <text x="142" y="165" className="chart-text">Trust-Aware</text>
                    <text x="230" y="165" className="chart-text">Oracle</text>
                  </svg>
                </div>

                {/* Table of Metrics */}
                <div>
                  <table className="data-table">
                    <thead>
                      <tr>
                        <th>Policy</th>
                        <th>Genuine Rescued (Mean ± 95% CI)</th>
                        <th>Stat Gain (p-value)</th>
                        <th>Oracle Headroom</th>
                      </tr>
                    </thead>
                    <tbody>
                      <tr>
                        <td><strong>Blind LP</strong></td>
                        <td>28.00 ± 0.00</td>
                        <td>Baseline (0.0)</td>
                        <td>0.0%</td>
                      </tr>
                      <tr>
                        <td><strong>Trust-Aware (Val Thresh)</strong></td>
                        <td><strong>28.63 ± 0.32</strong></td>
                        <td><span style={{ color: 'var(--status-success-text)' }}>+0.63 (p = 0.00053)</span></td>
                        <td>2.6% of Oracle</td>
                      </tr>
                      <tr>
                        <td><strong>Trust-Aware (Continuous)</strong></td>
                        <td>28.00 ± 0.00</td>
                        <td>+0.00 (p = 1.000)</td>
                        <td>0.0%</td>
                      </tr>
                      <tr>
                        <td><strong>Oracle Upper Bound</strong></td>
                        <td>52.00 ± 0.00</td>
                        <td>+24.00 (Theoretical)</td>
                        <td>100.0%</td>
                      </tr>
                    </tbody>
                  </table>
                </div>
              </div>

              {/* Two-Sentence Box */}
              <div className="callout-box">
                <strong>What this shows:</strong> Calibrated thresholding achieves a statistically significant dispatch gain (+0.63 victims, p = 0.00053) by withholding dispatches to high-confidence synthetic hoaxes without suppressing genuine calls.
                <br />
                <strong>What it does not show:</strong> This does not imply immunity against adaptive human-written hoaxes or prove performance under dynamic communication outages and infrastructure destruction.
              </div>
            </div>
          </div>
        )}

        {/* ========================================================= */}
        {/* TAB 4: PIPELINE */}
        {/* ========================================================= */}
        {activeTab === 'pipeline' && (
          <div>
            <div className="page-header">
              <h1 className="page-title">Big Data Infrastructure & Resiliency Benchmark</h1>
              <p className="page-desc">
                Daemon operational status, write-ahead log recovery verification, and linear scaling telemetry across cluster nodes.
              </p>
            </div>

            {/* Component Status Table */}
            <div className="panel">
              <div className="panel-header">
                <span className="panel-title">Cluster Core Daemons</span>
                <span style={{ fontSize: '12px', color: 'var(--text-secondary)' }}>
                  As of: {health?.as_of || 'Just now'}
                </span>
              </div>
              <table className="data-table">
                <thead>
                  <tr>
                    <th>Component</th>
                    <th>Network Address / Identifier</th>
                    <th>Status</th>
                    <th>Last Message / Heartbeat</th>
                    <th>Key Metric</th>
                  </tr>
                </thead>
                <tbody>
                  {health?.services && Object.entries(health.services).map(([key, s]) => (
                    <tr key={key}>
                      <td><strong>{s.name}</strong></td>
                      <td>{s.host || s.path || s.port || 'In-Process'}</td>
                      <td>
                        <span className={`status-chip ${s.status === 'OPERATIONAL' ? 'status-chip-credible' : 'status-chip-synthetic'}`}>
                          {s.status}
                        </span>
                      </td>
                      <td>{s.last_seen || 'N/A'}</td>
                      <td><strong>{s.key_metric}</strong></td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>

            {/* Two Benchmark Tables: Crash Recovery and Scaling */}
            <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: '16px' }}>
              {/* Crash-Recovery Table */}
              <div className="panel">
                <div className="panel-header">
                  <span className="panel-title">Fault-Tolerance & Crash-Recovery Study</span>
                </div>
                <table className="data-table">
                  <thead>
                    <tr>
                      <th>Experiment Metric</th>
                      <th>Measured Result</th>
                      <th>Protocol / Mechanism</th>
                    </tr>
                  </thead>
                  <tbody>
                    <tr>
                      <td>Injected Failure</td>
                      <td>SIGKILL worker node</td>
                      <td>Chaos fault injection during streaming batch</td>
                    </tr>
                    <tr>
                      <td>Recovery Latency</td>
                      <td><strong>2.83 seconds</strong></td>
                      <td>HDFS write-ahead log state restoration</td>
                    </tr>
                    <tr>
                      <td>Message Loss</td>
                      <td><strong>0 events (0.00%)</strong></td>
                      <td>Kafka consumer group offset replay</td>
                    </tr>
                    <tr>
                      <td>Partition Consistency</td>
                      <td>Verified 100% atomic</td>
                      <td>Spark Catalyst atomic Parquet file committer</td>
                    </tr>
                  </tbody>
                </table>
              </div>

              {/* Scaling Benchmark Table */}
              <div className="panel">
                <div className="panel-header">
                  <span className="panel-title">Empirical Volume Scaling Benchmark</span>
                </div>
                <table className="data-table">
                  <thead>
                    <tr>
                      <th>Dataset Volume</th>
                      <th>Processing Time</th>
                      <th>Sustained Throughput</th>
                      <th>Complexity</th>
                    </tr>
                  </thead>
                  <tbody>
                    <tr>
                      <td>1,000,000 rows</td>
                      <td>14.20 s</td>
                      <td>70,422 rows/sec</td>
                      <td>Baseline O(N)</td>
                    </tr>
                    <tr>
                      <td>5,000,000 rows</td>
                      <td>68.50 s</td>
                      <td>72,992 rows/sec</td>
                      <td>Near-perfect linear</td>
                    </tr>
                    <tr>
                      <td>10,000,000 rows</td>
                      <td>139.10 s</td>
                      <td>71,890 rows/sec</td>
                      <td>Linear O(N) scaling</td>
                    </tr>
                  </tbody>
                </table>
              </div>
            </div>
          </div>
        )}

        {/* ========================================================= */}
        {/* TAB 5: DEMO */}
        {/* ========================================================= */}
        {activeTab === 'demo' && (
          <div>
            <div className="page-header" style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'flex-start' }}>
              <div>
                <h1 className="page-title">Live Pipeline Execution Walkthrough</h1>
                <p className="page-desc">
                  Interactive step-by-step verification of real cluster operations from Kafka ingestion to MILP dispatch.
                </p>
              </div>
              <div style={{ display: 'flex', flexDirection: 'column', alignItems: 'flex-end', gap: '6px' }}>
                <button className="btn-secondary" onClick={handleExportEvidence}>
                  Export evidence
                </button>
                {exportStatus && (
                  <span style={{ fontSize: '11px', color: 'var(--status-success-text)' }}>
                    {exportStatus}
                  </span>
                )}
              </div>
            </div>

            {/* List of 6 Steps */}
            {[
              { num: 1, title: 'Publish Test Event Batch to Apache Kafka', desc: 'Produces disaster alert payloads to topic disaster_raw_posts via ingestion simulator.' },
              { num: 2, title: 'Spark Structured Streaming Scoring & Feature Transform', desc: 'Processes micro-batch, applies TF-IDF and stylometrics, computes probability of synthetic hoax.' },
              { num: 3, title: 'Atomic Parquet Partition Materialization in HDFS', desc: 'Writes scored silver records to Hadoop distributed filesystem with write-ahead logs.' },
              { num: 4, title: 'Query Apache Hive Metastore Catalog via Spark SQL', desc: 'Queries table cse412_disaster.disaster_posts_silver catalog through Spark SQL engine.' },
              { num: 5, title: 'Stream Delivery Verification & Latency Tracking', desc: 'Validates Server-Sent Events transport and measures end-to-end event latency.' },
              { num: 6, title: 'Solve Multi-Depot Trust-Aware MILP Allocation', desc: 'Executes PuLP CBC optimizer to dispatch finite emergency assets and calculate saved victims.' }
            ].map(step => (
              <div key={step.num} className="panel" style={{ marginBottom: '14px' }}>
                <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: '8px' }}>
                  <div>
                    <strong>Step {step.num}: {step.title}</strong>
                    <div style={{ fontSize: '12px', color: 'var(--text-secondary)' }}>{step.desc}</div>
                  </div>
                  <button
                    className="btn-primary"
                    onClick={() => handleRunDemoStep(step.num)}
                    disabled={demoLoading[step.num]}
                  >
                    {demoLoading[step.num] ? 'Running...' : 'Run step'}
                  </button>
                </div>

                {demoOutputs[step.num] && (
                  <div>
                    <div style={{ fontSize: '11px', color: 'var(--text-secondary)', marginTop: '4px' }}>
                      Command: <code>{demoOutputs[step.num].command}</code> | Status: <strong>{demoOutputs[step.num].status}</strong>
                    </div>
                    <pre className="terminal-output">{demoOutputs[step.num].output}</pre>
                  </div>
                )}
              </div>
            ))}
          </div>
        )}
      </main>
    </div>
  );
}
