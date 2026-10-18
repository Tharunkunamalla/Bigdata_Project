import React, { useState, useEffect, useRef } from 'react';
import Header from './components/Header';
import LivePage from './pages/LivePage';
import AllocationPage from './pages/AllocationPage';
import PipelinePage from './pages/PipelinePage';
import ResultsPage from './pages/ResultsPage';
import { api, SSEClient } from './api/client';
import { INITIAL_SAMPLE_POSTS } from './utils/initialPosts';

export default function App() {
  const [activeTab, setActiveTab] = useState(() => {
    try {
      const t = new URLSearchParams(window.location.search).get('tab');
      return ['live', 'allocation', 'pipeline', 'results'].includes(t) ? t : 'live';
    } catch {
      return 'live';
    }
  });
  const [theme, setTheme] = useState(() => {
    try {
      const qTheme = new URLSearchParams(window.location.search).get('theme');
      if (qTheme === 'light' || qTheme === 'dark') return qTheme;
      return localStorage.getItem('crisisguard_theme') || 'dark';
    } catch {
      return 'dark';
    }
  });

  const [region, setRegion] = useState('houston');
  const [isPaused, setIsPaused] = useState(false);
  const isPausedRef = useRef(isPaused);
  isPausedRef.current = isPaused;

  const [health, setHealth] = useState(null);
  const [summary, setSummary] = useState(null);
  const [modelsData, setModelsData] = useState(null);
  const [depots, setDepots] = useState([
    { id: 'hou_downtown', name: 'Downtown Central Marina Base', latitude: 29.7560, longitude: -95.3620, capacities: { rescue_boats: 15 } },
    { id: 'hou_katy', name: 'West Sector Rescue Outpost (Katy)', latitude: 29.7858, longitude: -95.8245, capacities: { rescue_boats: 12 } },
    { id: 'hou_sugarland', name: 'South Sector Marine Staging', latitude: 29.6197, longitude: -95.6349, capacities: { rescue_boats: 10 } },
    { id: 'hou_cypress', name: 'North Sector Boat Depot', latitude: 29.9688, longitude: -95.6972, capacities: { rescue_boats: 15 } }
  ]);

  const [posts, setPosts] = useState(INITIAL_SAMPLE_POSTS);
  const [selectedPost, setSelectedPost] = useState(null);
  const [streamState, setStreamState] = useState('OPEN');
  const sseClientRef = useRef(null);

  // Apply visual theme to document
  useEffect(() => {
    document.documentElement.setAttribute('data-theme', theme);
    try {
      localStorage.setItem('crisisguard_theme', theme);
    } catch {
      // ignore
    }
  }, [theme]);

  const toggleTheme = () => {
    setTheme(prev => prev === 'dark' ? 'light' : 'dark');
  };

  // Initial data fetches
  useEffect(() => {
    api.getHealth().then(d => setHealth(d)).catch(err => console.error('Health fetch error:', err));
    api.getSummary().then(d => setSummary(d)).catch(err => console.error('Summary fetch error:', err));
    api.getModels().then(d => setModelsData(d)).catch(err => console.error('Models fetch error:', err));
  }, []);

  // Update depots when region changes
  useEffect(() => {
    api.getDepots(region)
      .then(d => {
        if (d && d.depots) setDepots(d.depots);
      })
      .catch(err => console.error('Depots fetch error:', err));
  }, [region]);

  // Connect SSE Stream
  useEffect(() => {
    const handleIncomingMessage = (newPost) => {
      if (isPausedRef.current) return;

      // Capture browser receive timestamp aligned with timeOrigin
      const browserNow = typeof performance !== 'undefined' && performance.timeOrigin
        ? performance.timeOrigin + performance.now()
        : Date.now();

      newPost.browser_receive_timestamp = new Date(browserNow).toISOString();

      setPosts(prev => {
        // Deduplicate by post_id and cap buffer at 250 records
        const filtered = prev.filter(p => p.post_id !== newPost.post_id);
        return [newPost, ...filtered].slice(0, 250);
      });
    };

    const handleStatusChange = (status) => {
      setStreamState(status === 'OPEN' ? 'LIVE' : status);
    };

    const client = new SSEClient({
      onMessage: handleIncomingMessage,
      onStatusChange: handleStatusChange,
      rate: 1.0,
      hoaxRate: 0.20,
      region
    });

    sseClientRef.current = client;
    client.connect();

    return () => {
      client.disconnect();
    };
  }, [region]);

  return (
    <div className="app-shell" style={{ display: 'flex', flexDirection: 'column', minHeight: '100vh', backgroundColor: 'var(--bg-page)' }}>
      <Header
        activeTab={activeTab}
        setActiveTab={setActiveTab}
        streamState={streamState === 'LIVE' ? 'LIVE' : (streamState === 'OPEN' ? 'LIVE' : streamState)}
        isSimulated={true}
        theme={theme}
        onToggleTheme={toggleTheme}
      />

      <main style={{ flex: 1, paddingBottom: '24px' }}>
        {activeTab === 'live' && (
          <LivePage
            region={region}
            setRegion={setRegion}
            depots={depots}
            posts={posts}
            streamState={streamState}
            isPaused={isPaused}
            setIsPaused={setIsPaused}
            selectedPost={selectedPost}
            setSelectedPost={setSelectedPost}
          />
        )}

        {activeTab === 'allocation' && (
          <AllocationPage
            region={region}
            setRegion={setRegion}
            depots={depots}
          />
        )}

        {activeTab === 'pipeline' && (
          <PipelinePage
            health={health}
          />
        )}

        {activeTab === 'results' && (
          <ResultsPage
            modelsData={modelsData}
          />
        )}
      </main>
    </div>
  );
}
