import React, { useState, useEffect, useRef, useCallback } from 'react';
import Header from './components/Header';
import SettingsDrawer from './components/SettingsDrawer';
import LivePage from './pages/LivePage';
import ModelsPage from './pages/ModelsPage';
import AllocationPage from './pages/AllocationPage';
import PipelinePage from './pages/PipelinePage';
import DemoPage from './pages/DemoPage';
import { api, SSEClient } from './api/client';
import { safeLocalStorageGet, safeLocalStorageSet } from './utils/formatters';
import { INITIAL_SAMPLE_POSTS } from './utils/initialPosts';

const DEFAULT_SETTINGS = {
  theme: 'dark',
  density: 'comfortable',
  timezone: 'local',
  units: 'km',
  streamRate: 1.0,
  visibleColumns: ['time', 'event', 'text', 'urgency', 'p_synth', 'trust', 'verdict']
};

export default function App() {
  // Query parameters on initial load
  const params = typeof window !== 'undefined' ? new URLSearchParams(window.location.search) : null;
  const initialTab = params?.get('tab') || 'live';
  const initialTheme = params?.get('theme') || 'dark';

  const [activeTab, setActiveTab] = useState(initialTab);
  const [theme, setTheme] = useState(() => {
    const saved = safeLocalStorageGet('crisisguard_settings', DEFAULT_SETTINGS);
    return params?.get('theme') || saved.theme || initialTheme;
  });

  const [settings, setSettings] = useState(() => {
    return safeLocalStorageGet('crisisguard_settings', DEFAULT_SETTINGS);
  });

  const [settingsOpen, setSettingsOpen] = useState(false);
  const [region, setRegion] = useState(params?.get('region') || 'houston');
  const [isPaused, setIsPaused] = useState(false);
  const isPausedRef = useRef(isPaused);
  isPausedRef.current = isPaused;

  // Stream & Telemetry states
  const [health, setHealth] = useState(null);
  const [summary, setSummary] = useState(null);
  const [modelsData, setModelsData] = useState(null);
  const [depots, setDepots] = useState([
    {"id": "hou_downtown", "name": "Downtown Central Marina Base", "latitude": 29.7560, "longitude": -95.3620, "capacities": {"rescue_boats": 15}},
    {"id": "hou_katy", "name": "West Sector Rescue Outpost (Katy)", "latitude": 29.7858, "longitude": -95.8245, "capacities": {"rescue_boats": 12}},
    {"id": "hou_sugarland", "name": "South Sector Marine Staging", "latitude": 29.6197, "longitude": -95.6349, "capacities": {"rescue_boats": 10}},
    {"id": "hou_cypress", "name": "North Sector Boat Depot", "latitude": 29.9688, "longitude": -95.6972, "capacities": {"rescue_boats": 15}}
  ]);
  const [posts, setPosts] = useState(INITIAL_SAMPLE_POSTS);
  const [selectedPost, setSelectedPost] = useState(null);

  // SSE & State Logic
  const [sseState, setSseState] = useState('OPEN');
  const [lastEventTime, setLastEventTime] = useState(() => new Date().toISOString());
  const [streamRate, setStreamRate] = useState(settings.streamRate || 1.0);
  const [hoaxRate, setHoaxRate] = useState(0.20);
  const sseClientRef = useRef(null);

  // Update theme on document
  useEffect(() => {
    document.documentElement.setAttribute('data-theme', theme);
  }, [theme]);

  const toggleTheme = () => {
    const next = theme === 'dark' ? 'light' : 'dark';
    setTheme(next);
    const updated = { ...settings, theme: next };
    setSettings(updated);
    safeLocalStorageSet('crisisguard_settings', updated);
  };

  const handleUpdateSettings = (newSettings) => {
    setSettings(newSettings);
    if (newSettings.theme && newSettings.theme !== theme) {
      setTheme(newSettings.theme);
    }
    if (newSettings.streamRate && newSettings.streamRate !== streamRate) {
      setStreamRate(newSettings.streamRate);
      sseClientRef.current?.updateConfig({ rate: newSettings.streamRate });
    }
    safeLocalStorageSet('crisisguard_settings', newSettings);
  };

  // Sync tab with URL
  const handleSelectTab = (tabId) => {
    setActiveTab(tabId);
    try {
      const url = new URL(window.location.href);
      url.searchParams.set('tab', tabId);
      window.history.replaceState({}, '', url.toString());
    } catch {
      // ignore
    }
  };

  // 1. Initial Data Fetch (Health, Summary, Models)
  const refreshHealth = useCallback(() => {
    api.getHealth()
      .then(d => setHealth(d))
      .catch(() => setHealth({ status: 'OFFLINE', services: {} }));
  }, []);

  useEffect(() => {
    refreshHealth();
    api.getSummary().then(d => setSummary(d)).catch(err => console.error(err));
    api.getModels().then(d => setModelsData(d)).catch(err => console.error(err));
  }, [refreshHealth]);

  // 2. Fetch Depots and Initial Sample Posts when Region changes
  useEffect(() => {
    api.getDepots(region)
      .then(d => setDepots(d.depots || []))
      .catch(err => console.error(err));

    api.getSamplePosts({ region, limit: 30, hoax_rate: hoaxRate })
      .then(d => {
        if (d.posts && d.posts.length > 0) {
          setPosts(d.posts);
          setLastEventTime(new Date().toISOString());
        }
      })
      .catch(err => console.error(err));
  }, [region]);

  // 3. Setup SSE Stream Client
  useEffect(() => {
    const handleNewPost = (post) => {
      setLastEventTime(new Date().toISOString());
      if (isPausedRef.current) return;

      setPosts((prevPosts) => {
        // Deduplicate by post_id
        const filtered = prevPosts.filter(p => p.post_id !== post.post_id);
        const next = [post, ...filtered];
        return next.slice(0, 500); // buffer cap
      });
    };

    const client = new SSEClient({
      onMessage: handleNewPost,
      onStatusChange: (status) => setSseState(status),
      rate: streamRate,
      hoaxRate: hoaxRate,
      region
    });

    sseClientRef.current = client;
    client.connect();

    return () => {
      client.disconnect();
    };
  }, [region]);

  // Update stream config (rate, hoax rate)
  const handleUpdateStreamConfig = ({ rate, hoaxRate: hRate }) => {
    if (rate !== undefined) setStreamRate(rate);
    if (hRate !== undefined) setHoaxRate(hRate);
    sseClientRef.current?.updateConfig({
      rate: rate !== undefined ? rate : streamRate,
      hoaxRate: hRate !== undefined ? hRate : hoaxRate,
      region
    });
  };

  // State Badge Logic
  // LIVE only if connected and received message in last 6 seconds; REPLAY while simulator streams; STALE/OFFLINE otherwise
  const streamBadgeState = (() => {
    if (sseState === 'OFFLINE') return posts.length > 0 ? 'STALE' : 'OFFLINE';
    if (!lastEventTime) return 'CONNECTING';
    const elapsed = Date.now() - new Date(lastEventTime).getTime();
    if (elapsed > 8000) return 'STALE';
    return 'REPLAY'; // Replay streaming simulator is active
  })();

  return (
    <div className="app-root">
      {/* Universal 1-Header on every page */}
      <Header
        activeTab={activeTab}
        setActiveTab={handleSelectTab}
        streamState={streamBadgeState}
        isSimulated={true}
        onOpenSettings={() => setSettingsOpen(true)}
        theme={theme}
        onToggleTheme={toggleTheme}
      />

      {/* Main Content Pages */}
      <main className="app-main-content">
        {activeTab === 'live' && (
          <LivePage
            region={region}
            setRegion={setRegion}
            depots={depots}
            posts={posts}
            streamState={streamBadgeState}
            lastEventTime={lastEventTime}
            isPaused={isPaused}
            setIsPaused={setIsPaused}
            onUpdateStreamConfig={handleUpdateStreamConfig}
            streamRate={streamRate}
            hoaxRate={hoaxRate}
            settings={settings}
            selectedPost={selectedPost}
            setSelectedPost={setSelectedPost}
          />
        )}

        {activeTab === 'models' && (
          <ModelsPage
            modelsData={modelsData}
          />
        )}

        {activeTab === 'allocation' && (
          <AllocationPage
            region={region}
            setRegion={setRegion}
            depots={depots}
            settings={settings}
          />
        )}

        {activeTab === 'pipeline' && (
          <PipelinePage
            health={health}
            onRefreshHealth={refreshHealth}
            settings={settings}
          />
        )}

        {activeTab === 'demo' && (
          <DemoPage />
        )}
      </main>

      {/* Settings Drawer */}
      <SettingsDrawer
        isOpen={settingsOpen}
        onClose={() => setSettingsOpen(false)}
        settings={settings}
        onUpdateSettings={handleUpdateSettings}
      />
    </div>
  );
}
