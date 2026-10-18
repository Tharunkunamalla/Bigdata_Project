import React, { useState, useEffect, useRef } from 'react';
import Header from './components/Header';
import StatusDrawer from './components/StatusDrawer';
import LivePage from './pages/LivePage';
import DispatchPage from './pages/DispatchPage';
import AboutPage from './pages/AboutPage';
import { api, SSEClient } from './api/client';

export default function App() {
  const [activeTab, setActiveTab] = useState(() => {
    try {
      const t = new URLSearchParams(window.location.search).get('tab');
      return ['live', 'dispatch', 'about'].includes(t) ? t : 'live';
    } catch {
      return 'live';
    }
  });

  const [region, setRegion] = useState('houston');
  const [isStatusDrawerOpen, setIsStatusDrawerOpen] = useState(false);
  const [health, setHealth] = useState(null);
  const [dataState, setDataState] = useState('REPLAY'); // 'LIVE', 'REPLAY', 'OFFLINE'
  const [lastMsgTime, setLastMsgTime] = useState(null);

  const [depots, setDepots] = useState([
    { id: 'hou_downtown', name: 'Downtown Central Marina Base', latitude: 29.7560, longitude: -95.3620, capacities: { rescue_boats: 15 } },
    { id: 'hou_katy', name: 'West Sector Rescue Outpost (Katy)', latitude: 29.7858, longitude: -95.8245, capacities: { rescue_boats: 12 } },
    { id: 'hou_sugarland', name: 'South Sector Marine Staging', latitude: 29.6197, longitude: -95.6349, capacities: { rescue_boats: 10 } },
    { id: 'hou_cypress', name: 'North Sector Boat Depot', latitude: 29.9688, longitude: -95.6972, capacities: { rescue_boats: 15 } }
  ]);

  const [posts, setPosts] = useState([]);
  const [selectedPost, setSelectedPost] = useState(null);
  const sseClientRef = useRef(null);

  // Poll health status for truthful status indicator & drawer
  const fetchHealth = async () => {
    try {
      const h = await api.getHealth();
      setHealth(h);
      if (h.data_state) {
        setDataState(h.data_state);
      }
      if (h.services?.kafka?.last_message) {
        setLastMsgTime(h.services.kafka.last_message);
      }
    } catch {
      setDataState('OFFLINE');
    }
  };

  useEffect(() => {
    fetchHealth();
    const interval = setInterval(fetchHealth, 5000);
    return () => clearInterval(interval);
  }, []);

  // Fetch verified regional disaster sample posts on mount and when region changes
  useEffect(() => {
    let isCancelled = false;
    api.getSamplePosts({ region, limit: 30, hoax_rate: 0.20 })
      .then((res) => {
        if (!isCancelled && res && res.posts) {
          setPosts(res.posts);
        }
      })
      .catch((err) => {
        console.error('Initial posts fetch error:', err);
      });

    return () => {
      isCancelled = true;
    };
  }, [region]);

  // Update depots when region changes
  useEffect(() => {
    api.getDepots(region)
      .then((d) => {
        if (d && d.depots) setDepots(d.depots);
      })
      .catch((err) => console.error('Depots fetch error:', err));
  }, [region]);

  const dataStateRef = useRef(dataState);
  useEffect(() => {
    dataStateRef.current = dataState;
  }, [dataState]);

  // Connect SSE live / replay stream
  useEffect(() => {
    const handleIncomingMessage = (newPost) => {
      if (dataStateRef.current === 'OFFLINE') return; // Stop appending posts when OFFLINE

      const browserNow = typeof performance !== 'undefined' && performance.timeOrigin
        ? performance.timeOrigin + performance.now()
        : Date.now();

      newPost.browser_receive_timestamp = new Date(browserNow).toISOString();

      setPosts((prev) => {
        // Deduplicate by post_id and cap buffer at 300 posts
        const filtered = prev.filter((p) => p.post_id !== newPost.post_id);
        return [newPost, ...filtered].slice(0, 300);
      });
    };

    const handleStatusChange = (status) => {
      if (status === 'ERROR' || status === 'OFFLINE') {
        setDataState('OFFLINE');
      } else if (status === 'OPEN' || status === 'LIVE') {
        fetchHealth();
      }
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
    <div className="app-shell">
      <Header
        activeTab={activeTab}
        setActiveTab={setActiveTab}
        dataState={dataState}
        onOpenStatus={() => setIsStatusDrawerOpen(true)}
      />

      <main className="app-main-content">
        {activeTab === 'live' && (
          <LivePage
            region={region}
            setRegion={setRegion}
            depots={depots}
            posts={posts}
            dataState={dataState}
            selectedPost={selectedPost}
            setSelectedPost={setSelectedPost}
          />
        )}

        {activeTab === 'dispatch' && (
          <DispatchPage
            region={region}
            setRegion={setRegion}
            depots={depots}
          />
        )}

        {activeTab === 'about' && (
          <AboutPage />
        )}
      </main>

      {/* Status drawer */}
      <StatusDrawer
        isOpen={isStatusDrawerOpen}
        onClose={() => setIsStatusDrawerOpen(false)}
        health={health}
        dataState={dataState}
        lastMsgTime={lastMsgTime}
      />
    </div>
  );
}
