import React, { useEffect, useRef, useState } from 'react';
import L from 'leaflet';
import 'leaflet/dist/leaflet.css';

// OpenStreetMap tiles configured in one centralized location
export const OSM_TILE_URL = 'https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png';
export const OSM_ATTRIBUTION = '&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors';

export default function MapComponent({
  regionCenter = [29.7604, -95.3698],
  regionZoom = 10,
  depots = [],
  posts = [],
  assignments = [],
  colorBy = 'trust', // 'trust', 'urgency', 'event'
  showDepots = true,
  showPosts = true,
  showLines = true,
  showClusters = false,
  onSelectPost = null,
  fitDataTrigger = 0
}) {
  const mapContainerRef = useRef(null);
  const mapInstanceRef = useRef(null);
  const depotsGroupRef = useRef(null);
  const postsGroupRef = useRef(null);
  const linesGroupRef = useRef(null);
  const clustersGroupRef = useRef(null);
  const [tileError, setTileError] = useState(false);

  // Helper color functions
  const getMarkerColor = (post) => {
    if (colorBy === 'urgency') {
      const u = post.urgency_level || 3.0;
      if (u >= 4.0) return '#ef4444'; // Red
      if (u >= 2.5) return '#f59e0b'; // Amber
      return '#3b82f6'; // Blue
    }
    if (colorBy === 'event') {
      const ev = (post.event_type || '').toLowerCase();
      if (ev.includes('flood')) return '#0284c7';
      if (ev.includes('earthquake')) return '#d97706';
      if (ev.includes('fire')) return '#dc2626';
      if (ev.includes('hurricane') || ev.includes('storm')) return '#7c3aed';
      return '#64748b';
    }
    // Default: Color by Trust Score
    const t = post.trust_score !== undefined ? post.trust_score : (1.0 - (post.prob_synthetic_score || 0.05));
    if (t >= 0.82) return '#10b981'; // High Trust (Green)
    if (t >= 0.50) return '#f59e0b'; // Moderate Trust (Amber)
    return '#ef4444'; // Suspected Synthetic / Low Trust (Red)
  };

  // 1. Initialize Map
  useEffect(() => {
    if (!mapContainerRef.current) return;

    if (!mapInstanceRef.current) {
      const map = L.map(mapContainerRef.current, {
        center: regionCenter,
        zoom: regionZoom,
        zoomControl: true,
        attributionControl: true
      });

      const tileLayer = L.tileLayer(OSM_TILE_URL, {
        attribution: OSM_ATTRIBUTION,
        subdomains: ['a', 'b', 'c'],
        maxZoom: 19,
        className: 'osm-tiles'
      });

      tileLayer.on('tileerror', () => {
        setTileError(true);
      });

      tileLayer.addTo(map);

      depotsGroupRef.current = L.layerGroup().addTo(map);
      postsGroupRef.current = L.layerGroup().addTo(map);
      linesGroupRef.current = L.layerGroup().addTo(map);
      clustersGroupRef.current = L.layerGroup().addTo(map);

      mapInstanceRef.current = map;
    } else {
      mapInstanceRef.current.setView(regionCenter, regionZoom);
    }
  }, [regionCenter, regionZoom]);

  // 2. Render Depots
  useEffect(() => {
    if (!depotsGroupRef.current) return;
    depotsGroupRef.current.clearLayers();
    if (!showDepots) return;

    depots.forEach((depot) => {
      const icon = L.divIcon({
        className: 'depot-pin',
        html: `<div style="background:#2563eb; color:#ffffff; border:2px solid #ffffff; width:24px; height:24px; border-radius:4px; display:flex; align-items:center; justify-content:center; font-size:11px; font-weight:700; box-shadow:0 1px 3px rgba(0,0,0,0.3);">D</div>`,
        iconSize: [24, 24],
        iconAnchor: [12, 12]
      });

      const cap = depot.capacities ? Object.values(depot.capacities)[0] : 15;
      const marker = L.marker([depot.latitude, depot.longitude], { icon })
        .bindPopup(`
          <div style="font-size:12px; font-family:sans-serif; color:#0f172a;">
            <strong>${depot.name}</strong><br/>
            <span>Logistics Facility (Capacity: ${cap} units)</span><br/>
            <span style="color:#64748b;">${depot.latitude.toFixed(4)}, ${depot.longitude.toFixed(4)}</span>
          </div>
        `);
      depotsGroupRef.current.addLayer(marker);
    });
  }, [depots, showDepots]);

  // 3. Render Posts
  useEffect(() => {
    if (!postsGroupRef.current) return;
    postsGroupRef.current.clearLayers();
    if (!showPosts) return;

    posts.forEach((post) => {
      const color = getMarkerColor(post);
      const isHoax = post.is_flagged_misinformation === 1 || (post.prob_synthetic_score >= 0.1786);

      const icon = L.divIcon({
        className: 'post-marker',
        html: `<div style="background:${color}; width:14px; height:14px; border-radius:50%; border:2px solid #ffffff; box-shadow:0 1px 2px rgba(0,0,0,0.25);"></div>`,
        iconSize: [14, 14],
        iconAnchor: [7, 7]
      });

      const marker = L.marker([post.latitude, post.longitude], { icon });
      marker.bindPopup(`
        <div style="font-size:12px; font-family:sans-serif; max-width:240px; color:#0f172a;">
          <div style="display:flex; justify-content:space-between; margin-bottom:4px;">
            <strong style="color:${isHoax ? '#dc2626' : '#15803d'};">${isHoax ? 'Suspected Synthetic' : 'Credible Report'}</strong>
            <span style="color:#64748b;">${post.timestamp || ''}</span>
          </div>
          <p style="margin:4px 0; color:#334155; line-height:1.3;">${post.text || ''}</p>
          <div style="font-size:11px; color:#475569; border-top:1px solid #e2e8f0; padding-top:4px; margin-top:4px;">
            <span>Trust: <strong>${(post.trust_score !== undefined ? post.trust_score : 1.0).toFixed(3)}</strong></span> | 
            <span>Urgency: <strong>${(post.urgency_level || 3.0).toFixed(1)}</strong></span>
          </div>
        </div>
      `);

      if (onSelectPost) {
        marker.on('click', () => onSelectPost(post));
      }

      postsGroupRef.current.addLayer(marker);
    });
  }, [posts, showPosts, colorBy]);

  // 4. Render Allocation Lines
  useEffect(() => {
    if (!linesGroupRef.current) return;
    linesGroupRef.current.clearLayers();
    if (!showLines || !assignments || assignments.length === 0) return;

    assignments.forEach((a) => {
      const isWasted = a.is_synthetic === 1;
      const lineColor = isWasted ? '#ef4444' : '#10b981';
      const poly = L.polyline(
        [
          [a.depot_lat, a.depot_lon],
          [a.demand_lat, a.demand_lon]
        ],
        {
          color: lineColor,
          weight: 2,
          opacity: 0.7,
          dashArray: isWasted ? '4, 4' : '6, 2'
        }
      ).bindPopup(`
        <div style="font-size:12px; font-family:sans-serif; color:#0f172a;">
          <strong>${a.status}</strong><br/>
          From: ${a.depot_name}<br/>
          Transit Distance: ${a.distance_km} km
        </div>
      `);
      linesGroupRef.current.addLayer(poly);
    });
  }, [assignments, showLines]);

  // 5. Fit bounds to visible markers and depots
  useEffect(() => {
    if (!mapInstanceRef.current) return;
    const allCoords = [];
    if (showDepots) {
      depots.forEach(d => allCoords.push([d.latitude, d.longitude]));
    }
    if (showPosts) {
      posts.forEach(p => allCoords.push([p.latitude, p.longitude]));
    }
    if (allCoords.length > 0) {
      const bounds = L.latLngBounds(allCoords);
      mapInstanceRef.current.fitBounds(bounds, { padding: [30, 30], maxZoom: 13 });
    }
  }, [fitDataTrigger]);

  return (
    <div className="map-wrapper" style={{ position: 'relative', width: '100%', height: '100%', minHeight: '380px' }}>
      <div ref={mapContainerRef} style={{ width: '100%', height: '100%', minHeight: '380px' }} />

      {tileError && (
        <div className="map-fallback-banner">
          Map tiles unavailable (offline / network error) — displaying marker coordinates directly.
        </div>
      )}

      {/* Map Legend */}
      <div className="map-legend">
        <div className="legend-row">
          <span className="legend-dot" style={{ background: '#2563eb', borderRadius: '2px' }}>D</span>
          <span>OSM Depots</span>
        </div>
        {colorBy === 'trust' && (
          <>
            <div className="legend-row">
              <span className="legend-dot" style={{ background: '#10b981' }}></span>
              <span>Credible (Trust &ge; 0.82)</span>
            </div>
            <div className="legend-row">
              <span className="legend-dot" style={{ background: '#f59e0b' }}></span>
              <span>Moderate (0.50–0.81)</span>
            </div>
            <div className="legend-row">
              <span className="legend-dot" style={{ background: '#ef4444' }}></span>
              <span>Suspected Synthetic (&lt; 0.50)</span>
            </div>
          </>
        )}
        {colorBy === 'urgency' && (
          <>
            <div className="legend-row">
              <span className="legend-dot" style={{ background: '#ef4444' }}></span>
              <span>Urgent (4.0–5.0)</span>
            </div>
            <div className="legend-row">
              <span className="legend-dot" style={{ background: '#f59e0b' }}></span>
              <span>Moderate (2.5–3.9)</span>
            </div>
            <div className="legend-row">
              <span className="legend-dot" style={{ background: '#3b82f6' }}></span>
              <span>Advisory (&lt; 2.5)</span>
            </div>
          </>
        )}
      </div>
    </div>
  );
}
