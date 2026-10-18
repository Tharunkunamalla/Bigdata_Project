import React, { useEffect, useRef, useState } from 'react';
import L from 'leaflet';
import 'leaflet/dist/leaflet.css';

export const OSM_TILE_URL = 'https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png';
export const OSM_ATTRIBUTION = '&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors';

const COLOR_NORMAL = '#64748b'; // Neutral muted slate/blue
const COLOR_FLAGGED = '#dc2626'; // Warning color (amber-red)
const COLOR_DEPOT = '#2563eb'; // Depot accent blue

export default function MapComponent({
  regionCenter = [29.7604, -95.3698],
  regionZoom = 11,
  depots = [],
  posts = [],
  assignments = [],
  showDepots = true,
  showPosts = true,
  showLines = true,
  onSelectPost = null,
  fitDataTrigger = 0
}) {
  const mapContainerRef = useRef(null);
  const mapInstanceRef = useRef(null);
  const depotsGroupRef = useRef(null);
  const postsGroupRef = useRef(null);
  const linesGroupRef = useRef(null);
  const [tileError, setTileError] = useState(false);

  // Initialize Map
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

      mapInstanceRef.current = map;
    } else {
      mapInstanceRef.current.setView(regionCenter, regionZoom);
    }
  }, [regionCenter, regionZoom]);

  // Render Depots (distinct square marker)
  useEffect(() => {
    if (!depotsGroupRef.current) return;
    depotsGroupRef.current.clearLayers();
    if (!showDepots) return;

    depots.forEach((depot) => {
      const icon = L.divIcon({
        className: 'custom-depot-icon',
        html: `<div style="background:${COLOR_DEPOT}; color:#ffffff; border:2px solid #ffffff; width:22px; height:22px; border-radius:3px; display:flex; align-items:center; justify-content:center; font-size:11px; font-weight:700; box-shadow:0 1px 3px rgba(0,0,0,0.35);">D</div>`,
        iconSize: [22, 22],
        iconAnchor: [11, 11]
      });

      const cap = depot.capacities ? Object.values(depot.capacities)[0] : 15;
      const marker = L.marker([depot.latitude, depot.longitude], { icon })
        .bindPopup(`
          <div style="font-size:12px; font-family:sans-serif; color:#0f172a;">
            <strong>${depot.name}</strong><br/>
            <span>Rescue depot (${cap} units available)</span>
          </div>
        `);
      depotsGroupRef.current.addLayer(marker);
    });
  }, [depots, showDepots]);

  // Render Posts with proximity clustering
  useEffect(() => {
    if (!postsGroupRef.current) return;
    postsGroupRef.current.clearLayers();
    if (!showPosts || !posts || posts.length === 0) return;

    // Cluster posts using spatial grid (~0.015 degrees)
    const gridSize = 0.018;
    const clusters = {};

    posts.forEach((post) => {
      const lat = Number(post.latitude);
      const lon = Number(post.longitude);
      if (isNaN(lat) || isNaN(lon)) return;

      const gridKey = `${Math.round(lat / gridSize)}_${Math.round(lon / gridSize)}`;
      if (!clusters[gridKey]) {
        clusters[gridKey] = [];
      }
      clusters[gridKey].push(post);
    });

    Object.values(clusters).forEach((cluster) => {
      const count = cluster.length;
      const hasFlagged = cluster.some((p) =>
        Boolean(p.is_flagged || p.is_flagged_misinformation === 1 || (p.prob_synthetic_score !== undefined && p.prob_synthetic_score >= 0.1786))
      );
      const color = hasFlagged ? COLOR_FLAGGED : COLOR_NORMAL;

      // Center coords of cluster
      const avgLat = cluster.reduce((sum, p) => sum + Number(p.latitude), 0) / count;
      const avgLon = cluster.reduce((sum, p) => sum + Number(p.longitude), 0) / count;

      if (count === 1) {
        // Individual marker
        const post = cluster[0];
        const isFlagged = hasFlagged;
        const icon = L.divIcon({
          className: 'custom-post-marker',
          html: `<div style="background:${color}; width:12px; height:12px; border-radius:50%; border:2px solid #ffffff; box-shadow:0 1px 3px rgba(0,0,0,0.3);"></div>`,
          iconSize: [12, 12],
          iconAnchor: [6, 6]
        });

        const marker = L.marker([avgLat, avgLon], { icon });
        marker.bindPopup(`
          <div style="font-size:12px; font-family:sans-serif; max-width:240px; color:#0f172a;">
            <div style="display:flex; justify-content:space-between; margin-bottom:4px;">
              <strong style="color:${isFlagged ? '#dc2626' : '#334155'};">${isFlagged ? 'Flagged report' : 'Civilian report'}</strong>
              <span style="color:#64748b;">${post.timestamp || ''}</span>
            </div>
            <p style="margin:4px 0; color:#334155; line-height:1.3;">${post.text || ''}</p>
            <div style="font-size:11px; color:#64748b; margin-top:4px;">
              <span>Event: <strong>${post.event_type || 'Disaster'}</strong></span>
              ${post.is_urgent ? ' &bull; <strong style="color:#b91c1c;">Urgent</strong>' : ''}
            </div>
          </div>
        `);

        if (onSelectPost) {
          marker.on('click', () => onSelectPost(post));
        }

        postsGroupRef.current.addLayer(marker);
      } else {
        // Cluster marker
        const icon = L.divIcon({
          className: 'custom-cluster-marker',
          html: `<div style="background:${color}; color:#ffffff; width:26px; height:26px; border-radius:50%; border:2px solid #ffffff; display:flex; align-items:center; justify-content:center; font-size:11px; font-weight:700; box-shadow:0 1px 4px rgba(0,0,0,0.35); cursor:pointer;">${count}</div>`,
          iconSize: [26, 26],
          iconAnchor: [13, 13]
        });

        const marker = L.marker([avgLat, avgLon], { icon });
        marker.bindPopup(`
          <div style="font-size:12px; font-family:sans-serif; max-width:240px; color:#0f172a;">
            <strong>Cluster of ${count} reports</strong><br/>
            <span>${hasFlagged ? 'Contains flagged reports' : 'All reports credible'}</span>
            <hr style="margin:4px 0; border:0; border-top:1px solid #e2e8f0;"/>
            <p style="margin:2px 0; color:#475569; font-size:11px;">${cluster[0].text?.slice(0, 80)}...</p>
          </div>
        `);

        marker.on('click', () => {
          if (mapInstanceRef.current) {
            mapInstanceRef.current.setView([avgLat, avgLon], Math.min(14, (mapInstanceRef.current.getZoom() || 11) + 2));
          }
          if (onSelectPost) {
            onSelectPost(cluster[0]);
          }
        });

        postsGroupRef.current.addLayer(marker);
      }
    });
  }, [posts, showPosts, onSelectPost]);

  // Render Dispatch Routes (lines from depot to call)
  useEffect(() => {
    if (!linesGroupRef.current) return;
    linesGroupRef.current.clearLayers();
    if (!showLines || !assignments || assignments.length === 0) return;

    assignments.forEach((a) => {
      const poly = L.polyline(
        [
          [a.depot_lat, a.depot_lon],
          [a.demand_lat, a.demand_lon]
        ],
        {
          color: '#2563eb',
          weight: 2,
          opacity: 0.75,
          dashArray: '4, 4'
        }
      ).bindPopup(`
        <div style="font-size:12px; font-family:sans-serif; color:#0f172a;">
          <strong>Dispatch route</strong><br/>
          From: ${a.depot_name}<br/>
          Distance: ${a.distance_km} km
        </div>
      `);
      linesGroupRef.current.addLayer(poly);
    });
  }, [assignments, showLines]);

  // Fit bounds when requested
  useEffect(() => {
    if (!mapInstanceRef.current) return;
    const allCoords = [];
    if (showDepots) {
      depots.forEach((d) => {
        if (!isNaN(d.latitude) && !isNaN(d.longitude)) {
          allCoords.push([d.latitude, d.longitude]);
        }
      });
    }
    if (showPosts) {
      posts.forEach((p) => {
        const lat = Number(p.latitude);
        const lon = Number(p.longitude);
        if (!isNaN(lat) && !isNaN(lon)) {
          allCoords.push([lat, lon]);
        }
      });
    }
    if (allCoords.length > 0) {
      const bounds = L.latLngBounds(allCoords);
      mapInstanceRef.current.fitBounds(bounds, { padding: [30, 30], maxZoom: 13 });
    }
  }, [fitDataTrigger, regionCenter]);

  return (
    <div className="map-wrapper">
      <div ref={mapContainerRef} className="map-container" />

      {tileError && (
        <div className="map-fallback-banner">
          Map tiles offline — coordinates rendered directly.
        </div>
      )}

      {/* Short legend and simulation note */}
      <div className="map-legend">
        <div className="legend-item">
          <span className="legend-depot-box">D</span>
          <span>Depot</span>
        </div>
        <div className="legend-item">
          <span className="legend-dot normal" />
          <span>Report</span>
        </div>
        <div className="legend-item">
          <span className="legend-dot flagged" />
          <span>Flagged</span>
        </div>
      </div>

      <div className="map-simulation-note">
        Locations are simulated inside the region
      </div>
    </div>
  );
}
