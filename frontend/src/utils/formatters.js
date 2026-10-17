/**
 * frontend/src/utils/formatters.js
 * Plain, robust formatting utilities with safe localStorage and URL synchronization.
 */

export function safeLocalStorageGet(key, defaultValue) {
  try {
    const val = localStorage.getItem(key);
    return val !== null ? JSON.parse(val) : defaultValue;
  } catch (e) {
    return defaultValue;
  }
}

export function safeLocalStorageSet(key, value) {
  try {
    localStorage.setItem(key, JSON.stringify(value));
  } catch (e) {
    // quota exceeded or disabled
  }
}

export function formatDecimals(val, decimals = 3) {
  if (val === null || val === undefined || isNaN(val)) return '—';
  return Number(val).toFixed(decimals);
}

export function formatPercent(val, decimals = 1) {
  if (val === null || val === undefined || isNaN(val)) return '—';
  return `${Number(val).toFixed(decimals)}%`;
}

export function formatTimestamp(ts, timezone = 'local') {
  if (!ts) return '—';
  try {
    // If it's already HH:MM:SS format
    if (typeof ts === 'string' && ts.length === 8 && ts.includes(':')) {
      return ts;
    }
    const d = new Date(ts);
    if (isNaN(d.getTime())) return String(ts);
    if (timezone === 'utc') {
      return d.toISOString().substring(11, 19) + ' UTC';
    }
    return d.toLocaleTimeString([], { hour12: false });
  } catch {
    return String(ts);
  }
}

export function formatDistance(km, units = 'km') {
  if (km === null || km === undefined || isNaN(km)) return '—';
  if (units === 'miles') {
    return `${(Number(km) * 0.621371).toFixed(2)} mi`;
  }
  return `${Number(km).toFixed(2)} km`;
}

export function downloadFile(content, filename, type = 'text/csv;charset=utf-8;') {
  const blob = new Blob([content], { type });
  const url = URL.createObjectURL(blob);
  const link = document.createElement('a');
  link.setAttribute('href', url);
  link.setAttribute('download', filename);
  document.body.appendChild(link);
  link.click();
  document.body.removeChild(link);
  URL.revokeObjectURL(url);
}

export function exportPostsAsCSV(posts, filename = 'crisisguard_filtered_posts.csv') {
  if (!posts || posts.length === 0) return;
  const headers = ['post_id', 'timestamp', 'event_type', 'humanitarian_category', 'urgency_level', 'prob_synthetic_score', 'trust_score', 'status_label', 'injected_ground_truth', 'latitude', 'longitude', 'text'];
  const rows = posts.map(p => [
    p.post_id,
    p.timestamp,
    p.event_type,
    p.humanitarian_category,
    p.urgency_level,
    p.prob_synthetic_score,
    p.trust_score,
    p.status_label,
    p.injected_ground_truth,
    p.latitude,
    p.longitude,
    `"${(p.text || '').replace(/"/g, '""')}"`
  ]);
  const csvContent = [headers.join(','), ...rows.map(r => r.join(','))].join('\n');
  downloadFile(csvContent, filename);
}

export function exportPostsAsJSON(posts, filename = 'crisisguard_filtered_posts.json') {
  if (!posts || posts.length === 0) return;
  downloadFile(JSON.stringify(posts, null, 2), filename, 'application/json');
}

export function exportAllocationAsCSV(assignments, filename = 'crisisguard_allocations.csv') {
  if (!assignments || assignments.length === 0) return;
  const headers = ['demand_id', 'depot_name', 'status', 'distance_km', 'urgency', 'trust', 'is_synthetic', 'demand_lat', 'demand_lon', 'depot_lat', 'depot_lon'];
  const rows = assignments.map(a => [
    a.demand_id,
    `"${(a.depot_name || '').replace(/"/g, '""')}"`,
    a.status,
    a.distance_km,
    a.urgency,
    a.trust,
    a.is_synthetic,
    a.demand_lat,
    a.demand_lon,
    a.depot_lat,
    a.depot_lon
  ]);
  const csvContent = [headers.join(','), ...rows.map(r => r.join(','))].join('\n');
  downloadFile(csvContent, filename);
}
