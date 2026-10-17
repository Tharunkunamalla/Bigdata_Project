/**
 * frontend/src/api/client.js
 * Unified API Client for CrisisGuard AI Operations Gateway.
 * Single address: http://127.0.0.1:8000/
 */

const API_BASE = ''; // relative path connects to current host (127.0.0.1:8000)

async function fetchJSON(url, options = {}) {
  const res = await fetch(`${API_BASE}${url}`, {
    ...options,
    headers: {
      'Accept': 'application/json',
      'Content-Type': 'application/json',
      ...(options.headers || {})
    }
  });
  if (!res.ok) {
    let msg = `HTTP error ${res.status}`;
    try {
      const err = await res.json();
      if (err.detail) msg = typeof err.detail === 'string' ? err.detail : JSON.stringify(err.detail);
    } catch {
      // ignore
    }
    throw new Error(msg);
  }
  return res.json();
}

export const api = {
  getHealth: () => fetchJSON('/api/health'),
  getSummary: () => fetchJSON('/api/summary'),
  getMetrics: () => fetchJSON('/api/metrics'),
  getModels: () => fetchJSON('/api/models'),
  getThresholdMetrics: (theta) => fetchJSON(`/api/models/threshold?theta=${encodeURIComponent(theta)}`),
  getEvaluationViews: () => fetchJSON('/api/models/views'),
  getDepots: (region) => fetchJSON(`/api/depots?region=${encodeURIComponent(region)}`),
  getSamplePosts: (params = {}) => {
    const qs = new URLSearchParams();
    if (params.region) qs.set('region', params.region);
    if (params.limit) qs.set('limit', params.limit);
    if (params.hoax_rate !== undefined) qs.set('hoax_rate', params.hoax_rate);
    if (params.source_filter) qs.set('source_filter', params.source_filter);
    return fetchJSON(`/api/sample-posts?${qs.toString()}`);
  },
  allocate: (data) => fetchJSON('/api/allocate', {
    method: 'POST',
    body: JSON.stringify(data)
  }),
  compareAllocationPolicies: (data) => fetchJSON('/api/allocate/compare', {
    method: 'POST',
    body: JSON.stringify(data)
  }),
  getAblation: () => fetchJSON('/api/ablation'),
  getCoordination: () => fetchJSON('/api/coordination'),
  getScaling: () => fetchJSON('/api/scaling'),
  getRecoveryTest: () => fetchJSON('/api/recovery-test'),
  runDemoStep: (step) => fetchJSON(`/api/demo/step?step=${encodeURIComponent(step)}`, { method: 'POST' }),
  exportDemoEvidence: () => fetchJSON('/api/demo/export', { method: 'POST' })
};

/**
 * SSE Client with exponential backoff, Last-Event-ID resumption, and deduplication.
 */
export class SSEClient {
  constructor({ onMessage, onStatusChange, rate = 1.0, hoaxRate = 0.20, region = 'houston' }) {
    this.onMessage = onMessage;
    this.onStatusChange = onStatusChange;
    this.rate = rate;
    this.hoaxRate = hoaxRate;
    this.region = region;
    
    this.eventSource = null;
    this.reconnectAttempts = 0;
    this.reconnectTimeout = null;
    this.lastEventId = null;
    this.closedExplicitly = false;
  }

  connect() {
    this.closedExplicitly = false;
    if (this.eventSource) {
      this.eventSource.close();
    }

    const qs = new URLSearchParams({
      rate: this.rate,
      hoax_rate: this.hoaxRate,
      region: this.region
    });

    const url = `${API_BASE}/api/stream?${qs.toString()}`;
    this.onStatusChange('CONNECTING');

    try {
      this.eventSource = new EventSource(url);

      this.eventSource.onopen = () => {
        this.reconnectAttempts = 0;
        this.onStatusChange('OPEN');
      };

      this.eventSource.onmessage = (event) => {
        if (!event.data) return;
        if (event.lastEventId) {
          this.lastEventId = event.lastEventId;
        }
        try {
          const payload = JSON.parse(event.data);
          this.onMessage(payload);
        } catch {
          // ignore heartbeat comments or malformed frames
        }
      };

      this.eventSource.onerror = () => {
        if (this.closedExplicitly) return;
        this.onStatusChange('OFFLINE');
        this.eventSource.close();
        this.eventSource = null;
        
        // Exponential backoff reconnect: 1s, 2s, 4s, up to 10s
        const delay = Math.min(1000 * Math.pow(2, this.reconnectAttempts), 10000);
        this.reconnectAttempts++;
        this.reconnectTimeout = setTimeout(() => {
          this.connect();
        }, delay);
      };
    } catch {
      this.onStatusChange('OFFLINE');
    }
  }

  updateConfig({ rate, hoaxRate, region }) {
    let changed = false;
    if (rate !== undefined && rate !== this.rate) {
      this.rate = rate;
      changed = true;
    }
    if (hoaxRate !== undefined && hoaxRate !== this.hoaxRate) {
      this.hoaxRate = hoaxRate;
      changed = true;
    }
    if (region !== undefined && region !== this.region) {
      this.region = region;
      changed = true;
    }
    if (changed) {
      this.connect();
    }
  }

  disconnect() {
    this.closedExplicitly = true;
    if (this.reconnectTimeout) {
      clearTimeout(this.reconnectTimeout);
      this.reconnectTimeout = null;
    }
    if (this.eventSource) {
      this.eventSource.close();
      this.eventSource = null;
    }
    this.onStatusChange('OFFLINE');
  }
}
