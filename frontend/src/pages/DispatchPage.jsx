import React, { useState, useEffect } from 'react';
import MapComponent from '../components/MapComponent';
import { api } from '../api/client';
import { Truck, CheckCircle2, AlertOctagon, RotateCcw } from 'lucide-react';

const REGION_CENTERS = {
  houston: { center: [29.7604, -95.3698], zoom: 11, label: 'Houston (Hurricane Harvey)' },
  california: { center: [38.5816, -121.4944], zoom: 10, label: 'Northern California (Wildfires)' },
  nepal: { center: [27.7172, 85.3240], zoom: 11, label: 'Kathmandu Valley (Earthquake)' },
  miami: { center: [25.7617, -80.1918], zoom: 11, label: 'Miami / South Florida (Hurricane Irma)' }
};

export default function DispatchPage({ region, setRegion, depots }) {
  const [unitCount, setUnitCount] = useState(20);
  const [ignoreReliability, setIgnoreReliability] = useState(false);
  const [dispatchResult, setDispatchResult] = useState(null);
  const [isSolving, setIsSolving] = useState(false);
  const [errorMsg, setErrorMsg] = useState(null);

  const currentRegionConfig = REGION_CENTERS[region] || REGION_CENTERS.houston;

  const runDispatchPlan = async (ignore = ignoreReliability) => {
    setIsSolving(true);
    setErrorMsg(null);
    try {
      const res = await api.allocate({
        region,
        capacity: unitCount,
        policy: ignore ? 'blind_lp' : 'trust_aware_validation_thresh',
        ignore_reliability: ignore
      });
      setDispatchResult(res);
    } catch (err) {
      console.error('Dispatch optimization error:', err);
      setErrorMsg('Unable to plan dispatch. Verify API status.');
    } finally {
      setIsSolving(false);
    }
  };

  // Run on mount or region change
  useEffect(() => {
    runDispatchPlan(ignoreReliability);
  }, [region]);

  const handleToggleIgnore = (e) => {
    const val = e.target.checked;
    setIgnoreReliability(val);
    runDispatchPlan(val);
  };

  return (
    <div className="dispatch-page-layout">
      {/* Top planning controls */}
      <div className="dispatch-controls-card">
        <div className="dispatch-controls-row">
          <div className="ctrl-group">
            <label htmlFor="dispatch-region-select" className="ctrl-label">Region</label>
            <select
              id="dispatch-region-select"
              className="ctrl-select"
              value={region}
              onChange={(e) => setRegion(e.target.value)}
            >
              <option value="houston">Houston</option>
              <option value="california">Northern California</option>
              <option value="nepal">Kathmandu Valley</option>
              <option value="miami">Miami / South Florida</option>
            </select>
          </div>

          <div className="ctrl-group">
            <label htmlFor="rescue-units-input" className="ctrl-label">
              Available rescue units: <strong className="tabular-nums">{unitCount}</strong>
            </label>
            <input
              id="rescue-units-input"
              type="range"
              min="5"
              max="60"
              step="5"
              value={unitCount}
              onChange={(e) => setUnitCount(parseInt(e.target.value, 10))}
              className="ctrl-slider"
            />
          </div>

          <div className="ctrl-group toggle-group">
            <label className="toggle-label" htmlFor="ignore-reliability-toggle">
              <input
                id="ignore-reliability-toggle"
                type="checkbox"
                checked={ignoreReliability}
                onChange={handleToggleIgnore}
                className="toggle-checkbox"
              />
              <span className="toggle-text">Ignore report reliability</span>
            </label>
            <span className="toggle-hint">Dispatches without trust weighting</span>
          </div>

          <div className="ctrl-group action-group">
            <button
              id="plan-dispatch-btn"
              className="btn-primary"
              onClick={() => runDispatchPlan(ignoreReliability)}
              disabled={isSolving}
            >
              <Truck size={15} />
              <span>{isSolving ? 'Planning...' : 'Plan dispatch'}</span>
            </button>
          </div>
        </div>
      </div>

      {/* Main layout: Map + Summary */}
      <div className="dispatch-grid">
        {/* Map showing assignments */}
        <div className="dispatch-map-container">
          <MapComponent
            regionCenter={currentRegionConfig.center}
            regionZoom={currentRegionConfig.zoom}
            depots={depots}
            posts={[]}
            assignments={dispatchResult?.assignments || []}
            showDepots={true}
            showPosts={false}
            showLines={true}
            fitDataTrigger={dispatchResult?.assignments?.length || 0}
          />
        </div>

        {/* Short list of assignments and counts */}
        <div className="dispatch-summary-panel">
          <h3 className="section-title">Dispatch plan</h3>

          {errorMsg && (
            <div className="alert-box error">
              <span>{errorMsg}</span>
            </div>
          )}

          {dispatchResult ? (
            <div className="plan-results">
              <div className="metrics-summary-list">
                <div className="summary-stat-row">
                  <span className="stat-label">Units sent</span>
                  <span className="stat-value tabular-nums">{dispatchResult.units_sent ?? dispatchResult.total_served ?? 0}</span>
                </div>
                <div className="summary-stat-row">
                  <span className="stat-label">Calls covered</span>
                  <span className="stat-value tabular-nums">{dispatchResult.calls_covered ?? dispatchResult.total_served ?? 0}</span>
                </div>
                <div className="summary-stat-row">
                  <span className="stat-label">Flagged calls skipped</span>
                  <span className="stat-value stat-skipped tabular-nums">
                    {dispatchResult.flagged_calls_skipped ?? 0}
                  </span>
                </div>
              </div>

              {/* Assignments breakdown */}
              <div className="assignments-list-container">
                <h4 className="assignments-heading">Assigned routes</h4>
                <div className="assignments-scroll-area">
                  {dispatchResult.assignments?.length === 0 ? (
                    <p className="empty-text">No active assignments generated.</p>
                  ) : (
                    dispatchResult.assignments?.map((a, i) => (
                      <div key={i} className="assignment-item">
                        <div className="assignment-header">
                          <span className="depot-tag">{a.depot_name}</span>
                          <span className="distance-tag tabular-nums">{a.distance_km} km</span>
                        </div>
                        <div className="assignment-details">
                          <span className="call-id">Call #{a.demand_id}</span>
                          {a.is_synthetic === 1 && (
                            <span className="badge-flagged-small">Flagged call</span>
                          )}
                        </div>
                      </div>
                    ))
                  )}
                </div>
              </div>

              {/* Mandatory plain single sentence note */}
              <div className="simulation-disclaimer">
                {dispatchResult.simulation_note || 'This is a simulation on replayed posts.'}
              </div>
            </div>
          ) : (
            <div className="plan-loading">
              <p>Calculating optimal route assignments...</p>
            </div>
          )}
        </div>
      </div>
    </div>
  );
}
