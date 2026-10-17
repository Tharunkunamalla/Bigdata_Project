import React, { useState } from 'react';
import { api } from '../api/client';

export default function DemoPage() {
  const [stepOutputs, setStepOutputs] = useState({});
  const [loadingSteps, setLoadingSteps] = useState({});
  const [runningAll, setRunningAll] = useState(false);
  const [exportResult, setExportResult] = useState(null);

  const stepsCatalog = [
    {
      num: 1,
      title: 'Publish Test Event Batch to Apache Kafka',
      desc: 'Ingests raw simulated emergency distress alerts into Kafka topic disaster_raw_posts.',
      defaultCmd: 'python3 ingestion/replay_simulator.py --batch-size 10 --topic disaster_raw_posts'
    },
    {
      num: 2,
      title: 'Spark Structured Streaming Scoring & Feature Transform',
      desc: 'Applies Spark MLlib TF-IDF and stylometrics; computes probability of synthetic deception.',
      defaultCmd: 'python3 streaming/spark_stream_scorer.py --once'
    },
    {
      num: 3,
      title: 'Atomic Parquet Partition Materialization in HDFS',
      desc: 'Commits silver Parquet blocks to HDFS with write-ahead checkpointing for fault tolerance.',
      defaultCmd: 'hdfs dfs -ls /user/hive/warehouse/cse412_disaster.db/disaster_posts_silver'
    },
    {
      num: 4,
      title: 'Query Apache Hive Metastore Catalog via Spark SQL',
      desc: 'Queries registered Hive Metastore catalog tables (thrift://127.0.0.1:9083) directly via Spark SQL.',
      defaultCmd: 'python3 scripts/verify_hive_and_query.py --dry-run'
    },
    {
      num: 5,
      title: 'Stream Delivery Verification & Latency Tracking',
      desc: 'Verifies sub-second delivery over Server-Sent Events with measured end-to-end latency.',
      defaultCmd: 'curl -s http://127.0.0.1:8000/api/sample-posts?limit=5'
    },
    {
      num: 6,
      title: 'Solve Multi-Depot Trust-Aware MILP Allocation',
      desc: 'Formulates and solves mixed-integer optimization in PuLP CBC, withholding fleet assets from hoaxes.',
      defaultCmd: 'python3 allocation/run_ablation_v3.py --region houston --single-seed'
    }
  ];

  const handleRunStep = async (stepNum) => {
    setLoadingSteps(prev => ({ ...prev, [stepNum]: true }));
    try {
      const res = await api.runDemoStep(stepNum);
      setStepOutputs(prev => ({ ...prev, [stepNum]: res }));
    } catch (err) {
      setStepOutputs(prev => ({
        ...prev,
        [stepNum]: {
          status: 'ERROR',
          error: err.message,
          output: `Execution failed: ${err.message}\nEquivalent fallback command: ${stepsCatalog[stepNum - 1].defaultCmd}`
        }
      }));
    } finally {
      setLoadingSteps(prev => ({ ...prev, [stepNum]: false }));
    }
  };

  const handleRunAll = async () => {
    setRunningAll(true);
    for (let i = 1; i <= 6; i++) {
      await handleRunStep(i);
    }
    setRunningAll(false);
  };

  const handleExportEvidence = async () => {
    try {
      const res = await api.exportDemoEvidence();
      setExportResult(res);
      setTimeout(() => setExportResult(null), 5000);
    } catch (err) {
      alert(`Export failed: ${err.message}`);
    }
  };

  return (
    <div className="demo-page-container">
      {/* Top Banner */}
      <div className="section-card" style={{ marginBottom: '16px' }}>
        <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', flexWrap: 'wrap', gap: '8px' }}>
          <div>
            <h2 className="section-title" style={{ margin: 0 }}>End-to-End Pipeline Demonstration</h2>
            <span className="text-muted" style={{ fontSize: '12px' }}>
              Execute each big data pipeline stage sequentially with live command execution and measured outputs.
            </span>
          </div>

          <div style={{ display: 'flex', gap: '8px' }}>
            <button
              className="btn-primary"
              onClick={handleRunAll}
              disabled={runningAll}
            >
              {runningAll ? 'Executing Pipeline...' : 'Run All (Steps 1–6)'}
            </button>
            <button
              className="btn-secondary"
              onClick={handleExportEvidence}
            >
              Export Evidence
            </button>
          </div>
        </div>

        {exportResult && (
          <div className="callout-box" style={{ marginTop: '10px', borderColor: 'var(--status-success-text)', fontSize: '12px' }}>
            Evidence exported successfully to: <strong>{exportResult.evidence_file}</strong>
          </div>
        )}
      </div>

      {/* Step Cards List */}
      <div style={{ display: 'flex', flexDirection: 'column', gap: '14px' }}>
        {stepsCatalog.map((s) => {
          const out = stepOutputs[s.num];
          const isLoading = loadingSteps[s.num];

          return (
            <div key={s.num} className="section-card" style={{ padding: '14px 18px' }}>
              <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: '8px' }}>
                <div style={{ display: 'flex', alignItems: 'center', gap: '10px' }}>
                  <div style={{
                    width: '28px',
                    height: '28px',
                    borderRadius: '50%',
                    background: out?.status === 'PASS' ? 'var(--status-success-bg)' : 'var(--bg-surface-subtle)',
                    color: out?.status === 'PASS' ? 'var(--status-success-text)' : 'var(--text-primary)',
                    display: 'flex',
                    alignItems: 'center',
                    justifyContent: 'center',
                    fontWeight: 700,
                    fontSize: '13px',
                    border: '1px solid var(--border-subtle)'
                  }}>
                    {s.num}
                  </div>
                  <div>
                    <strong style={{ fontSize: '14px' }}>{s.title}</strong>
                    <div className="text-muted" style={{ fontSize: '12px' }}>{s.desc}</div>
                  </div>
                </div>

                <div style={{ display: 'flex', alignItems: 'center', gap: '10px' }}>
                  {out?.status === 'PASS' && (
                    <span className="badge badge-live">VERIFIED ({out.elapsed_sec}s)</span>
                  )}
                  {out?.status === 'ERROR' && (
                    <span className="badge badge-offline">FAILED</span>
                  )}
                  <button
                    className="btn-primary"
                    style={{ fontSize: '12px', padding: '5px 12px' }}
                    onClick={() => handleRunStep(s.num)}
                    disabled={isLoading}
                  >
                    {isLoading ? 'Running...' : 'Run Step'}
                  </button>
                </div>
              </div>

              {/* Terminal Command Box */}
              <div style={{
                fontSize: '11px',
                fontFamily: 'monospace',
                color: 'var(--text-secondary)',
                backgroundColor: 'var(--bg-surface-subtle)',
                padding: '6px 10px',
                borderRadius: '4px',
                border: '1px solid var(--border-subtle)',
                marginTop: '6px'
              }}>
                $ {out?.command || s.defaultCmd}
              </div>

              {/* Terminal Output */}
              {out && (
                <pre className="terminal-output" style={{ marginTop: '8px' }}>
                  {out.output}
                </pre>
              )}
            </div>
          );
        })}
      </div>
    </div>
  );
}
