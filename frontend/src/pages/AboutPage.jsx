import React from 'react';
import { FileText, Github, ExternalLink, ShieldCheck, Database, Layers, Radio } from 'lucide-react';

export default function AboutPage() {
  return (
    <div className="about-page-layout">
      <div className="about-card">
        <h2 className="about-title">About CrisisGuard</h2>
        <p className="about-lead">
          Real-time disaster response verification and resource dispatch system.
        </p>

        {/* Five short sentences in plain English */}
        <div className="sentences-card">
          <ol className="plain-sentences-list">
            <li>
              CrisisGuard helps emergency coordinators verify incoming disaster reports and allocate limited rescue units.
            </li>
            <li>
              The system analyzes report phrasing and repetition to flag suspected synthetic or automated hoaxes before responders deploy.
            </li>
            <li>
              All social posts in this dashboard are replayed from verified disaster datasets, and map locations are simulated within the region.
            </li>
            <li>
              The processing pipeline runs on Apache Kafka for event streaming, Apache Spark for real-time scoring, and Hadoop HDFS with Hive for durable storage.
            </li>
            <li>
              Detailed benchmark tables, ablation studies, and architectural evaluations are documented in the project report and GitHub repository.
            </li>
          </ol>
        </div>

        {/* Links to documentation and repository */}
        <div className="about-links-section">
          <a
            href="/docs/REPORT.pdf"
            target="_blank"
            rel="noopener noreferrer"
            className="btn-doc-link"
          >
            <FileText size={16} />
            <span>Read full project report (PDF)</span>
            <ExternalLink size={14} className="icon-ext" />
          </a>

          <a
            href="https://github.com/sivasaiboggu/Bigdata_Project"
            target="_blank"
            rel="noopener noreferrer"
            className="btn-doc-link secondary"
          >
            <Github size={16} />
            <span>GitHub repository</span>
            <ExternalLink size={14} className="icon-ext" />
          </a>
        </div>

        {/* Core tools summary */}
        <div className="tools-grid">
          <div className="tool-item">
            <Radio size={16} className="tool-icon" />
            <div>
              <strong>Apache Kafka</strong>
              <span>Event ingestion & scored post streaming</span>
            </div>
          </div>
          <div className="tool-item">
            <ZapIcon size={16} className="tool-icon" />
            <div>
              <strong>Apache Spark</strong>
              <span>Streaming feature extraction & logistic scoring</span>
            </div>
          </div>
          <div className="tool-item">
            <Database size={16} className="tool-icon" />
            <div>
              <strong>Hadoop HDFS</strong>
              <span>Partitioned parquet storage & checkin dumps</span>
            </div>
          </div>
          <div className="tool-item">
            <Layers size={16} className="tool-icon" />
            <div>
              <strong>Apache Hive</strong>
              <span>External metastore & analytical SQL queries</span>
            </div>
          </div>
        </div>
      </div>
    </div>
  );
}

function ZapIcon(props) {
  return (
    <svg
      {...props}
      viewBox="0 0 24 24"
      fill="none"
      stroke="currentColor"
      strokeWidth="2"
      strokeLinecap="round"
      strokeLinejoin="round"
    >
      <polygon points="13 2 3 14 12 14 11 22 21 10 12 10 13 2" />
    </svg>
  );
}
