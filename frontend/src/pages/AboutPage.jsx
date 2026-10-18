import React from 'react';
import { FileText, Github, ExternalLink, Database, Layers, Radio, Cpu } from 'lucide-react';

export default function AboutPage() {
  return (
    <div className="about-page-layout">
      <div className="about-card">
        <h2 className="about-title">About CrisisGuard</h2>
        <p className="about-lead">
          Real-time emergency post verification and resource dispatch system.
        </p>

        {/* Five short sentences in plain English */}
        <div className="sentences-card">
          <ol className="plain-sentences-list">
            <li>
              CrisisGuard helps emergency coordinators verify incoming disaster reports and allocate limited rescue units.
            </li>
            <li>
              The system analyzes wording and style features to flag suspected synthetic reports before responders deploy.
            </li>
            <li>
              All posts in this dashboard are a replay of recorded disaster posts plus simulated hoaxes, and map locations are simulated inside the region.
            </li>
            <li>
              The backend pipeline is powered by Apache Kafka, Apache Spark, Hadoop HDFS, and Apache Hive.
            </li>
            <li>
              Technical benchmarks and methodology are available in the project report PDF and GitHub repository.
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

        {/* Core tools 2x2 grid */}
        <div className="tools-grid">
          <div className="tool-item">
            <Radio size={20} className="tool-icon" />
            <div className="tool-info">
              <strong className="tool-title">Apache Kafka</strong>
              <span className="tool-desc">Event streaming for raw and scored posts</span>
            </div>
          </div>

          <div className="tool-item">
            <Cpu size={20} className="tool-icon" />
            <div className="tool-info">
              <strong className="tool-title">Apache Spark</strong>
              <span className="tool-desc">Real-time feature extraction and scoring with Spark MLlib models</span>
            </div>
          </div>

          <div className="tool-item">
            <Database size={20} className="tool-icon" />
            <div className="tool-info">
              <strong className="tool-title">Hadoop HDFS</strong>
              <span className="tool-desc">Durable data stored as Parquet files</span>
            </div>
          </div>

          <div className="tool-item">
            <Layers size={20} className="tool-icon" />
            <div className="tool-info">
              <strong className="tool-title">Apache Hive</strong>
              <span className="tool-desc">Analytical catalog queried through Spark SQL</span>
            </div>
          </div>
        </div>
      </div>
    </div>
  );
}
