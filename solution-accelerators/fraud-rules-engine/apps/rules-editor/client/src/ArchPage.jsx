import React, { useEffect, useRef, useState } from 'react'
import { createPortal } from 'react-dom'

// ── Data ─────────────────────────────────────────────────────────

const STORY = [
  {
    phase: '01',
    label: 'Blaze Advisor Today',
    color: '#64748b',
    nodes: [
      { id: 'ba', label: 'FICO Blaze\nAdvisor', sub: 'Proprietary rules engine', icon: 'warn' },
      { id: 'export', label: 'DMN Export', sub: 'Open OMG standard', icon: 'export' },
    ],
    note: 'Existing Blaze rules export natively to DMN — no translation layer needed.',
  },
  {
    phase: '02',
    label: 'Rule Authoring',
    color: '#2563eb',
    nodes: [
      { id: 'editor', label: 'KIE DMN\nEditor', sub: 'Apache KIE 10.1.0', icon: 'edit' },
      { id: 'uc', label: 'UC Volume', sub: 'Unity Catalog', icon: 'volume' },
      { id: 'versions', label: 'rule_versions\nDelta Table', sub: 'DRAFT → ACTIVE', icon: 'table' },
    ],
    note: 'Business analysts edit decision tables visually. Versions saved to Unity Catalog.',
  },
  {
    phase: '03',
    label: 'Batch Scoring',
    color: '#059669',
    nodes: [
      { id: 'job', label: 'Databricks\nJob', sub: '04_batch_scoring.py', icon: 'job' },
      { id: 'drools', label: 'Drools 8.30\nDMN Engine', sub: 'FEEL Level 3 · COLLECT+SUM', icon: 'engine' },
      { id: 'spark', label: 'Spark UDF\n+ repartition', sub: 'Compile-once per task · 64 cores', icon: 'spark' },
    ],
    note: 'Repartition to cores×4 so Spark schedules many small tasks across the cluster. DMN compiles once per task and caches in a ConcurrentHashMap keyed by XML hash.',
  },
  {
    phase: '04',
    label: 'Results',
    color: '#d97706',
    nodes: [
      { id: 'delta', label: 'scored_results\nDelta Table', sub: '10M rows in 3:36', icon: 'delta' },
      { id: 'actions', label: 'PASS · REVIEW\nFLAG', sub: 'Score thresholds: <30 / 30–59 / ≥60', icon: 'score' },
    ],
    note: 'Full audit trail with DMN version, timestamp, and score per return. 21 rules across 6 IRM 25.1.2 categories.',
  },
]

const TECH = [
  { label: 'DMN Standard', value: 'OMG 1.5', detail: 'Decision Model & Notation' },
  { label: 'Rules Engine', value: 'Drools 8.30', detail: 'Apache KIE, FEEL Level 3' },
  { label: 'Runtime', value: 'DBR 17.3', detail: 'Java 17, Scala 2.13' },
  { label: 'Storage', value: 'Delta Lake', detail: 'Unity Catalog governance' },
  { label: 'Scale', value: '10M rows / 3:36', detail: '4× c5.4xlarge, 64 cores' },
  { label: 'Rules', value: '21 rules', detail: 'IRM 25.1.2 · 6 categories' },
  { label: 'Shaded JAR', value: '2.0.0', detail: 'ANTLR + Eclipse + XStream shaded' },
  { label: 'Runtime Engine', value: 'Standard', detail: 'Photon skipped — JVM UDF is opaque' },
]

// ── Icon Component ────────────────────────────────────────────────

const ICONS = {
  warn:   <path d="M12 9v4m0 4h.01M10.29 3.86L1.82 18a2 2 0 0 0 1.71 3h16.94a2 2 0 0 0 1.71-3L13.71 3.86a2 2 0 0 0-3.42 0z"/>,
  export: <><path d="M18 13v6a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2V8a2 2 0 0 1 2-2h6"/><polyline points="15 3 21 3 21 9"/><line x1="10" y1="14" x2="21" y2="3"/></>,
  edit:   <><path d="M11 4H4a2 2 0 0 0-2 2v14a2 2 0 0 0 2 2h14a2 2 0 0 0 2-2v-7"/><path d="M18.5 2.5a2.121 2.121 0 0 1 3 3L12 15l-4 1 1-4 9.5-9.5z"/></>,
  volume: <><ellipse cx="12" cy="5" rx="9" ry="3"/><path d="M21 12c0 1.66-4 3-9 3s-9-1.34-9-3"/><path d="M3 5v14c0 1.66 4 3 9 3s9-1.34 9-3V5"/></>,
  table:  <><rect x="3" y="3" width="18" height="18" rx="2" ry="2"/><line x1="3" y1="9" x2="21" y2="9"/><line x1="3" y1="15" x2="21" y2="15"/><line x1="9" y1="9" x2="9" y2="21"/></>,
  job:    <polygon points="5 3 19 12 5 21 5 3"/>,
  engine: <><circle cx="12" cy="12" r="3"/><path d="M19.07 4.93a10 10 0 0 1 0 14.14M4.93 4.93a10 10 0 0 0 0 14.14"/></>,
  spark:  <polyline points="13 2 3 14 12 14 11 22 21 10 12 10 13 2"/>,
  delta:  <><path d="M22 12h-4l-3 9L9 3l-3 9H2"/></>,
  score:  <><circle cx="12" cy="12" r="10"/><polyline points="12 6 12 12 16 14"/></>,
}

function NodeIcon({ type }) {
  return (
    <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" strokeLinejoin="round">
      {ICONS[type] || null}
    </svg>
  )
}

// ── Component ─────────────────────────────────────────────────────

export default function ArchPage({ onClose }) {
  const [visible, setVisible] = useState(false)
  const [activePhase, setActivePhase] = useState(null)

  const handleCloseRef = useRef(null)

  const handleClose = () => {
    setVisible(false)
    setTimeout(onClose, 220)
  }

  // Keep ref current so the keydown listener never goes stale
  useEffect(() => { handleCloseRef.current = handleClose })

  useEffect(() => {
    const rafId = requestAnimationFrame(() => setVisible(true))
    const onKey = (e) => { if (e.key === 'Escape') handleCloseRef.current?.() }
    window.addEventListener('keydown', onKey)
    return () => {
      cancelAnimationFrame(rafId)
      window.removeEventListener('keydown', onKey)
    }
  }, [])

  return createPortal(
    <div
      className={`arch-overlay ${visible ? 'arch-visible' : ''}`}
      onClick={e => e.target === e.currentTarget && handleClose()}
    >
      <div className="arch-panel" role="dialog" aria-modal="true" aria-labelledby="arch-title-id">

        {/* Header */}
        <div className="arch-header">
          <div className="arch-header-left">
            <div className="arch-eyebrow">Tax Authority Fraud Detection</div>
            <h1 className="arch-title" id="arch-title-id">Blaze Advisor → Databricks</h1>
            <div className="arch-subtitle">Open-standard DMN on an open-source runtime, fully on Databricks</div>
          </div>
          <button className="arch-close" onClick={handleClose} aria-label="Close architecture overview">✕</button>
        </div>

        <div className="arch-body">

          {/* Flow diagram */}
          <div className="arch-flow">
            {STORY.map((phase, pi) => (
              <React.Fragment key={phase.phase}>
                <div
                  className={`arch-phase ${activePhase === pi ? 'arch-phase-active' : ''}`}
                  onMouseEnter={() => setActivePhase(pi)}
                  onMouseLeave={() => setActivePhase(null)}
                  style={{ '--phase-color': phase.color }}
                >
                  <div className="arch-phase-num">{phase.phase}</div>
                  <div className="arch-phase-label">{phase.label}</div>
                  <div className="arch-nodes">
                    {phase.nodes.map(node => (
                      <div key={node.id} className="arch-node">
                        <div className="arch-node-icon"><NodeIcon type={node.icon} /></div>
                        <div className="arch-node-text">
                          <div className="arch-node-label">{node.label}</div>
                          <div className="arch-node-sub">{node.sub}</div>
                        </div>
                      </div>
                    ))}
                  </div>
                  {activePhase === pi && (
                    <div className="arch-note">{phase.note}</div>
                  )}
                </div>
                {pi < STORY.length - 1 && (
                  <div className="arch-arrow">
                    <div className="arch-arrow-line">
                      <div className="arch-arrow-dot"></div>
                    </div>
                    <div className="arch-arrow-head">▶</div>
                  </div>
                )}
              </React.Fragment>
            ))}
          </div>

          {/* Key insight */}
          <div className="arch-insight">
            <div className="arch-insight-icon"><svg width='16' height='16' viewBox='0 0 24 24' fill='none' stroke='currentColor' strokeWidth='2'><path d='M21 2v6h-6M3 12a9 9 0 0 1 15-6.7L21 8M3 22v-6h6M21 12a9 9 0 0 1-15 6.7L3 16'/></svg></div>
            <div className="arch-insight-text">
              <strong>The key insight:</strong> Blaze Advisor exports rules as DMN — the same standard Drools evaluates natively.
              No translation layer. Business analysts edit rules in the KIE visual editor. Databricks handles the rest at any scale.
            </div>
          </div>

          {/* Tech stack */}
          <div className="arch-tech-grid">
            {TECH.map(t => (
              <div key={t.label} className="arch-tech-card">
                <div className="arch-tech-value">{t.value}</div>
                <div className="arch-tech-label">{t.label}</div>
                <div className="arch-tech-detail">{t.detail}</div>
              </div>
            ))}
          </div>

          {/* Data flow detail */}
          <div className="arch-detail-grid">
            <div className="arch-detail-box">
              <div className="arch-detail-title">DMN Governance Flow</div>
              <div className="arch-steps">
                <div className="arch-step"><span className="arch-step-num">1</span>Analyst imports Blaze DMN export or edits directly in KIE editor</div>
                <div className="arch-step"><span className="arch-step-num">2</span>Save as DRAFT — stored in UC Volume with metadata in rule_versions Delta table</div>
                <div className="arch-step"><span className="arch-step-num">3</span>Review and test against sample returns using the Test panel</div>
                <div className="arch-step"><span className="arch-step-num">4</span>Promote to ACTIVE — archives previous version, one active at a time</div>
                <div className="arch-step"><span className="arch-step-num">5</span>Run batch scoring — job reads ACTIVE DMN from UC Volume</div>
              </div>
            </div>
            <div className="arch-detail-box">
              <div className="arch-detail-title">Drools on Spark</div>
              <div className="arch-steps">
                <div className="arch-step"><span className="arch-step-num">1</span>Broadcast DMN XML string to all executors via SparkContext.broadcast()</div>
                <div className="arch-step"><span className="arch-step-num">2</span>Repartition input to defaultParallelism × 4 — surfaces task progress in Spark UI and prevents a few giant opaque tasks from stalling the job</div>
                <div className="arch-step"><span className="arch-step-num">3</span>Scala UDF compiles DMN once per task, caches in a ConcurrentHashMap keyed by XML hashCode — safe across warm clusters and stale $iw refs</div>
                <div className="arch-step"><span className="arch-step-num">4</span>FEEL expressions evaluated by Drools against each row's 17-field fact context</div>
                <div className="arch-step"><span className="arch-step-num">5</span>COLLECT+SUM aggregates scores — each of the 21 rules contributes independently</div>
              </div>
            </div>
          </div>

        </div>
      </div>
    </div>
  , document.body)
}
