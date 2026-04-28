import React, { useState, useEffect, useRef, useCallback } from 'react'
import { DmnEditor } from '@kie-tools/dmn-editor/dist/DmnEditor'
import { getMarshaller } from '@kie-tools/dmn-marshaller'
import { normalize } from '@kie-tools/dmn-marshaller/dist/normalization/normalize'
import '@kie-tools/dmn-marshaller/dist/kie-extensions'
import * as api from './api'
import './styles.css'
import ArchPage from './ArchPage'

/* ── Version Panel ─────────────────────────────────────────────── */

function VersionPanel({ versions, activeVersionId, onSelect, onRefresh }) {
  return (
    <div className="version-panel">
      <div className="section-label">
        <span>VERSIONS</span>
        <button onClick={onRefresh} className="icon-btn" title="Refresh">↻</button>
      </div>
      <div className="version-list">
        {versions.map(v => (
          <div
            key={v.version_id}
            onClick={() => onSelect(v.version_id)}
            className={`version-item ${v.version_id === activeVersionId ? 'selected' : ''}`}
          >
            <div className="version-row">
              <span className="version-id">{v.version_id}</span>
              <span className={`pill pill-${v.status.toLowerCase()}`}>{v.status}</span>
            </div>
            {v.notes && <div className="version-notes">{v.notes}</div>}
          </div>
        ))}
      </div>
    </div>
  )
}

/* ── DMN Input Extraction ──────────────────────────────────────── */

// Walk the live DMN model and extract inputData nodes with their type.
// The KIE marshaller stores the element tag in __$$element.
function extractDmnInputs(model) {
  const defs = model?.definitions
  if (!defs) return null
  const elements = defs.drgElement || []
  const inputs = elements
    .filter(el => el.__$$element === 'inputData' || el.__$$element === 'dmn:inputData')
    .map(el => {
      const name = el['@_name'] || ''
      const typeRef = el.variable?.['@_typeRef'] || el['@_typeRef'] || 'string'
      return { name, typeRef: typeRef.toLowerCase() }
    })
    .filter(inp => inp.name)
  return inputs.length > 0 ? inputs : null
}

// Map DMN typeRef to a UI control type
function dmnTypeToControl(typeRef) {
  if (!typeRef) return 'text'
  const t = typeRef.toLowerCase()
  if (t === 'boolean') return 'boolean'
  if (t === 'number' || t === 'integer' || t === 'double' || t === 'long') return 'number'
  return 'text'
}

// Default value per control type
function defaultForType(controlType) {
  if (controlType === 'boolean') return false
  if (controlType === 'number') return 0
  return ''
}

// Build default facts from extracted DMN inputs
function buildDefaultFacts(inputs) {
  return Object.fromEntries(
    inputs.map(inp => [inp.name, defaultForType(dmnTypeToControl(inp.typeRef))])
  )
}

/* ── Test Panel ────────────────────────────────────────────────── */

// Fallback fields used when DMN model can't be parsed yet
const FALLBACK_INPUTS = [
  { name: 'filing_status', typeRef: 'string' },
  { name: 'agi', typeRef: 'number' },
  { name: 'deduction_ratio', typeRef: 'number' },
  { name: 'foreign_income', typeRef: 'number' },
  { name: 'se_income', typeRef: 'number' },
  { name: 'charitable_ratio', typeRef: 'number' },
  { name: 'prior_audit', typeRef: 'boolean' },
]

const FALLBACK_DEFAULTS = {
  filing_status: 'MFJ', agi: 85000, deduction_ratio: 0.28,
  foreign_income: 0, se_income: 0, charitable_ratio: 0.04, prior_audit: false,
}

// Friendly label from snake_case name
function fieldLabel(name) {
  return name.replace(/_/g, ' ').replace(/\b\w/g, c => c.toUpperCase())
}

function TestPanel({ model }) {
  const dmnInputs = model ? extractDmnInputs(model) : null
  const inputs = dmnInputs || FALLBACK_INPUTS
  const isDynamic = !!dmnInputs

  const [facts, setFacts] = useState(dmnInputs ? buildDefaultFacts(dmnInputs) : FALLBACK_DEFAULTS)
  const [result, setResult] = useState(null)
  const [loading, setLoading] = useState(false)

  // Re-sync facts when DMN inputs change (new fields added via UC browser)
  const prevInputNames = useRef(inputs.map(i => i.name).join(','))
  const inputKey = inputs.map(i => i.name).join(',')
  useEffect(() => {
    const names = inputKey
    if (names !== prevInputNames.current) {
      prevInputNames.current = names
      setFacts(prev => {
        const next = {}
        inputs.forEach(inp => {
          const ctrl = dmnTypeToControl(inp.typeRef)
          next[inp.name] = inp.name in prev ? prev[inp.name] : defaultForType(ctrl)
        })
        return next
      })
      setResult(null)
    }
  }, [inputKey])

  const handleEvaluate = async () => {
    setLoading(true); setResult(null)
    try { setResult(await api.evaluateReturn(facts)) }
    catch (e) { setResult({ error: e.message }) }
    finally { setLoading(false) }
  }

  const update = (k, v) => setFacts(f => ({ ...f, [k]: v }))

  return (
    <div className="panel-body">
      {isDynamic && (
        <div className="dynamic-badge">
          <span className="dyn-dot"></span>
          <span>{inputs.length} inputs from live DMN</span>
        </div>
      )}
      <div className="form-stack">
        {inputs.map(inp => {
          const ctrl = dmnTypeToControl(inp.typeRef)
          const val = facts[inp.name]
          const label = fieldLabel(inp.name)

          if (ctrl === 'boolean') {
            return (
              <label key={inp.name} className="field field-toggle">
                <span>{label}</span>
                <button
                  type="button"
                  className={`toggle ${val ? 'on' : ''}`}
                  onClick={() => update(inp.name, !val)}
                >{val ? 'YES' : 'NO'}</button>
              </label>
            )
          }

          // Special case: filing_status — render as select when it's the known enum field
          if (inp.name === 'filing_status') {
            return (
              <label key={inp.name} className="field">
                <span>{label}</span>
                <select value={val} onChange={e => update(inp.name, e.target.value)}>
                  <option value="MFJ">MFJ</option>
                  <option value="S">Single</option>
                  <option value="MFS">MFS</option>
                  <option value="HH">HH</option>
                  <option value="QSS">QSS</option>
                </select>
              </label>
            )
          }

          if (ctrl === 'number') {
            const isRatio = inp.name.includes('ratio')
            return (
              <label key={inp.name} className="field">
                <span>{label}</span>
                <input
                  type="number"
                  step={isRatio ? '0.01' : '1'}
                  value={val}
                  onChange={e => update(inp.name, +e.target.value)}
                />
              </label>
            )
          }

          return (
            <label key={inp.name} className="field">
              <span>{label}</span>
              <input
                type="text"
                value={val}
                onChange={e => update(inp.name, e.target.value)}
              />
            </label>
          )
        })}
      </div>
      <button onClick={handleEvaluate} disabled={loading} className="action-btn action-primary">
        {loading ? 'Evaluating…' : 'Evaluate Return'}
      </button>
      {result && !result.error && (
        <div className={`result-card result-${(result.recommended_action || 'unknown').toLowerCase()}`}>
          <div className="result-score">{result.audit_score}</div>
          <div className="result-action">{result.recommended_action}</div>
          <div className="result-hint">
            {result.recommended_action === 'PASS' && 'No action required'}
            {result.recommended_action === 'REVIEW' && 'Manual review recommended'}
            {result.recommended_action === 'FLAG' && 'High-priority audit candidate'}
          </div>
          {result.fired_rules?.length > 0 && (
            <div className="result-rules">
              {result.fired_rules.map((r, i) => <div key={i} className="result-rule">↳ {r}</div>)}
            </div>
          )}
        </div>
      )}
      {result?.error && <div className="error-msg">{result.error}</div>}

      <div className="eval-note">
        {isDynamic
          ? 'Fields match live DMN inputs. Batch scoring uses Drools on cluster.'
          : 'Test evaluation uses Python scoring (mirrors v1.0 rules). Batch scoring uses the active DMN via Drools on cluster.'}
      </div>

      <SampleReturns onSelect={(ret) => {
        setFacts(prev => {
          const next = { ...prev }
          Object.keys(ret).forEach(k => {
            if (!(k in next)) return
            const inp = inputs.find(i => i.name === k)
            const ctrl = inp ? dmnTypeToControl(inp.typeRef) : 'text'
            if (ctrl === 'boolean') next[k] = ret[k] === true || ret[k] === 'true'
            else if (ctrl === 'number') next[k] = Number(ret[k] || 0)
            else next[k] = ret[k] ?? ''
          })
          return next
        })
        setResult(null)
      }} />
    </div>
  )
}

/* ── Sample Returns ────────────────────────────────────────────── */

function SampleReturns({ onSelect }) {
  const [returns, setReturns] = useState([])
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState(null)

  const fetchSample = async () => {
    setError(null)
    setLoading(true)
    try { setReturns(await api.getSampleReturns(10)) }
    catch (e) { console.error(e); setError(e.message) }
    setLoading(false)
  }

  return (
    <div className="sample-returns">
      <div className="sample-header">
        <span className="section-label-inline">SAMPLE RETURNS</span>
        <button onClick={fetchSample} disabled={loading} className="preset-btn" style={{ width: 'auto', flex: 'none', padding: '3px 10px' }}>
          {loading ? '…' : returns.length ? 'Refresh' : 'Load Sample'}
        </button>
      </div>
      {returns.length > 0 && (
        <div className="sample-list">
          {returns.map((ret, i) => (
            <div key={ret.return_id || i} className="sample-row" onClick={() => onSelect(ret)}>
              <div className="sample-id">{ret.return_id}</div>
              <div className="sample-detail">
                <span>{ret.filing_status}</span>
                <span>${Number(ret.agi).toLocaleString()}</span>
                {Number(ret.se_income) > 0 && <span>SE</span>}
                {Number(ret.foreign_income) > 0 && <span>FI</span>}
                {(ret.prior_audit === true || ret.prior_audit === 'true') && <span>PA</span>}
              </div>
            </div>
          ))}
        </div>
      )}
      {error && <div className="error-msg" style={{ marginTop: 6 }}>{error}</div>}
    </div>
  )
}

/* ── Unity Catalog Browser ─────────────────────────────────────── */

// Map UC type strings to DMN typeRef strings
function ucTypeToDmn(ucType) {
  const t = (ucType || '').toUpperCase()
  if (['BOOLEAN'].includes(t)) return 'boolean'
  if (['INT', 'INTEGER', 'LONG', 'SHORT', 'BYTE', 'FLOAT', 'DOUBLE', 'DECIMAL', 'BIGINT', 'SMALLINT', 'TINYINT', 'REAL'].includes(t)) return 'number'
  return 'string'
}

function CatalogBrowser({ onAddInput }) {
  const [catalogs, setCatalogs] = useState([])
  const [selectedCatalog, setSelectedCatalog] = useState('')
  const [schemas, setSchemas] = useState([])
  const [selectedSchema, setSelectedSchema] = useState('')
  const [tables, setTables] = useState([])
  const [selectedTable, setSelectedTable] = useState(null)
  const [columns, setColumns] = useState([])
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState(null)
  const [added, setAdded] = useState(new Set())

  // Load catalogs on mount
  useEffect(() => {
    setLoading(true)
    api.listCatalogs()
      .then(cs => { setCatalogs(cs); setLoading(false) })
      .catch(e => { setError(e.message); setLoading(false) })
  }, [])

  const handleCatalogChange = async (cat) => {
    setError(null)
    setSelectedCatalog(cat)
    setSelectedSchema('')
    setSchemas([])
    setTables([])
    setSelectedTable(null)
    setColumns([])
    if (!cat) return
    setLoading(true)
    try { setSchemas(await api.listSchemas(cat)) }
    catch (e) { setError(e.message) }
    setLoading(false)
  }

  const handleSchemaChange = async (schema) => {
    setError(null)
    setSelectedSchema(schema)
    setTables([])
    setSelectedTable(null)
    setColumns([])
    if (!schema) return
    setLoading(true)
    try { setTables(await api.listTables(selectedCatalog, schema)) }
    catch (e) { setError(e.message) }
    setLoading(false)
  }

  const handleTableClick = async (table) => {
    setError(null)
    setSelectedTable(table)
    setColumns([])
    setLoading(true)
    try {
      const res = await api.listColumns(selectedCatalog, selectedSchema, table.name)
      setColumns(res.columns)
    }
    catch (e) { setError(e.message) }
    setLoading(false)
  }

  const handleAdd = (col) => {
    const key = `${selectedTable.name}.${col.name}`
    onAddInput({ name: col.name, typeRef: ucTypeToDmn(col.type), sourceTable: selectedTable.full_name })
    setAdded(prev => new Set([...prev, key]))
  }

  return (
    <div className="panel-body catalog-browser">
      <div className="catalog-intro">
        Browse Unity Catalog to add table columns as DMN inputs.
      </div>

      {error && <div className="error-msg" style={{ marginBottom: 8 }}>{error} <button className="preset-btn" onClick={() => setError(null)} style={{ marginLeft: 4 }}>×</button></div>}

      {/* Catalog picker */}
      <label className="field">
        <span>Catalog</span>
        <select value={selectedCatalog} onChange={e => handleCatalogChange(e.target.value)} disabled={loading && !catalogs.length}>
          <option value="">— select —</option>
          {catalogs.map(c => <option key={c} value={c}>{c}</option>)}
        </select>
      </label>

      {/* Schema picker */}
      {selectedCatalog && (
        <label className="field">
          <span>Schema</span>
          <select value={selectedSchema} onChange={e => handleSchemaChange(e.target.value)} disabled={loading && !schemas.length}>
            <option value="">— select —</option>
            {schemas.map(s => <option key={s} value={s}>{s}</option>)}
          </select>
        </label>
      )}

      {/* Tables list */}
      {selectedSchema && tables.length > 0 && (
        <div className="catalog-section">
          <div className="section-label-inline" style={{ marginBottom: 4 }}>TABLES</div>
          <div className="catalog-table-list">
            {tables.map(t => (
              <button
                key={t.name}
                className={`catalog-table-row ${selectedTable?.name === t.name ? 'selected' : ''}`}
                onClick={() => handleTableClick(t)}
              >
                <span className="catalog-table-icon">▤</span>
                <span className="catalog-table-name">{t.name}</span>
              </button>
            ))}
          </div>
        </div>
      )}

      {loading && <div className="catalog-loading">Loading…</div>}

      {/* Columns list */}
      {selectedTable && columns.length > 0 && (
        <div className="catalog-section">
          <div className="catalog-col-header">
            <div className="section-label-inline">{selectedTable.name}</div>
            <div className="catalog-col-count">{columns.length} cols</div>
          </div>
          <div className="catalog-col-list">
            {columns.map(col => {
              const key = `${selectedTable.name}.${col.name}`
              const isAdded = added.has(key)
              return (
                <div key={col.name} className="catalog-col-row">
                  <div className="catalog-col-info">
                    <span className="catalog-col-name">{col.name}</span>
                    <span className="catalog-col-type">{col.type}</span>
                  </div>
                  <button
                    className={`catalog-add-btn ${isAdded ? 'added' : ''}`}
                    onClick={() => handleAdd(col)}
                    disabled={isAdded}
                    title={isAdded ? 'Already added to DMN' : `Add ${col.name} as DMN input`}
                  >
                    {isAdded ? '✓' : '+'}
                  </button>
                </div>
              )
            })}
          </div>
        </div>
      )}

      {selectedTable && !loading && columns.length === 0 && (
        <div className="catalog-empty">No columns found.</div>
      )}
    </div>
  )
}

/* ── Scoring Panel ─────────────────────────────────────────────── */
// Parse notebook exit output: "SUCCESS: scored=1000 errors=0 dmn_version=v1.0"
function parseOutput(output) {
  if (!output) return {}
  return {
    scored: output.match(/scored=(\d+)/)?.[1],
    errors: output.match(/errors=(\d+)/)?.[1],
    version: output.match(/dmn_version=(\S+)/)?.[1],
  }
}

function fmtDur(s) {
  if (!s) return null
  return s < 60 ? `${s}s` : `${Math.floor(s/60)}m ${s%60}s`
}

function ScoringPanel() {
  const [results, setResults] = React.useState(null)
  const [batchSize, setBatchSize] = React.useState('10000')
  const [selectedVersion, setSelectedVersion] = React.useState('')  // '' = use ACTIVE
  const [versions, setVersions] = React.useState([])
  const [activeRun, setActiveRun] = React.useState(null)
  const [serverHistory, setServerHistory] = React.useState([])
  const [submitting, setSubmitting] = React.useState(false)
  const [historyLoading, setHistoryLoading] = React.useState(true)

  const refresh = () => {
    api.getScoringResults().then(setResults).catch(() => {})
    setHistoryLoading(true)
    api.getScoringHistory().then(h => { setServerHistory(h); setHistoryLoading(false) }).catch(() => setHistoryLoading(false))
  }

  React.useEffect(() => {
    refresh()
    api.fetchVersions().then(v => setVersions(v)).catch(() => {})
  }, [])

  React.useEffect(() => {
    if (!activeRun || activeRun.done) return
    const lcs = activeRun.status?.life_cycle_state
    if (lcs === 'TERMINATED' || lcs === 'INTERNAL_ERROR') return
    const interval = setInterval(async () => {
      try {
        const status = await api.getScoringStatus(activeRun.runId)
        const elapsed = Math.round((Date.now() - activeRun.submittedAt) / 1000)
        setActiveRun(prev => ({ ...prev, status, elapsed }))
        if (status.life_cycle_state === 'TERMINATED' || status.life_cycle_state === 'INTERNAL_ERROR') {
          setActiveRun(prev => ({ ...prev, done: true }))
          setTimeout(refresh, 2000)
        }
      } catch (err) {
        if (err.notFound) {
          setActiveRun(prev => ({ ...prev, done: true }))
        }
        /* otherwise keep polling */
      }
    }, 5000)
    return () => clearInterval(interval)
  }, [activeRun?.runId, activeRun?.status?.life_cycle_state])

  const handleRun = async () => {
    if (submitting || isRunning) return  // hard guard against double-click
    setSubmitting(true)
    const limit = batchSize === 'all' ? null : Number(batchSize)
    const label = batchSize === 'all' ? 'All' : batchSize === '1000000' ? '1M' : batchSize === '100000' ? '100K' : batchSize === '10000' ? '10K' : batchSize === '1000' ? '1K' : batchSize
    try {
      const r = await api.runScoring(limit, selectedVersion || null)
      setActiveRun({ runId: r.run_id, runUrl: r.run_url, limit: label,
        version: selectedVersion || 'ACTIVE',
        submittedAt: Date.now(), status: { life_cycle_state: 'PENDING' }, elapsed: 0, done: false })
    } catch (e) {
      setActiveRun({ runId: null, limit: label, submittedAt: Date.now(),
        status: { life_cycle_state: 'FAILED', state_message: e.message }, done: true })
    } finally {
      setSubmitting(false)
    }
  }

  const s = results?.summary || {}
  const total = Number(s.total || 0)
  const isRunning = activeRun && !activeRun.done &&
    ['PENDING', 'RUNNING'].includes(activeRun.status?.life_cycle_state)
  const btnDisabled = submitting || isRunning

  const sizeOptions = [
    { value: '100', label: '100' }, { value: '1000', label: '1K' },
    { value: '10000', label: '10K' }, { value: '100000', label: '100K' },
    { value: '1000000', label: '1M' }, { value: 'all', label: 'All' },
  ]

  // Merge session run + server history (deduped by run_id)
  const sessionEntry = activeRun ? [{
    run_id: activeRun.runId, run_page_url: activeRun.runUrl,
    life_cycle_state: activeRun.status?.life_cycle_state,
    result_state: activeRun.status?.result_state,
    state_message: activeRun.status?.state_message || '',
    duration_s: activeRun.elapsed, notebook_output: null,
    _limit: activeRun.limit, _version: activeRun.version, _session: true,
  }] : []
  const historyEntries = [
    ...sessionEntry,
    ...serverHistory.filter(h => h.run_id !== activeRun?.runId),
  ].slice(0, 8)

  return (
    <div className="panel-body">
      {total > 0 && (
        <>
          <div className="score-hero">
            <div className="score-hero-num">{total.toLocaleString()}</div>
            <div className="score-hero-label">Returns Scored</div>
          </div>
          <div className="stacked-bar">
            <div className="bar-seg bar-pass" style={{ flex: Number(s.pass_count) }}></div>
            <div className="bar-seg bar-review" style={{ flex: Number(s.review_count) }}></div>
            <div className="bar-seg bar-flag" style={{ flex: Number(s.flag_count) }}></div>
          </div>
          <div className="breakdown">
            <div className="bd-row"><span className="bd-dot dot-pass"></span><span className="bd-label">PASS</span><span className="bd-val">{Number(s.pass_count).toLocaleString()}</span><span className="bd-pct">{(Number(s.pass_count)/total*100).toFixed(1)}%</span></div>
            <div className="bd-row"><span className="bd-dot dot-review"></span><span className="bd-label">REVIEW</span><span className="bd-val">{Number(s.review_count).toLocaleString()}</span><span className="bd-pct">{(Number(s.review_count)/total*100).toFixed(1)}%</span></div>
            <div className="bd-row"><span className="bd-dot dot-flag"></span><span className="bd-label">FLAG</span><span className="bd-val">{Number(s.flag_count).toLocaleString()}</span><span className="bd-pct">{(Number(s.flag_count)/total*100).toFixed(1)}%</span></div>
          </div>
          <div className="meta-row">
            <span>Avg {s.avg_score}</span><span>·</span><span>{s.dmn_version}</span>
          </div>
        </>
      )}

      <div className="version-select-row">
        <span className="section-label-inline">RULE VERSION</span>
        <select
          value={selectedVersion}
          onChange={e => setSelectedVersion(e.target.value)}
          className="version-select"
        >
          <option value="">Active ({versions.find(v => v.status === 'ACTIVE')?.version_id || '…'})</option>
          {versions.filter(v => v.status !== 'ARCHIVED').map(v => (
            <option key={v.version_id} value={v.version_id}>
              {v.version_id} — {v.status}
            </option>
          ))}
        </select>
      </div>
      <div className="batch-size-row">
        <span className="section-label-inline">RETURNS TO SCORE</span>
        <div className="size-options">
          {sizeOptions.map(opt => (
            <button key={opt.value} className={`size-opt ${batchSize === opt.value ? 'active' : ''}`}
              onClick={() => setBatchSize(opt.value)}>{opt.label}</button>
          ))}
        </div>
      </div>
      <button onClick={handleRun} disabled={btnDisabled} className="action-btn action-dark">
        {submitting ? 'Submitting…' : isRunning ? `${activeRun.status?.life_cycle_state || 'Running'}\u2026` : 'Run Batch Scoring'}
      </button>

      {historyLoading ? (
        <div className="run-history">
          <div className="run-history-header"><span className="section-label-inline">RUN HISTORY</span></div>
          <div style={{ padding: '8px 0', color: 'var(--steel)', fontSize: 11 }}>Loading…</div>
        </div>
      ) : historyEntries.length > 0 && (
        <div className="run-history">
          <div className="run-history-header">
            <span className="section-label-inline">RUN HISTORY</span>
            <button onClick={refresh} className="icon-btn" style={{ fontSize: 11 }}>↻</button>
          </div>
          {historyEntries.map((h, i) => {
            const parsed = parseOutput(h.notebook_output)
            const lcs = h.life_cycle_state
            const rs = h.result_state
            const active = lcs === 'PENDING' || lcs === 'RUNNING'
            const ok = rs === 'SUCCESS'
            const bad = rs === 'FAILED' || lcs === 'INTERNAL_ERROR'
            const dur = fmtDur(h.duration_s)

            return (
              <div key={h.run_id || i} className="run-row">
                <div className="run-row-indicator">
                  {active && <div className="run-dot-spinner"></div>}
                  {ok && <div className="run-dot run-dot-success">✓</div>}
                  {bad && <div className="run-dot run-dot-failed">✗</div>}
                  {!active && !ok && !bad && <div className="run-dot">·</div>}
                </div>
                <div className="run-row-body">
                  <div className="run-row-top">
                    <span className="run-limit">
                      {parsed.scored
                        ? `${Number(parsed.scored).toLocaleString()} returns`
                        : h._limit
                          ? `${h._limit} returns`
                          : h.run_name
                            ? h.run_name.replace('rules-app-scoring-', '').replace(/^\d{8}-\d{6}$/, '').trim() || 'batch scoring'
                            : 'batch scoring'}
                    </span>
                    {h.run_page_url && (
                      <a className="run-id-link" href={h.run_page_url} target="_blank" rel="noreferrer">↗ view</a>
                    )}
                  </div>
                  <div className="run-row-meta">
                    {(parsed.version || h._version) && <span className="run-version">{parsed.version || h._version}</span>}
                    {dur && <span className="run-dur">{dur}</span>}
                    <span className={`run-state-badge ${ok ? 'state-success' : bad ? 'state-failed' : 'state-running'}`}>
                      {active
                        ? (h.state_message && h.state_message !== 'In run'
                            ? h.state_message
                            : lcs === 'PENDING' ? 'Waiting for cluster…' : `Running · ${h.duration_s||0}s`)
                        : rs || lcs || 'unknown'}
                    </span>
                  </div>
                </div>
              </div>
            )
          })}
        </div>
      )}
    </div>
  )
}

/* ── Main App ──────────────────────────────────────────────────── */

export default function App() {
  const [model, setModel] = useState(null)
  const [originalVersion, setOriginalVersion] = useState(undefined)
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState(null)
  const [dirty, setDirty] = useState(false)
  const [versions, setVersions] = useState([])
  const [currentVersionId, setCurrentVersionId] = useState(null)
  const [rightPanel, setRightPanel] = useState('test')
  const [saving, setSaving] = useState(false)
  const [leftOpen, setLeftOpen] = useState(true)
  const [rightOpen, setRightOpen] = useState(true)
  const [showArch, setShowArch] = useState(false)
  const marshallerRef = useRef(null)
  const fileInputRef = useRef(null)

  const refreshVersions = useCallback(async () => {
    try { setVersions(await api.fetchVersions()) } catch (e) { console.error(e) }
  }, [])

  const loadVersion = useCallback(async (versionId) => {
    setLoading(true); setError(null)
    try {
      const data = await api.fetchDmn(versionId)
      const m = getMarshaller(data.dmn_xml)
      marshallerRef.current = m
      setOriginalVersion(m.originalVersion)
      setModel(normalize(m.parser.parse()))
      setCurrentVersionId(versionId)
      setDirty(false)
    } catch (e) { setError(e.message) }
    setLoading(false)
  }, [])

  useEffect(() => {
    (async () => {
      await refreshVersions()
      try {
        const active = await api.fetchActiveDmn()
        const m = getMarshaller(active.dmn_xml)
        marshallerRef.current = m
        setOriginalVersion(m.originalVersion)
        setModel(normalize(m.parser.parse()))
        setCurrentVersionId(active.version_id)
      } catch (e) { setError(e.message) }
      setLoading(false)
    })()
  }, [])

  const handleModelChange = useCallback((m) => { setModel(m); setDirty(true) }, [])

  const handleSave = useCallback(async () => {
    if (!marshallerRef.current || !model) return
    setSaving(true)
    try {
      const xml = marshallerRef.current.builder.build(model)
      const notes = window.prompt('Version notes (optional):') ?? ''
      if (notes === null) return
      const result = await api.saveDmn(xml, notes)
      setCurrentVersionId(result.version_id)
      setDirty(false)
      await refreshVersions()
    } catch (e) { alert(`Save failed: ${e.message}`) }
    finally { setSaving(false) }
  }, [model, refreshVersions])

  const handlePromote = useCallback(async () => {
    if (!currentVersionId) return
    if (!confirm(`Promote ${currentVersionId} to ACTIVE?`)) return
    try { await api.promoteVersion(currentVersionId); await refreshVersions() }
    catch (e) { alert(`Promote failed: ${e.message}`) }
  }, [currentVersionId, refreshVersions])

  const handleImport = useCallback((e) => {
    const file = e.target.files?.[0]
    if (!file) return
    const reader = new FileReader()
    reader.onload = async (evt) => {
      const xml = evt.target.result
      try {
        // Parse and load into editor
        const m = getMarshaller(xml)
        marshallerRef.current = m
        setOriginalVersion(m.originalVersion)
        setModel(normalize(m.parser.parse()))
        setDirty(true)
        setCurrentVersionId(null) // Imported, not yet saved

        // Auto-save as draft
        const notes = `Imported from ${file.name}`
        const result = await api.saveDmn(xml, notes)
        setCurrentVersionId(result.version_id)
        setDirty(false)
        await refreshVersions()
      } catch (err) {
        alert(`Import failed: ${err.message}\n\nMake sure the file is valid DMN XML.`)
      }
    }
    reader.readAsText(file)
    // Reset the input so the same file can be imported again
    e.target.value = ''
  }, [refreshVersions])

  const currentStatus = versions.find(v => v.version_id === currentVersionId)?.status

  if (loading) return (
    <div className="splash">
      <div className="splash-brand">IRS Rules Engine</div>
      <div className="splash-sub">Loading decision model…</div>
      <div className="splash-bar"><div className="splash-bar-fill"></div></div>
    </div>
  )

  if (error && !model) return (
    <div className="splash">
      <div className="splash-brand" style={{ color: '#c0392b' }}>Connection Error</div>
      <div className="splash-sub">{error}</div>
    </div>
  )

  return (
    <div className="app">
      {showArch && <ArchPage onClose={() => setShowArch(false)} />}
      <header className="header">
        <div className="header-left">
          <button className="sidebar-toggle" onClick={() => setLeftOpen(o => !o)} title="Toggle versions">☰</button>
          <div className="brand">
            <svg width="20" height="20" viewBox="0 0 32 32" style={{ borderRadius: 4, flexShrink: 0 }}>
              <rect width="32" height="32" rx="6" fill="#1a3d5c"/>
              <rect x="6" y="8" width="20" height="3" rx="1.5" fill="#60a5fa"/>
              <rect x="6" y="13" width="14" height="3" rx="1.5" fill="#93c5fd"/>
              <rect x="6" y="18" width="17" height="3" rx="1.5" fill="#bfdbfe"/>
              <polygon points="18,24 26,24 22,28" fill="#2563eb" opacity="0.9"/>
            </svg>
            IRS Rules Engine
          </div>
          <div className="header-sep"></div>
          {currentVersionId && (
            <div className="header-meta">
              <span className="header-vid">{currentVersionId}</span>
              <span className={`pill pill-${currentStatus?.toLowerCase()}`}>{currentStatus}</span>
              {dirty && <span className="pill pill-dirty">Modified</span>}
            </div>
          )}
        </div>
        <div className="header-right">
          <input
            ref={fileInputRef}
            type="file"
            accept=".dmn,.xml"
            onChange={handleImport}
            style={{ display: 'none' }}
          />
          <button className="hdr-btn hdr-btn-ghost" onClick={() => setShowArch(true)}>Architecture</button>
          <button className="hdr-btn hdr-btn-ghost" onClick={() => fileInputRef.current?.click()}>
            Import DMN
          </button>
          <button className="hdr-btn hdr-btn-ghost" onClick={handlePromote} disabled={currentStatus === 'ACTIVE'}>Promote</button>
          <button className="hdr-btn hdr-btn-accent" onClick={handleSave} disabled={saving}>{saving ? 'Saving…' : 'Save Draft'}</button>
          <button className="sidebar-toggle" onClick={() => setRightOpen(o => !o)} title="Toggle panel">☰</button>
        </div>
      </header>

      <div className="body">
        {leftOpen && (
          <aside className="sidebar left">
            <VersionPanel versions={versions} activeVersionId={currentVersionId} onSelect={loadVersion} onRefresh={refreshVersions} />
          </aside>
        )}

        <main className="editor">
          {model && <DmnEditor model={model} originalVersion={originalVersion} onModelChange={handleModelChange} />}
        </main>

        {rightOpen && (
          <aside className="sidebar right">
            <div className="tab-bar">
              <button className={`tab ${rightPanel === 'test' ? 'active' : ''}`} onClick={() => setRightPanel('test')}>Test</button>
              <button className={`tab ${rightPanel === 'scoring' ? 'active' : ''}`} onClick={() => setRightPanel('scoring')}>Scoring</button>
              <button className={`tab ${rightPanel === 'inputs' ? 'active' : ''}`} onClick={() => setRightPanel('inputs')}>Inputs</button>
            </div>
            {rightPanel === 'test' && <TestPanel model={model} />}
            {rightPanel === 'scoring' && <ScoringPanel />}
            {rightPanel === 'inputs' && (
              <CatalogBrowser onAddInput={(inp) => {
                if (!model) return
                // Inject a new inputData node into the live DMN model
                const newNode = {
                  __$$element: 'inputData',
                  '@_id': `input-${Date.now()}`,
                  '@_name': inp.name,
                  variable: {
                    '@_id': `var-${Date.now()}`,
                    '@_name': inp.name,
                    '@_typeRef': inp.typeRef,
                  },
                }
                const defs = model.definitions
                const existing = (defs.drgElement || [])
                // Don't add duplicates
                if (existing.some(el => el['@_name'] === inp.name && el.__$$element === 'inputData')) return
                const updated = {
                  ...model,
                  definitions: {
                    ...defs,
                    drgElement: [...existing, newNode],
                  },
                }
                handleModelChange(updated)
              }} />
            )}
          </aside>
        )}
      </div>
    </div>
  )
}
