const BASE = '/api'

export async function fetchVersions() {
  const r = await fetch(`${BASE}/dmn/versions`)
  if (!r.ok) throw new Error(`Failed to list versions: ${r.status}`)
  return (await r.json()).versions
}

export async function fetchDmn(versionId) {
  const r = await fetch(`${BASE}/dmn/${versionId}`)
  if (!r.ok) throw new Error(`Failed to load DMN: ${r.status}`)
  return r.json()
}

export async function fetchActiveDmn() {
  const r = await fetch(`${BASE}/dmn/active`)
  if (!r.ok) throw new Error(`Failed to load active DMN: ${r.status}`)
  return r.json()
}

export async function saveDmn(dmnXml, notes = '') {
  const r = await fetch(`${BASE}/dmn/save`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ dmn_xml: dmnXml, notes }),
  })
  if (!r.ok) {
    const body = await r.json().catch(() => null)
    throw new Error(body?.detail || `Operation failed: ${r.status}`)
  }
  return r.json()
}

export async function promoteVersion(versionId) {
  const r = await fetch(`${BASE}/dmn/promote`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ version_id: versionId }),
  })
  if (!r.ok) {
    const body = await r.json().catch(() => null)
    throw new Error(body?.detail || `Operation failed: ${r.status}`)
  }
  return r.json()
}

export async function evaluateReturn(facts) {
  const r = await fetch(`${BASE}/dmn/evaluate`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(facts),
  })
  if (!r.ok) {
    const body = await r.json().catch(() => null)
    throw new Error(body?.detail || `Operation failed: ${r.status}`)
  }
  return r.json()
}

export async function getScoringResults() {
  const r = await fetch(`${BASE}/scoring/results`)
  if (!r.ok) throw new Error(`Failed to get results: ${r.status}`)
  return r.json()
}

export async function runScoring(limit = null, versionId = null) {
  const body = {}
  if (limit !== null) body.limit = limit
  if (versionId) body.version_id = versionId
  const r = await fetch(`${BASE}/scoring/run`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(body),
  })
  if (!r.ok) {
    const body2 = await r.json().catch(() => null)
    throw new Error(body2?.detail || `Operation failed: ${r.status}`)
  }
  return r.json()
}

export async function getScoringHistory() {
  const r = await fetch(`${BASE}/scoring/history`)
  if (!r.ok) throw new Error(`Failed to get history: ${r.status}`)
  return (await r.json()).history
}

export async function getScoringStatus(runId) {
  const r = await fetch(`${BASE}/scoring/run/${runId}`)
  if (r.status === 404) throw Object.assign(new Error('Run not found'), { notFound: true })
  if (!r.ok) {
    const body = await r.json().catch(() => null)
    throw new Error(body?.detail || `Failed to get status: ${r.status}`)
  }
  return r.json()
}

export async function getSampleReturns(count = 10) {
  const r = await fetch(`${BASE}/dmn/sample-returns?count=${count}`)
  if (!r.ok) throw new Error(`Failed to get sample returns: ${r.status}`)
  return (await r.json()).returns
}

// ── Unity Catalog Browser ─────────────────────────────────────────────

export async function listCatalogs() {
  const r = await fetch(`${BASE}/catalog/catalogs`)
  if (!r.ok) throw new Error(`Failed to list catalogs: ${r.status}`)
  return (await r.json()).catalogs
}

export async function listSchemas(catalog) {
  const r = await fetch(`${BASE}/catalog/schemas?catalog=${encodeURIComponent(catalog)}`)
  if (!r.ok) throw new Error(`Failed to list schemas: ${r.status}`)
  return (await r.json()).schemas
}

export async function listTables(catalog, schema) {
  const r = await fetch(`${BASE}/catalog/tables?catalog=${encodeURIComponent(catalog)}&schema=${encodeURIComponent(schema)}`)
  if (!r.ok) throw new Error(`Failed to list tables: ${r.status}`)
  return (await r.json()).tables
}

export async function listColumns(catalog, schema, table) {
  const r = await fetch(`${BASE}/catalog/columns?catalog=${encodeURIComponent(catalog)}&schema=${encodeURIComponent(schema)}&table=${encodeURIComponent(table)}`)
  if (!r.ok) throw new Error(`Failed to list columns: ${r.status}`)
  return (await r.json())
}
