// server.js
import express from 'express'
import cors from 'cors'
import fetch from 'node-fetch'
import JSONStream from 'JSONStream'

const app = express()
const PORT = process.env.PORT || 8081

const UPSTREAM_URL =
  'https://apicloudfruit.ranco.cl/produccion/cb/CB_GenerarDataReportes/InformeGestionCuentasCorrientes/20169/%20/%20/%20/json/10112/0'
const UP_USER = process.env.UP_USER || 'Pdiaz@ranco.cl'
const UP_PASS = process.env.UP_PASS || 'Pato2023'

const UPSTREAM_TIMEOUT_MS = 600_000  // 10 min
const CACHE_TTL_MS        = 600_000  // 10 min

const CAMPOS = [
  'IdFactEmbarque', 'ClaveEmbarque', 'EstadoLiquidacion',
  'CodigoRecibidor', 'Recibidor', 'CodigoTipoNave', 'TipoNave',
  'Moneda', 'FOBChileAjustado', 'TotalFOBChile',
  'Contenedor', 'TiCaCursoLegal', 'FechaLiquidacion',
  'FechaDespacho', 'Naviera', 'Destino', 'CodigoTemporada',
]

app.use(cors({
  origin: ['http://localhost:5173', 'http://localhost:3000'],
  methods: ['GET'],
}))
app.use(express.json())

// ─── Cache ────────────────────────────────────────────────────────────────────
const cache = new Map()

function getCache(key) {
  const v = cache.get(key)
  if (!v) return null
  if (Date.now() > v.expiresAt) { cache.delete(key); return null }
  return v.value
}

function setCache(key, value) {
  cache.set(key, { expiresAt: Date.now() + CACHE_TTL_MS, value })
}

// ─── Progreso SSE ─────────────────────────────────────────────────────────────
let currentProgress = { count: 0, mb: 0, status: 'idle' }

app.get('/api/progress', (req, res) => {
  res.setHeader('Content-Type', 'text/event-stream')
  res.setHeader('Cache-Control', 'no-cache')
  res.setHeader('Connection', 'keep-alive')

  const sendProgress = () => {
    res.write(`data: ${JSON.stringify(currentProgress)}\n\n`)
  }

  sendProgress() // Enviar estado inmediatamente al conectar
  const interval = setInterval(sendProgress, 1000)
  req.on('close', () => clearInterval(interval))
})

// ─── fetchUpstream ────────────────────────────────────────────────────────────
async function fetchUpstream() {
  const cached = getCache('upstream:slim')
  if (cached) return cached

  currentProgress = { count: 0, mb: 0, status: 'descargando' }
  const auth = Buffer.from(`${UP_USER}:${UP_PASS}`).toString('base64')
  const startedAt = Date.now()
  console.log('🔄 Descargando upstream en streaming...')

  const controller = new AbortController()
  const timer = setTimeout(() => controller.abort(), UPSTREAM_TIMEOUT_MS)

  try {
    const res = await fetch(UPSTREAM_URL, {
      headers: { Authorization: `Basic ${auth}`, Accept: 'application/json' },
      signal: controller.signal,
    })

    if (!res.ok) {
      const text = await res.text().catch(() => '')
      throw new Error(`Upstream HTTP ${res.status}: ${text}`)
    }

    const rows = await new Promise((resolve, reject) => {
      const collected = []
      let totalBytes = 0
      const jsonStream = JSONStream.parse('Data.*')

      jsonStream.on('data', (raw) => {
        const obj = {}
        for (const f of CAMPOS) {
          if (raw[f] !== undefined) obj[f] = raw[f]
        }
        collected.push(obj)
        currentProgress.count++
        if (currentProgress.count % 5000 === 0) {
          currentProgress.mb = Math.round(totalBytes / 1024 / 1024)
          process.stdout.write(`\r📥 ${currentProgress.count} filas | ${currentProgress.mb} MB`)
        }
      })

      jsonStream.on('end', () => {
        process.stdout.write('\n')
          const tookMs = Date.now() - startedAt
          console.log(`✅ ${currentProgress.count} filas en ${(tookMs / 1000).toFixed(1)}s`)

          currentProgress.status = 'completado' // Se pone en completado

          // AGREGA ESTO: Volver a idle después de 10 segundos para que no aparezca a usuarios nuevos
          setTimeout(() => {
            currentProgress.status = 'idle'
          }, 10000)

          resolve(collected)
      })

      jsonStream.on('error', (e) => {
        currentProgress.status = 'error'
        reject(e)
      })

      res.body.on('data', (chunk) => {
        totalBytes += chunk.length
        jsonStream.write(chunk)
      })
      res.body.on('end', () => jsonStream.end())
      res.body.on('error', (e) => {
        currentProgress.status = 'error'
        reject(e)
      })
    })

    setCache('upstream:slim', rows)
    return rows

  } catch (e) {
    currentProgress.status = 'error'
    throw e
  } finally {
    clearTimeout(timer)
  }
}

// ─── Normalizar ───────────────────────────────────────────────────────────────
function normalizeRow(r) {
  return {
    idFactEmbarque:   r.IdFactEmbarque,
    claveEmbarque:    r.ClaveEmbarque     || '',
    estado:           r.EstadoLiquidacion || '',
    recibidor:        r.CodigoRecibidor   || '',
    recibidorNombre:  r.Recibidor         || '',
    tipoNave:         r.CodigoTipoNave    || '',
    tipoNaveNombre:   r.TipoNave          || '',
    moneda:           r.Moneda            || '',
    totalFobNum:      Number(r.FOBChileAjustado ?? r.TotalFOBChile ?? 0),
    contenedor:       r.Contenedor        || '',
    tipoCambio:       r.TiCaCursoLegal    ?? null,
    fechaLiquidacion: r.FechaLiquidacion  ?? null,
    fechaDespacho:    r.FechaDespacho     ?? null,
    naviera:          r.Naviera           || '',
    destino:          r.Destino           || '',
    temporada:        r.CodigoTemporada   || '',
  }
}

// ─── Agregar por embarque ─────────────────────────────────────────────────────
function aggregateByEmbarque(rows) {
  const map = new Map()

  for (const r of rows) {
    const norm = normalizeRow(r)
    const key  = norm.claveEmbarque
    if (!key) continue

    if (!map.has(key)) map.set(key, { ...norm, totalFobNum: 0 })

    const entry = map.get(key)
    entry.totalFobNum     += Number.isFinite(norm.totalFobNum) ? norm.totalFobNum : 0
    entry.estado           = norm.estado
    entry.recibidor        = norm.recibidor
    entry.recibidorNombre  = norm.recibidorNombre
    entry.tipoNave         = norm.tipoNave
    entry.tipoNaveNombre   = norm.tipoNaveNombre
    entry.moneda           = norm.moneda
    entry.contenedor       = norm.contenedor
    entry.tipoCambio       = norm.tipoCambio
    entry.fechaLiquidacion = norm.fechaLiquidacion
    entry.fechaDespacho    = norm.fechaDespacho
  }

  return Array.from(map.values())
    .map(x => ({
      ...x,
      id: x.claveEmbarque,
      totalFob: x.totalFobNum.toLocaleString('en-US', {
        minimumFractionDigits: 2,
        maximumFractionDigits: 2,
      }),
    }))
    .sort((a, b) => a.claveEmbarque.localeCompare(b.claveEmbarque))
}

// ─── Rutas ────────────────────────────────────────────────────────────────────
app.get('/api/liquidaciones', async (req, res) => {
  const { recibidor = '', claveEmbarque = '', estado = '' } = req.query

  const cacheKey = `liq:${recibidor}|${claveEmbarque}|${estado}`
  const cached   = getCache(cacheKey)
  if (cached) return res.json({ data: cached, cached: true })

  try {
    const allRows = await fetchUpstream()

    const fRec = String(recibidor).trim().toUpperCase()
    const fCla = String(claveEmbarque).trim().toUpperCase()
    const fEst = String(estado).trim().toUpperCase()

    const filtered = allRows.filter(r => {
      const rr = String(r?.CodigoRecibidor   || '').toUpperCase()
      const cc = String(r?.ClaveEmbarque     || '').toUpperCase()
      const ee = String(r?.EstadoLiquidacion || '').toUpperCase()
      return (
        (!fRec || rr.includes(fRec)) &&
        (!fCla || cc.includes(fCla)) &&
        (!fEst || ee.includes(fEst))
      )
    })

    const data = aggregateByEmbarque(filtered)
    setCache(cacheKey, data)
    res.json({ data, total: data.length, cached: false })

  } catch (e) {
    console.error('❌ /api/liquidaciones:', e.message)
    res.status(502).json({ error: e.message })
  }
})

app.get('/api/liquidaciones/opciones', async (_req, res) => {
  const cacheKey = 'opciones'
  const cached   = getCache(cacheKey)
  if (cached) return res.json(cached)

  try {
    const allRows = await fetchUpstream()

    const recibidores = [...new Set(allRows.map(r => r.CodigoRecibidor).filter(Boolean))].sort()
    const estados     = [...new Set(allRows.map(r => r.EstadoLiquidacion).filter(Boolean))].sort()
    const claves      = [...new Set(allRows.map(r => r.ClaveEmbarque).filter(Boolean))].sort()

    const result = { recibidores, estados, claves }
    setCache(cacheKey, result)
    res.json(result)

  } catch (e) {
    console.error('❌ /api/liquidaciones/opciones:', e.message)
    res.status(502).json({ error: e.message })
  }
})

app.get('/health', (_req, res) => res.json({ ok: true, ts: new Date().toISOString() }))

// ─── Precalentar cache al arrancar ────────────────────────────────────────────
setTimeout(() => {
  console.log('🔥 Precalentando cache...')
  fetchUpstream().catch(e => console.error('❌ Precalentamiento fallido:', e.message))
}, 500)

app.listen(PORT, () => {
  console.log(`✅ Backend en http://localhost:${PORT}`)
  console.log(`ℹ️  Timeout: ${UPSTREAM_TIMEOUT_MS / 1000}s | Cache: ${CACHE_TTL_MS / 1000}s`)
})