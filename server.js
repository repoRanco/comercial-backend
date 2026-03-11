// server.js
import express from 'express'
import cors from 'cors'
import fetch from 'node-fetch'
import JSONStream from 'JSONStream'
import multer   from 'multer'
import { spawn } from 'child_process'
import path     from 'path'
import fs       from 'fs'
import { fileURLToPath } from 'url'

const app = express()
const PORT = process.env.PORT || 8081

const UPSTREAM_URL = 'https://apicloudfruit.ranco.cl/produccion/cb/CB_GenerarDataReportes/InformeGestionCuentasCorrientes/20169/%20/%20/%20/json/10112/0'
const UP_USER = process.env.UP_USER || 'Pdiaz@ranco.cl'
const UP_PASS = process.env.UP_PASS || 'Pato2023'

const UPSTREAM_TIMEOUT_MS = 900_000
// ✅ Cache de 24 horas — no se borra al cambiar de página
const CACHE_TTL_MS = 24 * 60 * 60 * 1000

const CAMPOS = [
  'IdFactEmbarque', 'ClaveEmbarque', 'EstadoLiquidacion',
  'CodigoRecibidor', 'Recibidor', 'CodigoTipoNave', 'TipoNave',
  'IdEspecie', 'CodigoEspecie', 'Especie', 'Moneda',
  'FOBChileAjustado', 'TotalFOBChile', 'Contenedor',
  'TiCaCursoLegal', 'FechaLiquidacion', 'FechaDespacho',
  'Naviera', 'Destino', 'CodigoTemporada',
]

const __dirname = path.dirname(fileURLToPath(import.meta.url))

app.use(cors({
  origin: ['http://localhost:5173', 'http://localhost:3000'],
  methods: ['GET', 'POST'],
}))
app.use(express.json())

// ─── Cache ────────────────────────────────────────────────────────────────────
const cache = new Map()
let isDownloading = false
let downloadPromise = null
let lastUpdated = null // ✅ Guardamos cuándo fue la última descarga exitosa

function getCache(key) {
  const v = cache.get(key)
  if (!v) return null
  if (Date.now() > v.expiresAt) { cache.delete(key); return null }
  return v.value
}

function setCache(key, value) {
  cache.set(key, { expiresAt: Date.now() + CACHE_TTL_MS, value })
}

// ✅ Limpia TODO el cache (upstream + filtros + opciones)
function clearAllCache() {
  cache.clear()
  console.log('🗑️  Cache limpiado completamente')
}

// ─── Progreso SSE ─────────────────────────────────────────────────────────────
let currentProgress = { count: 0, mb: 0, status: 'idle', lastUpdated: null }

app.get('/api/progress', (req, res) => {
  res.setHeader('Content-Type', 'text/event-stream')
  res.setHeader('Cache-Control', 'no-cache')
  res.setHeader('Connection', 'keep-alive')
  const sendProgress = () => res.write(`data: ${JSON.stringify(currentProgress)}\n\n`)
  sendProgress()
  const interval = setInterval(sendProgress, 1000)
  req.on('close', () => clearInterval(interval))
})

// ─── fetchUpstream ────────────────────────────────────────────────────────────
async function fetchUpstream() {
  const cached = getCache('upstream:slim')
  if (cached) return cached

  if (isDownloading) {
    console.log('⏳ Ya hay una descarga en curso, esperando...')
    return downloadPromise
  }

  isDownloading = true
  downloadPromise = (async () => {
    currentProgress = { count: 0, mb: 0, status: 'descargando', lastUpdated: null }
    const auth = Buffer.from(`${UP_USER}:${UP_PASS}`).toString('base64')
    const startedAt = Date.now()
    console.log('🔄 Iniciando descarga desde Ranco Cloud...')

    const controller = new AbortController()
    const timer = setTimeout(() => controller.abort(), UPSTREAM_TIMEOUT_MS)

    try {
      const res = await fetch(UPSTREAM_URL, {
        headers: { Authorization: `Basic ${auth}`, Accept: 'application/json' },
        signal: controller.signal,
      })

      if (!res.ok) throw new Error(`Upstream HTTP ${res.status}`)

      const rows = await new Promise((resolve, reject) => {
        const collected = []
        let totalBytes = 0
        const jsonStream = JSONStream.parse('Data.*')

        jsonStream.on('data', (raw) => {
          const obj = {}
          for (const f of CAMPOS) { if (raw[f] !== undefined) obj[f] = raw[f] }
          collected.push(obj)
          currentProgress.count++
          if (currentProgress.count % 10000 === 0) {
            currentProgress.mb = Math.round(totalBytes / 1024 / 1024)
            console.log(`📥 Progreso: ${currentProgress.count} filas | ${currentProgress.mb} MB`)
          }
        })

        jsonStream.on('end', () => {
          const tookMs = Date.now() - startedAt
          lastUpdated = new Date().toISOString()
          console.log(`✅ Descarga finalizada: ${currentProgress.count} filas en ${(tookMs / 1000).toFixed(1)}s`)
          currentProgress.status = 'completado'
          currentProgress.lastUpdated = lastUpdated
          resolve(collected)
        })

        jsonStream.on('error', reject)
        res.body.on('data', (chunk) => { totalBytes += chunk.length; jsonStream.write(chunk) })
        res.body.on('end', () => jsonStream.end())
        res.body.on('error', reject)
      })

      setCache('upstream:slim', rows)
      return rows

    } catch (e) {
      currentProgress.status = 'error'
      console.error('❌ Error en descarga:', e.message)
      throw e
    } finally {
      clearTimeout(timer)
      isDownloading = false
      downloadPromise = null
    }
  })()

  return downloadPromise
}

// ─── Normalización ────────────────────────────────────────────────────────────
function normalizeRow(r) {
  return {
    idFactEmbarque: r.IdFactEmbarque,
    claveEmbarque: r.ClaveEmbarque || '',
    estado: r.EstadoLiquidacion || '',
    recibidor: r.CodigoRecibidor || '',
    recibidorNombre: r.Recibidor || '',
    tipoNave: r.CodigoTipoNave || '',
    tipoNaveNombre: r.TipoNave || '',
    especie: r.CodigoEspecie || '',
    especieNombre: r.Especie || '',
    moneda: r.Moneda || '',
    totalFobNum: Number(r.FOBChileAjustado ?? r.TotalFOBChile ?? 0),
    contenedor: r.Contenedor || '',
    tipoCambio: r.TiCaCursoLegal ?? null,
    fechaLiquidacion: r.FechaLiquidacion ?? null,
    fechaDespacho: r.FechaDespacho ?? null,
    naviera: r.Naviera || '',
    destino: r.Destino || '',
    temporada: r.CodigoTemporada || '',
  }
}

function aggregateByEmbarque(rows) {
  const map = new Map()
  for (const r of rows) {
    const norm = normalizeRow(r)
    if (!norm.claveEmbarque) continue
    if (!map.has(norm.claveEmbarque)) map.set(norm.claveEmbarque, { ...norm, totalFobNum: 0 })
    const entry = map.get(norm.claveEmbarque)
    entry.totalFobNum += Number.isFinite(norm.totalFobNum) ? norm.totalFobNum : 0
  }
  return Array.from(map.values()).map(x => ({
    ...x,
    id: x.claveEmbarque,
    totalFob: x.totalFobNum.toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 }),
  })).sort((a, b) => a.claveEmbarque.localeCompare(b.claveEmbarque))
}

// ─── Rutas API ────────────────────────────────────────────────────────────────
app.get('/api/liquidaciones', async (req, res) => {
  const { recibidor = '', claveEmbarque = '', estado = '', especie = '', tipoNave = '' } = req.query
  const cacheKey = `liq:${recibidor}|${claveEmbarque}|${estado}|${especie}|${tipoNave}`
  const cached = getCache(cacheKey)
  if (cached) return res.json({ data: cached, cached: true, lastUpdated })

  try {
    const allRows = await fetchUpstream()
    const fRec = String(recibidor).trim().toUpperCase()
    const fCla = String(claveEmbarque).trim().toUpperCase()
    const fEst = String(estado).trim().toUpperCase()
    const fEsp = String(especie).trim().toUpperCase()
    const fNav = String(tipoNave).trim().toUpperCase()

    const filtered = allRows.filter(r => {
      const rr = String(r?.CodigoRecibidor || '').toUpperCase()
      const cc = String(r?.ClaveEmbarque || '').toUpperCase()
      const ee = String(r?.EstadoLiquidacion || '').toUpperCase()
      const es = String(r?.CodigoEspecie || '').toUpperCase()
      const tn = String(r?.CodigoTipoNave || '').toUpperCase()
      return ((!fRec || rr.includes(fRec)) && (!fCla || cc.includes(fCla)) && (!fEst || ee.includes(fEst)) && (!fEsp || es.includes(fEsp)) && (!fNav || tn.includes(fNav)))
    })

    const data = aggregateByEmbarque(filtered)
    setCache(cacheKey, data)
    res.json({ data, total: data.length, cached: false, lastUpdated })
  } catch (e) {
    res.status(502).json({ error: 'Error al obtener datos de Ranco Cloud' })
  }
})

app.get('/api/liquidaciones/opciones', async (_req, res) => {
  const cached = getCache('opciones')
  if (cached) return res.json({ ...cached, lastUpdated })
  try {
    const allRows = await fetchUpstream()
    const result = {
      recibidores: [...new Set(allRows.map(r => r.CodigoRecibidor).filter(Boolean))].sort(),
      estados: [...new Set(allRows.map(r => r.EstadoLiquidacion).filter(Boolean))].sort(),
      claves: [...new Set(allRows.map(r => r.ClaveEmbarque).filter(Boolean))].sort(),
      especies: [...new Set(allRows.map(r => r.CodigoEspecie).filter(Boolean))].sort(),
      tipoNaves: [...new Set(allRows.map(r => r.CodigoTipoNave).filter(Boolean))].sort(),
    }
    setCache('opciones', result)
    res.json({ ...result, lastUpdated })
  } catch (e) {
    res.status(502).json({ error: 'Error al obtener opciones' })
  }
})

// ✅ Estado del cache — para mostrar en el frontend cuándo fue la última actualización
app.get('/api/liquidaciones/estado', (_req, res) => {
  res.json({
    isDownloading,
    lastUpdated,
    cacheActivo: cache.has('upstream:slim'),
    progreso: currentProgress,
  })
})

// ✅ Refresh manual — limpia el cache y dispara nueva descarga en segundo plano
app.post('/api/liquidaciones/refresh', (req, res) => {
  if (isDownloading) {
    return res.json({
      message: 'Ya hay una descarga en curso',
      isDownloading: true,
      lastUpdated,
    })
  }

  clearAllCache()
  // Iniciamos en segundo plano sin bloquear la respuesta
  fetchUpstream().catch(e => console.error('❌ Error en refresh manual:', e.message))

  res.json({
    message: 'Actualización iniciada. Los datos estarán disponibles en ~10 minutos.',
    isDownloading: true,
    lastUpdated,
  })
})

// ─── ETL ──────────────────────────────────────────────────────────────────────
const upload = multer({ dest: path.join(__dirname, 'tmp_uploads') })
const SCRIPTS = {
  'M:Qupai': '001-M-Qupai.py', 'A:Qupai': '001-A-Qupai.py',
  'M:Sanyong': '002-M-Sanyong.py', 'A:Sanyong': '002-A-Sanyong.py',
  'M:RiverKing': '003-M-RiverKing.py', 'A:RiverKing': '003-A-RiverKing.py',
  'M:Wonong': '004-M-Wonong.py', 'A:Wonong': '004-A-Wonong.py',
  'M:SunVirtue': '005-M-SunVirtue.py', 'A:SunVirtue': '005-A-SunVirtue.py',
  'M:CCMax': '006-M-CCMax.py', 'A:CCMax': '006-A-CCMax.py',
  'M:Kingo': '007-M-Kingo.py', 'A:Kingo': '007-A-Kingo.py',
  'M:Xianfeng': '008-M-Xianfeng.py', 'A:Xianfeng': '008-A-Xianfeng.py',
  'M:HingLee': '009-M-HingLee.py', 'A:HingLee': '009-A-HingLee.py',
  'M:Qinguo': '010-M-Qinguo.py', 'A:Qinguo': '010-A-Qinguo.py',
  'M:FrutaCloud': '011-M-FrutaCloud.py', 'A:FrutaCloud': '011-A-FrutaCloud.py',
}

app.post('/api/etl/transformar', upload.single('archivo'), async (req, res) => {
  const inputPath = req.file?.path
  const { modo = '', cliente = '' } = req.body
  if (!inputPath) return res.status(400).json({ error: 'No se recibió archivo' })

  const scriptName = SCRIPTS[`${modo}:${cliente}`]
  if (!scriptName) {
    fs.unlinkSync(inputPath)
    return res.status(400).json({ error: 'Configuración ETL no válida' })
  }

  const scriptPath = path.join(__dirname, scriptName)
  const outputPath = inputPath + '_out.xlsx'

  try {
    await new Promise((resolve, reject) => {
      const py = spawn('python', [scriptPath, inputPath, outputPath])
      let stderr = ''
      py.stderr.on('data', d => stderr += d.toString())
      py.on('close', code => code !== 0 ? reject(new Error(stderr)) : resolve())
    })

    res.setHeader('Content-Type', 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
    res.setHeader('Content-Disposition', `attachment; filename="${cliente}_${modo}.xlsx"`)
    res.sendFile(outputPath, () => {
      if (fs.existsSync(inputPath)) fs.unlinkSync(inputPath)
      if (fs.existsSync(outputPath)) fs.unlinkSync(outputPath)
    })
  } catch (e) {
    console.error('❌ ETL Error:', e.message)
    if (fs.existsSync(inputPath)) fs.unlinkSync(inputPath)
    if (fs.existsSync(outputPath)) fs.unlinkSync(outputPath)
    res.status(500).json({ error: e.message })
  }
})

// ─── Health ───────────────────────────────────────────────────────────────────
app.get('/health', (_req, res) => res.json({
  ok: true,
  ts: new Date().toISOString(),
  cacheActivo: cache.has('upstream:slim'),
  lastUpdated,
  isDownloading,
}))

// ─── Inicio — SIN precalentamiento automático ─────────────────────────────────
app.listen(PORT, () => {
  console.log(`✅ Backend en http://localhost:${PORT}`)
  console.log(`ℹ️  Cache: 24h | Timeout: ${UPSTREAM_TIMEOUT_MS / 1000}s`)
  console.log(`ℹ️  Para actualizar datos: POST /api/liquidaciones/refresh`)
})